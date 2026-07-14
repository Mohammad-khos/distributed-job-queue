package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultTickInterval = time.Second
	cleanupTimeout      = 5 * time.Second
	claimBatchSize      = 10
)

type Dispatcher struct {
	tickInterval time.Duration
	repo         domain.Repository
	pendingJobs  chan *domain.Job

	pb.UnimplementedDispatcherServiceServer

	registryMu     sync.RWMutex
	workerRegistry map[string]*domain.WorkerSession
}

func NewDispatcher(tickInterval time.Duration, repo domain.Repository) *Dispatcher {
	if tickInterval <= 0 {
		tickInterval = defaultTickInterval
	}

	return &Dispatcher{
		tickInterval:   tickInterval,
		repo:           repo,
		pendingJobs:    make(chan *domain.Job, claimBatchSize),
		workerRegistry: make(map[string]*domain.WorkerSession),
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	go d.CheckDB(ctx)
	d.Scheduler(ctx)
}

func (d *Dispatcher) Connect(
	stream grpc.BidiStreamingServer[pb.WorkerEvent, pb.DispatcherCommand],
) error {
	firstEvent, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}

	session, err := d.registerWorker(firstEvent)
	if err != nil {
		return err
	}
	defer d.unregisterWorker(session)

	recvErr := make(chan error, 1)
	go d.receiveEvents(stream.Context(), session, stream, recvErr)

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()

		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err

		case command := <-session.Outbound:
			pbCommand := dispatcherCommandToProto(command)
			if pbCommand == nil {
				continue
			}
			if err := stream.Send(pbCommand); err != nil {
				return err
			}
		}
	}
}

func (d *Dispatcher) registerWorker(event *pb.WorkerEvent) (*domain.WorkerSession, error) {
	if event == nil || event.GetRegister() == nil {
		return nil, errors.New("first worker event must be register")
	}

	workerID := strings.TrimSpace(event.GetWorkerId())
	registration := event.GetRegister()
	concurrency := int(registration.GetConcurrency())

	if workerID == "" {
		return nil, errors.New("worker id is required")
	}
	if concurrency <= 0 {
		return nil, errors.New("worker concurrency must be greater than zero")
	}
	if len(registration.GetCapabilities()) == 0 {
		return nil, errors.New("worker capabilities are required")
	}

	session := &domain.WorkerSession{
		ID:             workerID,
		Capabilities:   registration.GetCapabilities(),
		Concurrency:    concurrency,
		AvailableSlots: concurrency,
		LastHeartbeat:  time.Now().UTC(),
		Status:         domain.WorkerStatusReady,
		Outbound:       make(chan *domain.DispatcherCommand, concurrency),
		Done:           make(chan struct{}),
		ReservedJobs:   make(map[string]struct{}),
		RunningJobIDs:  make(map[string]struct{}),
	}

	d.registryMu.Lock()
	defer d.registryMu.Unlock()

	if _, exists := d.workerRegistry[workerID]; exists {
		return nil, fmt.Errorf("worker %q is already connected", workerID)
	}

	d.workerRegistry[workerID] = session
	log.Printf("worker %s registered", workerID)

	return session, nil
}

func (d *Dispatcher) receiveEvents(
	ctx context.Context,
	session *domain.WorkerSession,
	stream grpc.BidiStreamingServer[pb.WorkerEvent, pb.DispatcherCommand],
	recvErr chan<- error,
) {
	for {
		event, err := stream.Recv()
		if err != nil {
			select {
			case recvErr <- err:
			case <-ctx.Done():
			}
			return
		}

		if event == nil {
			continue
		}
		if event.GetWorkerId() != session.ID {
			recvErr <- errors.New("worker id does not match registered stream")
			return
		}

		if err := d.handleWorkerEvent(ctx, session, event); err != nil {
			log.Printf("worker %s event failed: %v", session.ID, err)
		}
	}
}

func (d *Dispatcher) handleWorkerEvent(
	ctx context.Context,
	session *domain.WorkerSession,
	event *pb.WorkerEvent,
) error {
	switch payload := event.GetEvent().(type) {
	case *pb.WorkerEvent_Heartbeat:
		d.registryMu.Lock()
		session.LastHeartbeat = time.Now().UTC()
		d.registryMu.Unlock()
		return nil

	case *pb.WorkerEvent_JobAccepted:
		return d.handleJobAccepted(ctx, session, payload.JobAccepted.GetJobId())

	case *pb.WorkerEvent_JobResult:
		return d.handleJobResult(ctx, session, payload.JobResult)

	default:
		return errors.New("unsupported worker event")
	}
}

func (d *Dispatcher) handleJobAccepted(
	ctx context.Context,
	session *domain.WorkerSession,
	jobID string,
) error {
	if strings.TrimSpace(jobID) == "" {
		return errors.New("job id is required")
	}

	d.registryMu.Lock()
	if current, exists := d.workerRegistry[session.ID]; !exists || current != session {
		d.registryMu.Unlock()
		return fmt.Errorf("worker %q is not registered", session.ID)
	}
	if _, exists := session.ReservedJobs[jobID]; !exists {
		d.registryMu.Unlock()
		return fmt.Errorf("job %q is not reserved for worker %q", jobID, session.ID)
	}
	delete(session.ReservedJobs, jobID)
	session.RunningJobIDs[jobID] = struct{}{}
	updateSlots(session)
	d.registryMu.Unlock()

	if err := d.repo.MarkJobProcessing(ctx, jobID); err != nil {
		return fmt.Errorf("mark job processing: %w", err)
	}

	return nil
}

func (d *Dispatcher) handleJobResult(
	ctx context.Context,
	session *domain.WorkerSession,
	result *pb.JobResult,
) error {
	if result == nil || strings.TrimSpace(result.GetJobId()) == "" {
		return errors.New("job result requires job id")
	}

	jobID := result.GetJobId()

	d.registryMu.Lock()
	if current, exists := d.workerRegistry[session.ID]; !exists || current != session {
		d.registryMu.Unlock()
		return fmt.Errorf("worker %q is not registered", session.ID)
	}
	_, reserved := session.ReservedJobs[jobID]
	_, running := session.RunningJobIDs[jobID]
	if !reserved && !running {
		d.registryMu.Unlock()
		return fmt.Errorf("job %q is not assigned to worker %q", jobID, session.ID)
	}
	delete(session.ReservedJobs, jobID)
	delete(session.RunningJobIDs, jobID)
	updateSlots(session)
	d.registryMu.Unlock()

	if result.GetSuccess() {
		if !running {
			return fmt.Errorf("job %q returned success before acceptance", jobID)
		}
		if err := d.repo.MarkJobDone(ctx, jobID); err != nil {
			return fmt.Errorf("mark job done: %w", err)
		}
	} else if err := d.repo.MarkJobFailed(
		ctx,
		jobID,
		result.GetErrorMessage(),
	); err != nil {
		return fmt.Errorf("mark job failed: %w", err)
	}

	return nil
}

func dispatcherCommandToProto(command *domain.DispatcherCommand) *pb.DispatcherCommand {
	if command == nil || command.Type != domain.CommandAssignJob || command.Job == nil {
		return nil
	}

	job := command.Job
	assignJob := &pb.AssignJob{
		JobId:          job.ID,
		Type:           job.Type,
		Priority:       int32(job.Priority),
		RetryCount:     int32(job.RetryCount),
		MaxRetries:     int32(job.MaxRetries),
		TimeoutSeconds: int32(job.TimeoutSeconds),
	}

	if job.ScheduledAt != nil {
		assignJob.ScheduledAt = timestamppb.New(*job.ScheduledAt)
	}
	if !job.CreatedAt.IsZero() {
		assignJob.CreatedAt = timestamppb.New(job.CreatedAt)
	}

	return &pb.DispatcherCommand{
		Command: &pb.DispatcherCommand_AssignJob{AssignJob: assignJob},
	}
}

func (d *Dispatcher) CheckDB(ctx context.Context) {
	if d.repo == nil {
		log.Print("dispatcher repository is not configured")
		return
	}

	ticker := time.NewTicker(d.tickInterval)
	defer ticker.Stop()

	for {
		jobs, err := d.repo.ClaimQueuedJobs(ctx, claimBatchSize)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("claim queued jobs: %v", err)
		}

		for _, job := range jobs {
			select {
			case d.pendingJobs <- job:
			case <-ctx.Done():
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *Dispatcher) Scheduler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case job := <-d.pendingJobs:
			if job == nil {
				continue
			}

			worker := d.reserveWorker(job)
			if worker == nil {
				d.releaseReservedJob(ctx, job.ID)
				continue
			}

			command := &domain.DispatcherCommand{
				Type: domain.CommandAssignJob,
				Job:  job,
			}

			select {
			case worker.Outbound <- command:
			case <-worker.Done:
				d.rollbackReservation(worker, job.ID)
				d.releaseReservedJobInBackground(job.ID)
			case <-ctx.Done():
				d.rollbackReservation(worker, job.ID)
				d.releaseReservedJobInBackground(job.ID)
				return
			}
		}
	}
}

func (d *Dispatcher) reserveWorker(job *domain.Job) *domain.WorkerSession {
	d.registryMu.Lock()
	defer d.registryMu.Unlock()

	var selected *domain.WorkerSession
	for _, worker := range d.workerRegistry {
		if worker.Status != domain.WorkerStatusReady ||
			worker.AvailableSlots == 0 ||
			!slices.Contains(worker.Capabilities, job.Type) {
			continue
		}

		if selected == nil || worker.AvailableSlots > selected.AvailableSlots {
			selected = worker
		}
	}

	if selected == nil {
		return nil
	}

	selected.ReservedJobs[job.ID] = struct{}{}
	updateSlots(selected)
	return selected
}

func (d *Dispatcher) rollbackReservation(worker *domain.WorkerSession, jobID string) {
	d.registryMu.Lock()
	delete(worker.ReservedJobs, jobID)
	updateSlots(worker)
	d.registryMu.Unlock()
}

func (d *Dispatcher) releaseReservedJob(ctx context.Context, jobID string) {
	if d.repo == nil {
		return
	}

	if err := d.repo.ReleaseReservedJob(ctx, jobID); err != nil &&
		!errors.Is(err, context.Canceled) {
		log.Printf("release reserved job %s: %v", jobID, err)
	}
}

func (d *Dispatcher) releaseReservedJobInBackground(jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	d.releaseReservedJob(ctx, jobID)
}

func (d *Dispatcher) unregisterWorker(session *domain.WorkerSession) {
	d.registryMu.Lock()
	current, exists := d.workerRegistry[session.ID]
	if !exists || current != session {
		d.registryMu.Unlock()
		return
	}

	delete(d.workerRegistry, session.ID)
	close(session.Done)
	reservedJobs := jobIDs(session.ReservedJobs)
	runningJobs := jobIDs(session.RunningJobIDs)
	session.Status = "offline"
	d.registryMu.Unlock()

	if d.repo == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	for _, jobID := range reservedJobs {
		if err := d.repo.ReleaseReservedJob(ctx, jobID); err != nil {
			log.Printf("release job %s after disconnect: %v", jobID, err)
		}
	}
	for _, jobID := range runningJobs {
		if err := d.repo.MarkJobFailed(
			ctx,
			jobID,
			"worker disconnected before returning result",
		); err != nil {
			log.Printf("fail job %s after disconnect: %v", jobID, err)
		}
	}
}

func updateSlots(worker *domain.WorkerSession) {
	worker.RunningJobs = len(worker.RunningJobIDs)
	worker.AvailableSlots = worker.Concurrency -
		len(worker.ReservedJobs) -
		worker.RunningJobs

	if worker.AvailableSlots < 0 {
		worker.AvailableSlots = 0
	}
}

func jobIDs(jobs map[string]struct{}) []string {
	result := make([]string, 0, len(jobs))
	for jobID := range jobs {
		result = append(result, jobID)
	}
	return result
}
