package dispatcher

import (
	"context"
	"errors"
	"io"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const heartbeatTimeout = time.Second *15

type Dispatcher struct {
	tickInterval time.Duration
	repo         domain.Repository
	pendingJobs  chan *domain.Job
	pb.UnimplementedDispatcherServiceServer
	registryMu     sync.RWMutex
	workerRegistry map[string]*domain.WorkerSession
	Outbound       chan *domain.DispatcherCommand
}

func NewDispatcher(
	tickInterval time.Duration,
	repo domain.Repository,
) *Dispatcher {
	return &Dispatcher{
		tickInterval:   tickInterval,
		repo:           repo,
		pendingJobs:    make(chan *domain.Job, 10),
		workerRegistry: make(map[string]*domain.WorkerSession),
		Outbound:       make(chan *domain.DispatcherCommand),
	}
}

func (d *Dispatcher) Connect(
	stream grpc.BidiStreamingServer[
		pb.WorkerEvent,
		pb.DispatcherCommand,
	],
) error {
	ctx := stream.Context()
	recvErr := make(chan error, 1)

	go func() {
		for {
			event, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}

			d.handleWorkerEvent(event)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err

		case command, ok := <-d.Outbound:
			if !ok {
				return nil
			}

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

func (d *Dispatcher) handleWorkerEvent(event *pb.WorkerEvent) {
	switch payload := event.GetEvent().(type) {
	case *pb.WorkerEvent_Register:
		d.registryMu.Lock()
		d.workerRegistry[event.GetWorkerId()] = &domain.WorkerSession{
			ID:             event.GetWorkerId(),
			Capabilities:   payload.Register.GetCapabilities(),
			Concurrency:    int(payload.Register.GetConcurrency()),
			AvailableSlots: int(payload.Register.GetConcurrency()),
			LastHeartbeat:  time.Now(),
			Status:         "ready",
		}
		d.registryMu.Unlock()

	case *pb.WorkerEvent_Heartbeat:
		d.HandleHeartbeat(&domain.Heartbeat{
			WorkerID:       event.GetWorkerId(),
			RunningJobs:    int(payload.Heartbeat.GetRunningJobs()),
			AvailableSlots: int(payload.Heartbeat.GetAvailableSlots()),
			SentAt:         time.Now(),
		})

	case *pb.WorkerEvent_JobAccepted:
		log.Printf("worker %s accepted job %s", event.GetWorkerId(), payload.JobAccepted.GetJobId())

	case *pb.WorkerEvent_JobResult:
		log.Printf("worker %s finished job %s success=%t", event.GetWorkerId(), payload.JobResult.GetJobId(), payload.JobResult.GetSuccess())
	}
}

func dispatcherCommandToProto(command *domain.DispatcherCommand) *pb.DispatcherCommand {
	if command.Type != domain.CommandAssignJob || command.Job == nil {
		return nil
	}

	assignJob := &pb.AssignJob{
		JobId:          command.Job.ID,
		Type:           command.Job.Type,
		Priority:       int32(command.Job.Priority),
		RetryCount:     int32(command.Job.RetryCount),
		MaxRetries:     int32(command.Job.MaxRetries),
		TimeoutSeconds: int32(command.Job.TimeoutSeconds),
	}
	if command.Job.ScheduledAt != nil {
		assignJob.ScheduledAt = timestamppb.New(*command.Job.ScheduledAt)
	}
	if !command.Job.CreatedAt.IsZero() {
		assignJob.CreatedAt = timestamppb.New(command.Job.CreatedAt)
	}

	return &pb.DispatcherCommand{
		Command: &pb.DispatcherCommand_AssignJob{
			AssignJob: assignJob,
		},
	}
}

func (d *Dispatcher) CheckDB(ctx context.Context) {
	ticker := time.NewTicker(d.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			jobs, err := d.repo.ClaimQueuedJobs(ctx, 10)
			if err != nil {
				log.Printf("claim queued jobs: %v", err)
				continue
			}

			for _, job := range jobs {
				select {
				case d.pendingJobs <- job:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (d *Dispatcher) HandleHeartbeat(h *domain.Heartbeat) {
	d.registryMu.Lock()
	defer d.registryMu.Unlock()

	worker, exists := d.workerRegistry[h.WorkerID]
	if !exists {
		return
	}

	worker.RunningJobs = h.RunningJobs
	worker.AvailableSlots = h.AvailableSlots
	worker.LastHeartbeat = time.Now()
	worker.Status = "ready"
}

func (d *Dispatcher) Scheduler(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-d.pendingJobs:
			if !ok {
				return
			}
			d.registryMu.Lock()

			var availableWorker *domain.WorkerSession

			//schedule based on last heartbeat , capability , available slots and status
			for _, worker := range d.workerRegistry {
				if (time.Since(worker.LastHeartbeat) <= heartbeatTimeout) && worker.AvailableSlots > 0 && slices.Contains(worker.Capabilities, job.Type) && worker.Status == "ready" {
					worker.AvailableSlots--
					//add reserve job instaed run it later
					worker.RunningJobs++
					availableWorker = worker
					break
				}
			}
			d.registryMu.Unlock()

			if availableWorker == nil {
				//add retry logic later
				log.Println("no available worker found")
				continue
			}

			cmd := &domain.DispatcherCommand{
				Type: domain.CommandAssignJob,
				Job:  job,
			}
			select {
			case <-ctx.Done():
				d.registryMu.Lock()
				availableWorker.AvailableSlots++
				availableWorker.RunningJobs--
				d.registryMu.Unlock()
				return
			case d.Outbound <- cmd:
			}
		}
	}

}
