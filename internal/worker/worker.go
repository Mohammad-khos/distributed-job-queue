package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerNode struct {
	worker    *domain.Worker
	conn      *grpc.ClientConn
	stream    pb.DispatcherService_ConnectClient
	processor JobProcessor

	streamCancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

func NewWorkerNode(
	ctx context.Context,
	id string,
	concurrency int,
	capabilities []string,
	dispatcherAddr string,
	processor JobProcessor,
) (*WorkerNode, error) {
	if ctx == nil {
		return nil, errors.New("worker: context is required")
	}

	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("worker: worker id is required")
	}

	if concurrency <= 0 {
		return nil, errors.New(
			"worker: concurrency must be greater than zero",
		)
	}

	dispatcherAddr = strings.TrimSpace(dispatcherAddr)
	if dispatcherAddr == "" {
		return nil, errors.New(
			"worker: dispatcher address is required",
		)
	}

	if processor == nil {
		return nil, errors.New(
			"worker: job processor is required",
		)
	}

	capabilitySet := make(
		map[string]struct{},
		len(capabilities),
	)

	for _, capability := range capabilities {
		capability = strings.TrimSpace(capability)
		if capability == "" {
			continue
		}

		capabilitySet[capability] = struct{}{}
	}

	if len(capabilitySet) == 0 {
		return nil, errors.New(
			"worker: at least one capability is required",
		)
	}

	streamContext, streamCancel := context.WithCancel(ctx)

	conn, err := grpc.NewClient(
		dispatcherAddr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		streamCancel()

		return nil, fmt.Errorf(
			"create grpc client: %w",
			err,
		)
	}

	client := pb.NewDispatcherServiceClient(conn)

	stream, err := client.Connect(streamContext)
	if err != nil {
		streamCancel()
		_ = conn.Close()

		return nil, fmt.Errorf(
			"open dispatcher stream: %w",
			err,
		)
	}

	eventBufferSize := concurrency * 4
	if eventBufferSize < 4 {
		eventBufferSize = 4
	}

	return &WorkerNode{
		worker: &domain.Worker{
			ID:           id,
			Concurrency:  concurrency,
			Capabilities: capabilitySet,

			Jobs: make(
				chan *pb.AssignJob,
				concurrency,
			),

			Events: make(
				chan *pb.WorkerEvent,
				eventBufferSize,
			),

			Running: make(
				map[string]context.CancelFunc,
			),
		},
		conn:         conn,
		stream:       stream,
		processor:    processor,
		streamCancel: streamCancel,
	}, nil
}

func (n *WorkerNode) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("worker: context is required")
	}

	if n == nil || n.worker == nil {
		return errors.New("worker: worker node is not initialized")
	}

	if n.stream == nil {
		return errors.New("worker: grpc stream is not initialized")
	}

	if n.processor == nil {
		return errors.New("worker: job processor is not configured")
	}

	runContext, cancel := context.WithCancel(ctx)

	defer func() {
		cancel()

		if n.streamCancel != nil {
			n.streamCancel()
		}

		n.worker.Wg.Wait()
	}()

	errorChannel := make(
		chan error,
		n.worker.Concurrency+2,
	)

	startRoutine := func(
		name string,
		run func(context.Context) error,
	) {
		n.worker.Wg.Add(1)

		go func() {
			defer n.worker.Wg.Done()

			err := run(runContext)
			if err == nil || errors.Is(err, context.Canceled) {
				return
			}

			select {
			case errorChannel <- fmt.Errorf("%s: %w", name, err):
			default:
			}

			cancel()
		}()
	}

	startRoutine("send worker events", n.SendEvents)

	if err := n.publishRegister(runContext); err != nil {
		return fmt.Errorf("publish worker registration: %w", err)
	}

	for executorID := 1; executorID <= n.worker.Concurrency; executorID++ {
		name := fmt.Sprintf("executor-%d", executorID)

		startRoutine(name, n.HandleJob)
	}

	startRoutine(
		"receive dispatcher commands",
		n.ReceiveJobs,
	)

	select {
	case <-ctx.Done():
		return nil

	case err := <-errorChannel:
		return err
	}
}

func (n *WorkerNode) Close() error {
	if n == nil {
		return nil
	}

	n.closeOnce.Do(func() {
		if n.streamCancel != nil {
			n.streamCancel()
		}

		if n.conn != nil {
			n.closeErr = n.conn.Close()
		}
	})

	return n.closeErr
}

func (n *WorkerNode) ReceiveJobs(ctx context.Context) error {
	if n.stream == nil {
		return errors.New("grpc stream is not initialized")
	}

	for {
		command, err := n.stream.Recv()
		if errors.Is(err, io.EOF) {
			return io.EOF
		}

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return fmt.Errorf(
				"receive dispatcher command: %w",
				err,
			)
		}

		if command == nil {
			continue
		}

		if job := command.GetAssignJob(); job != nil {
			select {
			case n.worker.Jobs <- job:
			case <-ctx.Done():
				return ctx.Err()
			}

			continue
		}

		if cancelJob := command.GetCancelJob(); cancelJob != nil {
			n.cancelRunningJob(cancelJob.GetJobId())
		}
	}
}

func (n *WorkerNode) SendEvents(ctx context.Context) error {
	if n.stream == nil {
		return errors.New("grpc stream is not initialized")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-n.worker.Events:
			if !ok {
				return nil
			}

			if event == nil {
				continue
			}

			if err := n.stream.Send(event); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				return fmt.Errorf(
					"send worker event: %w",
					err,
				)
			}
		}
	}
}

func (n *WorkerNode) HandleJob(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case job, ok := <-n.worker.Jobs:
			if !ok {
				return nil
			}

			if job == nil {
				continue
			}

			if err := n.executeJob(ctx, job); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				return err
			}
		}
	}
}

func (n *WorkerNode) executeJob(
	workerContext context.Context,
	job *pb.AssignJob,
) error {
	if validationErr := n.validateAssignedJob(job); validationErr != nil {
		return n.publishJobResult(
			workerContext,
			job,
			nil,
			validationErr,
		)
	}

	jobContext, cancel := n.createJobContext(
		workerContext,
		job,
	)

	if err := n.registerRunningJob(job.GetJobId(), cancel); err != nil {
		cancel()

		return n.publishJobResult(
			workerContext,
			job,
			nil,
			err,
		)
	}

	defer func() {
		cancel()
		n.unregisterRunningJob(job.GetJobId())
	}()

	if err := n.publishJobAccepted(workerContext, job); err != nil {
		return err
	}

	output, jobErr := n.processJobSafely(
		jobContext,
		job,
	)

	if jobErr == nil {
		if contextErr := jobContext.Err(); contextErr != nil {
			jobErr = contextErr
		}
	}

	return n.publishJobResult(
		workerContext,
		job,
		output,
		jobErr,
	)
}

func (n *WorkerNode) processJobSafely(
	ctx context.Context,
	job *pb.AssignJob,
) (
	output []byte,
	err error,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"worker: panic while processing job %q: %v\n%s",
				job.GetJobId(),
				recovered,
				debug.Stack(),
			)
		}
	}()

	return n.processor.Process(ctx, job)
}

func (n *WorkerNode) createJobContext(
	parent context.Context,
	job *pb.AssignJob,
) (
	context.Context,
	context.CancelFunc,
) {
	timeoutSeconds := job.GetTimeoutSeconds()

	if timeoutSeconds > 0 {
		return context.WithTimeout(
			parent,
			time.Duration(timeoutSeconds)*time.Second,
		)
	}

	return context.WithCancel(parent)
}

func (n *WorkerNode) validateAssignedJob(
	job *pb.AssignJob,
) error {
	if job == nil {
		return errors.New("worker: assigned job is nil")
	}

	if strings.TrimSpace(job.GetJobId()) == "" {
		return errors.New("worker: assigned job id is required")
	}

	jobType := strings.TrimSpace(job.GetType())
	if jobType == "" {
		return fmt.Errorf(
			"worker: job %q type is required",
			job.GetJobId(),
		)
	}

	if !n.supports(jobType) {
		return fmt.Errorf(
			"worker %q does not support job type %q",
			n.worker.ID,
			jobType,
		)
	}

	return nil
}

func (n *WorkerNode) supports(jobType string) bool {
	n.worker.Mu.RLock()
	defer n.worker.Mu.RUnlock()

	_, supported := n.worker.Capabilities[jobType]

	return supported
}

func (n *WorkerNode) registerRunningJob(
	jobID string,
	cancel context.CancelFunc,
) error {
	n.worker.Mu.Lock()
	defer n.worker.Mu.Unlock()

	if _, exists := n.worker.Running[jobID]; exists {
		return fmt.Errorf(
			"worker: job %q is already running",
			jobID,
		)
	}

	n.worker.Running[jobID] = cancel

	return nil
}

func (n *WorkerNode) unregisterRunningJob(jobID string) {
	n.worker.Mu.Lock()
	defer n.worker.Mu.Unlock()

	delete(n.worker.Running, jobID)
}

func (n *WorkerNode) cancelRunningJob(jobID string) bool {
	if strings.TrimSpace(jobID) == "" {
		return false
	}

	n.worker.Mu.RLock()
	cancel, exists := n.worker.Running[jobID]
	n.worker.Mu.RUnlock()

	if !exists {
		return false
	}

	cancel()

	return true
}

func (n *WorkerNode) publishRegister(
	ctx context.Context,
) error {
	capabilities := make(
		[]string,
		0,
		len(n.worker.Capabilities),
	)

	for capability := range n.worker.Capabilities {
		capabilities = append(capabilities, capability)
	}

	sort.Strings(capabilities)

	return n.publishEvent(
		ctx,
		&pb.WorkerEvent{
			WorkerId: n.worker.ID,
			Event: &pb.WorkerEvent_Register{
				Register: &pb.RegisterWorker{
					Concurrency: int32(
						n.worker.Concurrency,
					),
					Capabilities: capabilities,
				},
			},
		},
	)
}

func (n *WorkerNode) publishJobAccepted(
	ctx context.Context,
	job *pb.AssignJob,
) error {
	return n.publishEvent(
		ctx,
		&pb.WorkerEvent{
			WorkerId: n.worker.ID,
			Event: &pb.WorkerEvent_JobAccepted{
				JobAccepted: &pb.JobAccepted{
					JobId: job.GetJobId(),
				},
			},
		},
	)
}

func (n *WorkerNode) publishJobResult(
	ctx context.Context,
	job *pb.AssignJob,
	output []byte,
	jobErr error,
) error {
	result := &pb.JobResult{
		Success: jobErr == nil,
		Output:  output,
	}

	if job != nil {
		result.JobId = job.GetJobId()
	}

	if jobErr != nil {
		result.ErrorMessage = jobErr.Error()
	}

	return n.publishEvent(
		ctx,
		&pb.WorkerEvent{
			WorkerId: n.worker.ID,
			Event: &pb.WorkerEvent_JobResult{
				JobResult: result,
			},
		},
	)
}

func (n *WorkerNode) publishEvent(
	ctx context.Context,
	event *pb.WorkerEvent,
) error {
	if event == nil {
		return errors.New("worker: event cannot be nil")
	}

	select {
	case n.worker.Events <- event:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}
