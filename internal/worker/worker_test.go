package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
)

type fakeJobProcessor struct {
	output []byte
	err    error

	received chan *pb.AssignJob
}

func (f *fakeJobProcessor) Process(
	ctx context.Context,
	job *pb.AssignJob,
) ([]byte, error) {
	select {
	case f.received <- job:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return f.output, f.err
}

func TestHandleJobConsumesJobFromChannel(t *testing.T) {
	t.Parallel()

	processor := &fakeJobProcessor{
		output:   []byte("processed"),
		received: make(chan *pb.AssignJob, 1),
	}

	node := &WorkerNode{
		worker: &domain.Worker{
			ID:          "worker-test",
			Concurrency: 1,
			Capabilities: map[string]struct{}{
				JobTypeAPICall: {},
			},
			Jobs:    make(chan *pb.AssignJob, 1),
			Events:  make(chan *pb.WorkerEvent, 2),
			Running: make(map[string]context.CancelFunc),
		},
		processor: processor,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executorDone := make(chan error, 1)

	go func() {
		executorDone <- node.HandleJob(ctx)
	}()

	job := &pb.AssignJob{
		JobId: "job-1",
		Type:  JobTypeAPICall,
	}

	node.worker.Jobs <- job

	select {
	case received := <-processor.received:
		if received.GetJobId() != job.GetJobId() {
			t.Fatalf(
				"expected job id %q, got %q",
				job.GetJobId(),
				received.GetJobId(),
			)
		}

	case <-time.After(time.Second):
		t.Fatal("processor did not receive job")
	}

	acceptedEvent := receiveWorkerEvent(t, node.worker.Events)

	if acceptedEvent.GetJobAccepted() == nil {
		t.Fatal("expected JobAccepted event")
	}

	if acceptedEvent.GetJobAccepted().GetJobId() != job.GetJobId() {
		t.Fatalf(
			"expected accepted job id %q, got %q",
			job.GetJobId(),
			acceptedEvent.GetJobAccepted().GetJobId(),
		)
	}

	resultEvent := receiveWorkerEvent(t, node.worker.Events)
	result := resultEvent.GetJobResult()

	if result == nil {
		t.Fatal("expected JobResult event")
	}

	if !result.GetSuccess() {
		t.Fatalf(
			"expected successful result, got error %q",
			result.GetErrorMessage(),
		)
	}

	if string(result.GetOutput()) != "processed" {
		t.Fatalf(
			"expected output %q, got %q",
			"processed",
			string(result.GetOutput()),
		)
	}

	cancel()

	select {
	case err := <-executorDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"expected context canceled, got %v",
				err,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("executor did not stop")
	}
}

func receiveWorkerEvent(
	t *testing.T,
	events <-chan *pb.WorkerEvent,
) *pb.WorkerEvent {
	t.Helper()

	select {
	case event := <-events:
		return event

	case <-time.After(time.Second):
		t.Fatal("worker event was not published")
		return nil
	}
}