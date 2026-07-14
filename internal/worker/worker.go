package worker

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
	jobhandler "github.com/mohammad-khos/distributed-job-queue/internal/job_handler"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerNode struct {
	worker  *domain.Worker
	conn    *grpc.ClientConn
	client  pb.DispatcherServiceClient
	stream  pb.DispatcherService_ConnectClient
	handler *jobhandler.Handler
}

func NewWorkerNode(
	ctx context.Context,
	id string,
	concurrency int,
	capabilities []string,
	dispatcherAddr string,
) (*WorkerNode, error) {
	conn, err := grpc.NewClient(
		dispatcherAddr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create grpc client: %w", err)
	}
	client := pb.NewDispatcherServiceClient(conn)
	stream, err := client.Connect(ctx)

	if err != nil {
		return nil, fmt.Errorf("open dispatcher stream: %w", err)
	}
	caps := make(map[string]struct{}, len(capabilities))

	for _, capability := range capabilities {
		caps[capability] = struct{}{}
	}
	worker := &domain.Worker{
		ID:           id,
		Concurrency:  concurrency,
		Capabilities: caps,
		Jobs:         make(chan *pb.AssignJob, concurrency),
		Events:       make(chan *pb.WorkerEvent, concurrency*2),
		Running:      make(map[string]context.CancelFunc),
	}

	workerNode := &WorkerNode{
		worker:  worker,
		conn:    conn,
		client:  client,
		stream:  stream,
		handler: jobhandler.NewHandler(nil),
	}
	return workerNode, nil
}

func (n *WorkerNode) Close() error {
	return n.conn.Close()
}

func (n *WorkerNode) ReceiveJobs(ctx context.Context) error {
	if n.stream == nil {
		return errors.New("grpc stream is not initialized")
	}

	for {
		command, err := n.stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return fmt.Errorf("receive dispatcher command: %w", err)
		}

		job := command.GetAssignJob()
		if job == nil {
			continue
		}

		select {
		case n.worker.Jobs <- job:

		case <-ctx.Done():
			return ctx.Err()
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
			var err error

			switch job.Type {
			case "api_call":
				err = n.handleAPICall(ctx, job)

			case "image_resize":
				err = n.handleImageResize(ctx, job)

			case "image_convert":
				err = n.handleImageConvert(ctx, job)

			default:
				err = fmt.Errorf("unsupported job type: %s", job.Type)
			}

			n.publishJobResult(ctx, job, err)

		}
	}
}

func (n *WorkerNode) publishJobResult(ctx context.Context, job *pb.AssignJob, jobErr error) {

}
