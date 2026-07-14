package worker

import (
	"context"

	jobhandler "github.com/mohammad-khos/distributed-job-queue/internal/job_handler"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
)

type JobProcessor interface {
	Process(ctx context.Context, job *pb.AssignJob) ([]byte, error)
}


type JobHandler interface {
	CallPublicAPI(
		ctx context.Context,
		req jobhandler.APICallRequest,
	) (*jobhandler.APICallResponse, error)

	ResizeImage(
		ctx context.Context,
		req jobhandler.ImageResizeRequest,
	) ([]byte, error)

	ConvertImage(
		ctx context.Context,
		req jobhandler.ImageConvertRequest,
	) ([]byte, error)
}