package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	jobhandler "github.com/mohammad-khos/distributed-job-queue/internal/job_handler"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
)

const (
	JobTypeAPICall      = "api_call"
	JobTypeImageResize  = "image_resize"
	JobTypeImageConvert = "image_convert"
)

type jobProcessor struct {
	handler JobHandler
}

type apiCallPayload struct {
	Method         string            `json:"method"`
	URL            string            `json:"url"`
	Headers        map[string]string `json:"headers"`
	Body           []byte            `json:"body"`
	TimeoutSeconds int               `json:"timeout_seconds"`
}

type imageResizePayload struct {
	Data            []byte `json:"data"`
	Width           int    `json:"width"`
	Height          int    `json:"height"`
	KeepAspectRatio bool   `json:"keep_aspect_ratio"`
	OutputFormat    string `json:"output_format"`
}

type imageConvertPayload struct {
	Data   []byte `json:"data"`
	Format string `json:"format"`
}


func NewJobProcessor(handler JobHandler) JobProcessor {
	return &jobProcessor{
		handler: handler,
	}
}

func (p *jobProcessor) Process(
	ctx context.Context,
	job *pb.AssignJob,
) ([]byte, error) {
	if job == nil {
		return nil, errors.New("worker: assigned job is nil")
	}

	if p == nil || p.handler == nil {
		return nil, errors.New("worker: job handler is not configured")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	switch strings.TrimSpace(job.GetType()) {
	case JobTypeAPICall:
		return p.processAPICall(ctx, job)

	case JobTypeImageResize:
		return p.processImageResize(ctx, job)

	case JobTypeImageConvert:
		return p.processImageConvert(ctx, job)

	default:
		return nil, fmt.Errorf(
			"worker: unsupported job type %q",
			job.GetType(),
		)
	}
}

func (p *jobProcessor) processAPICall(
	ctx context.Context,
	job *pb.AssignJob,
) ([]byte, error) {
	var payload apiCallPayload

	if err := decodeJobPayload(job, &payload); err != nil {
		return nil, err
	}

	timeoutSeconds := payload.TimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = int(job.GetTimeoutSeconds())
	}

	request := jobhandler.APICallRequest{
		Method:  payload.Method,
		URL:     payload.URL,
		Headers: payload.Headers,
		Body:    payload.Body,
	}

	if timeoutSeconds > 0 {
		request.Timeout = time.Duration(timeoutSeconds) * time.Second
	}

	response, err := p.handler.CallPublicAPI(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("execute api_call job: %w", err)
	}

	if response == nil {
		return nil, errors.New(
			"execute api_call job: handler returned nil response",
		)
	}

	if response.StatusCode >= http.StatusBadRequest {
		return response.Body, fmt.Errorf(
			"api_call returned status code %d",
			response.StatusCode,
		)
	}

	return response.Body, nil
}

func (p *jobProcessor) processImageResize(
	ctx context.Context,
	job *pb.AssignJob,
) ([]byte, error) {
	var payload imageResizePayload

	if err := decodeJobPayload(job, &payload); err != nil {
		return nil, err
	}

	output, err := p.handler.ResizeImage(
		ctx,
		jobhandler.ImageResizeRequest{
			Data:            payload.Data,
			Width:           payload.Width,
			Height:          payload.Height,
			KeepAspectRatio: payload.KeepAspectRatio,
			OutputFormat:    payload.OutputFormat,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("execute image_resize job: %w", err)
	}

	return output, nil
}

func (p *jobProcessor) processImageConvert(
	ctx context.Context,
	job *pb.AssignJob,
) ([]byte, error) {
	var payload imageConvertPayload

	if err := decodeJobPayload(job, &payload); err != nil {
		return nil, err
	}

	output, err := p.handler.ConvertImage(
		ctx,
		jobhandler.ImageConvertRequest{
			Data:   payload.Data,
			Format: payload.Format,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("execute image_convert job: %w", err)
	}

	return output, nil
}

func decodeJobPayload(job *pb.AssignJob, destination any) error {
	if job == nil {
		return errors.New("worker: assigned job is nil")
	}

	payload := job.GetPayload()
	if len(payload) == 0 {
		return fmt.Errorf(
			"worker: payload for job type %q is required",
			job.GetType(),
		)
	}

	if destination == nil {
		return errors.New(
			"worker: payload destination cannot be nil",
		)
	}

	if err := json.Unmarshal(payload, destination); err != nil {
		return fmt.Errorf(
			"decode %s job payload: %w",
			job.GetType(),
			err,
		)
	}

	return nil
}