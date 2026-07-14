package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	jobhandler "github.com/mohammad-khos/distributed-job-queue/internal/job_handler"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
)

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

func (n *WorkerNode) handleAPICall(ctx context.Context, job *pb.AssignJob) error {
	var payload apiCallPayload
	if err := decodeJobPayload(job, &payload); err != nil {
		return err
	}

	timeoutSeconds := payload.TimeoutSeconds
	if timeoutSeconds == 0 {
		timeoutSeconds = int(job.GetTimeoutSeconds())
	}

	req := jobhandler.APICallRequest{
		Method:  payload.Method,
		URL:     payload.URL,
		Headers: payload.Headers,
		Body:    payload.Body,
	}
	if timeoutSeconds > 0 {
		req.Timeout = time.Duration(timeoutSeconds) * time.Second
	}

	resp, err := n.handler.CallPublicAPI(ctx, req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("api_call returned status code %d", resp.StatusCode)
	}

	return nil
}

func (n *WorkerNode) handleImageResize(ctx context.Context, job *pb.AssignJob) error {
	var payload imageResizePayload
	if err := decodeJobPayload(job, &payload); err != nil {
		return err
	}

	_, err := n.handler.ResizeImage(ctx, jobhandler.ImageResizeRequest{
		Data:            payload.Data,
		Width:           payload.Width,
		Height:          payload.Height,
		KeepAspectRatio: payload.KeepAspectRatio,
		OutputFormat:    payload.OutputFormat,
	})
	return err
}

func (n *WorkerNode) handleImageConvert(ctx context.Context, job *pb.AssignJob) error {
	var payload imageConvertPayload
	if err := decodeJobPayload(job, &payload); err != nil {
		return err
	}

	_, err := n.handler.ConvertImage(ctx, jobhandler.ImageConvertRequest{
		Data:   payload.Data,
		Format: payload.Format,
	})
	return err
}



func decodeJobPayload(job *pb.AssignJob, dst any) error {
	payload := job.GetPayload()
	if len(payload) == 0 {
		return fmt.Errorf("%s job payload is required", job.GetType())
	}
	if err := json.Unmarshal(payload, dst); err != nil {
		return fmt.Errorf("decode %s job payload: %w", job.GetType(), err)
	}
	return nil
}
