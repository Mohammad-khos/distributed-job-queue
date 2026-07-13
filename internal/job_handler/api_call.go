package jobhandler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type APICallRequest struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    []byte
	Timeout time.Duration
}

type APICallResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	Duration   time.Duration
}

func (h *Handler) CallPublicAPI(ctx context.Context, req APICallRequest) (*APICallResponse, error) {
	if req.URL == "" {
		return nil, fmt.Errorf("jobhandler: api request url is required")
	}

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}

	client := h.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	body := bytes.NewReader(req.Body)
	rq, err := http.NewRequestWithContext(ctx, method, req.URL, body)
	if err != nil {
		return nil, err
	}

	for key, value := range req.Headers {
		rq.Header.Set(key, value)
	}

	start := time.Now()
	resp, err := client.Do(rq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &APICallResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       respBody,
		Duration:   time.Since(start),
	}, nil
}
