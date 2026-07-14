package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/joho/godotenv"
	jobhandler "github.com/mohammad-khos/distributed-job-queue/internal/job_handler"
	"github.com/mohammad-khos/distributed-job-queue/internal/worker"
	"github.com/mohammad-khos/distributed-job-queue/pkg/env"
	"go.uber.org/zap"
)

const (
	workerConcurrency       = 4
	defaultDispatcherTarget = "localhost:50051"
)

func main() {
	_ = godotenv.Load()

	logger := zap.Must(zap.NewProduction()).Sugar()
	defer func() { _ = logger.Sync() }()

	if err := run(logger); err != nil {
		logger.Errorw("worker stopped", "error", err)
		_ = logger.Sync()
		os.Exit(1)
	}
}

func run(logger *zap.SugaredLogger) error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	jobHandler := jobhandler.NewHandler(&http.Client{})
	processor := worker.NewJobProcessor(jobHandler)

	workerID := strings.TrimSpace(env.GetString("WORKER_ID", ""))
	if workerID == "" {
		workerID = defaultWorkerID()
	}

	dispatcherTarget := env.GetString(
		"DISPATCHER_GRPC_TARGET",
		defaultDispatcherTarget,
	)

	node, err := worker.NewWorkerNode(
		ctx,
		workerID,
		workerConcurrency,
		[]string{
			worker.JobTypeAPICall,
			worker.JobTypeImageResize,
			worker.JobTypeImageConvert,
		},
		dispatcherTarget,
		processor,
	)
	if err != nil {
		return fmt.Errorf("create worker node: %w", err)
	}
	defer node.Close()

	logger.Infow(
		"worker started",
		"worker_id", workerID,
		"dispatcher", dispatcherTarget,
		"concurrency", workerConcurrency,
	)

	if err := node.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run worker node: %w", err)
	}

	return nil
}

func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "worker-1"
	}

	return hostname
}
