package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/mohammad-khos/distributed-job-queue/internal/dispatcher"
	"github.com/mohammad-khos/distributed-job-queue/internal/infra/repo"
	"github.com/mohammad-khos/distributed-job-queue/pkg/db"
	"github.com/mohammad-khos/distributed-job-queue/pkg/env"
	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	defaultGRPCAddress      = ":50051"
	defaultDispatcherTick   = time.Second
	gracefulShutdownTimeout = 10 * time.Second
)

func main() {
	_ = godotenv.Load()

	logger := zap.Must(zap.NewProduction()).Sugar()
	defer func() { _ = logger.Sync() }()

	if err := run(logger); err != nil {
		logger.Errorw("dispatcher stopped", "error", err)
		_ = logger.Sync()
		os.Exit(1)
	}
}

func run(logger *zap.SugaredLogger) error {
	signalContext, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	ctx, cancel := context.WithCancel(signalContext)
	defer cancel()

	database, err := db.New(
		env.GetString("DB_URI", ""),
		env.GetInt("DB_MAX_OPEN_CONNS", 30),
		env.GetInt("DB_MAX_IDLE_CONNS", 30),
		env.GetString("DB_MAX_IDLE_TIME", "15m"),
	)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}

	sqlDB, err := database.DB()
	if err != nil {
		return fmt.Errorf("get database connection: %w", err)
	}
	defer sqlDB.Close()

	repository := repo.NewPostgresRepository(database)
	dispatcherNode := dispatcher.NewDispatcher(
		durationFromEnv("DISPATCHER_TICK_INTERVAL", defaultDispatcherTick),
		repository,
	)

	grpcServer := grpc.NewServer()
	pb.RegisterDispatcherServiceServer(grpcServer, dispatcherNode)

	address := env.GetString("DISPATCHER_GRPC_ADDR", defaultGRPCAddress)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	defer listener.Close()

	go dispatcherNode.CheckDB(ctx)
	go dispatcherNode.Scheduler(ctx)

	serveErrors := make(chan error, 1)
	go func() {
		logger.Infow("dispatcher started", "addr", address)
		serveErrors <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		logger.Info("dispatcher shutdown requested")
	case serveErr := <-serveErrors:
		cancel()
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			return fmt.Errorf("serve grpc: %w", serveErr)
		}
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(gracefulShutdownTimeout):
		grpcServer.Stop()
	}

	return nil
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	value := env.GetString(key, "")
	if value == "" {
		return fallback
	}

	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}

	return duration
}
