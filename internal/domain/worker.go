package domain

import (
	"context"
	"time"

	// "google.golang.org/grpc"
)
const CommandAssignJob = "assign_job"


type Worker struct {
	ID           string
	Capabilities map[string]struct{}
	Concurrency  int

	// صف داخلی jobها
	// jobs chan *pb.AssignJob

	// پیام‌هایی که باید برای Dispatcher ارسال شوند
	// events chan *pb.WorkerEvent

	// محدود کردن تعداد jobهای هم‌زمان
	slots chan struct{}

	// اتصال gRPC
	// conn *grpc.ClientConn
	// client pb.DispatcherServiceClient

	// jobهای در حال اجرا و cancel function آن‌ها
	// mu      sync.RWMutex
	running map[string]context.CancelFunc

	// // مدیریت lifecycle goroutineها
	// wg sync.WaitGroup
}

type WorkerSession struct {
	ID           string
	Capabilities []string
	Concurrency  int
	// ReservedJobs   int
	RunningJobs    int
	AvailableSlots int
	LastHeartbeat  time.Time
	Status         string
}

type DispatcherCommand struct {
	Type string
	Job  *Job
}

type Heartbeat struct {
	WorkerID       string
	RunningJobs    int
	AvailableSlots int
	SentAt         time.Time
}
