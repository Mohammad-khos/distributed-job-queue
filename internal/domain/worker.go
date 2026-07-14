package domain

import (
	"context"
	"sync"
	"time"

	pb "github.com/mohammad-khos/distributed-job-queue/shared/proto"
)

const (
	CommandAssignJob  = "assign_job"
	WorkerStatusReady = "ready"
)

type Worker struct {
	ID           string
	Capabilities map[string]struct{}
	Concurrency  int
	Jobs         chan *pb.AssignJob
	Events       chan *pb.WorkerEvent
	Mu           sync.RWMutex
	Running      map[string]context.CancelFunc
	Wg           sync.WaitGroup
}

type WorkerSession struct {
	ID             string
	Capabilities   []string
	Concurrency    int
	RunningJobs    int
	AvailableSlots int
	LastHeartbeat  time.Time
	Status         string

	Outbound      chan *DispatcherCommand
	Done          chan struct{}
	ReservedJobs  map[string]struct{}
	RunningJobIDs map[string]struct{}
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
