package dispatcher

import (
	"context"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
)

const heartbeatTimeout = time.Minute * 30

type Dispatcher struct {
	tickInterval time.Duration
	repo         domain.Repository
	pendingJobs  chan *domain.Job

	registryMu     sync.RWMutex
	workerRegistry map[string]*domain.WorkerSession
	Outbound       chan *domain.DispatcherCommand
}

func NewDispatcher(
	tickInterval time.Duration,
	repo domain.Repository,
) *Dispatcher {
	return &Dispatcher{
		tickInterval:   tickInterval,
		repo:           repo,
		pendingJobs:    make(chan *domain.Job, 10),
		workerRegistry: make(map[string]*domain.WorkerSession),
		Outbound:       make(chan *domain.DispatcherCommand),
	}
}

func (d *Dispatcher) CheckDB(ctx context.Context) {
	ticker := time.NewTicker(d.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			jobs, err := d.repo.ClaimQueuedJobs(ctx, 10)
			if err != nil {
				log.Printf("claim queued jobs: %v", err)
				continue
			}

			for _, job := range jobs {
				select {
				case d.pendingJobs <- job:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (d *Dispatcher) HandleHeartbeat(h *domain.Heartbeat) {
	d.registryMu.Lock()
	defer d.registryMu.Unlock()

	worker, exists := d.workerRegistry[h.WorkerID]
	if !exists {
		return
	}

	worker.RunningJobs = h.RunningJobs
	worker.AvailableSlots = h.AvailableSlots
	worker.LastHeartbeat = time.Now()
	worker.Status = "ready"
}

func (d *Dispatcher) SendLoop(ctx context.Context) {
	// for {
	// 	select {
	// 	case <-ctx.Done():
	// 		return
	// 	// case cmd := <-d.Outbound:
	// 		// Send to gRPC Connection : d.conn.Send(cmd)
	// 	}
	// }
}

func (d *Dispatcher) Scheduler(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-d.pendingJobs:
			if !ok {
				return
			}
			d.registryMu.Lock()

			var availableWorker *domain.WorkerSession

			//schedule based on last heartbeat , capability , available slots and status
			for _, worker := range d.workerRegistry {
				if (time.Since(worker.LastHeartbeat) <= heartbeatTimeout) && worker.AvailableSlots > 0 && slices.Contains(worker.Capabilities, job.Type) && worker.Status == "ready" {
					worker.AvailableSlots--
					//add reserve job instaed run it later
					worker.RunningJobs++
					availableWorker = worker
					break
				}
			}
			d.registryMu.Unlock()

			if availableWorker == nil {
				//add retry logic later
				log.Println("no available worker found")
				continue
			}

			cmd := &domain.DispatcherCommand{
				Type: domain.CommandAssignJob,
				Job:  job,
			}
			select {
			case <-ctx.Done():
				d.registryMu.Lock()
				availableWorker.AvailableSlots++
				availableWorker.RunningJobs--
				d.registryMu.Unlock()
				return
			case d.Outbound <- cmd:
			}
		}
	}

}
