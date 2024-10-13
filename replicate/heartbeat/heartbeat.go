package heartbeat

import (
	"context"
	"log"
	"sync"
	"time"
)

// Scheduler is a struct that manages the execution of a scheduled task at fixed intervals.
// The Scheduler struct contains the following fields:
//
// - action: a function that will be executed at the specified interval
// - heartBeatInterval: the duration between each execution of the action function
// - cancel: a context.CancelFunc that can be used to stop the scheduled task
// - mutex: a sync.Mutex used to protect the Scheduler's state
type Scheduler struct {
	action            func()
	heartBeatInterval time.Duration
	cancel            context.CancelFunc
	mutex             sync.Mutex
}

// NewHeartBeatScheduler creates a new instance of HeartBeatScheduler.
func NewHeartBeatScheduler(action func(), heartBeatIntervalMs int64) *Scheduler {
	return &Scheduler{
		action:            action,
		heartBeatInterval: time.Duration(heartBeatIntervalMs) * time.Millisecond,
	}
}

// Start begins the heartbeat scheduler, executing the action at fixed intervals.
func (h *Scheduler) Start() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.cancel != nil {
		log.Fatal("Scheduled task should be stopped before starting again")
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	go func(ctx context.Context) {
		ticker := time.NewTicker(h.heartBeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Println("Stopped scheduled task")
				return
			case <-ticker.C:
				h.action()
			}
		}
	}(ctx)
}

// Stop stops the heartbeat scheduler.
func (h *Scheduler) Stop() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
}

// Restart stops and starts the heartbeat scheduler.
func (h *Scheduler) Restart() {
	h.Stop()
	h.Start()
}
