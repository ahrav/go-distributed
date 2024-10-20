package singularupdatequeue

import (
	"fmt"
	"sync/atomic"
	"time"
)

// SingularUpdateQueue manages a queue of requests and processes them singularly
// using a provided handler function. It supports submitting requests and shutting
// down the queue gracefully.
type SingularUpdateQueue[Req any, Res any] struct {
	workQueue chan *RequestWrapper[Req, Res]
	handler   func(Req) Res
	isRunning atomic.Bool
}

// NewSingularUpdateQueue creates a new SingularUpdateQueue with the given handler.
// It initializes the workQueue with a buffer size of 100 and starts the processing
// goroutine.
func NewSingularUpdateQueue[Req any, Res any](handler func(Req) Res) *SingularUpdateQueue[Req, Res] {
	queue := &SingularUpdateQueue[Req, Res]{
		workQueue: make(chan *RequestWrapper[Req, Res], 100),
		handler:   handler,
	}
	queue.isRunning.Store(true)
	go queue.run()
	return queue
}

// Submit adds a new request to the queue and returns a RequestWrapper containing
// a future to receive the response. If the queue is not running, it returns an error.
func (q *SingularUpdateQueue[Req, Res]) Submit(request Req) (*RequestWrapper[Req, Res], error) {
	if !q.isRunning.Load() {
		return nil, fmt.Errorf("queue is not running")
	}
	wrapper := NewRequestWrapper[Req, Res](request)
	q.workQueue <- wrapper
	return wrapper, nil
}

// run is the internal method that continuously processes requests from the workQueue.
// It applies the handler to each request and completes the corresponding future with
// the result or an error if the handler panics.
func (q *SingularUpdateQueue[Req, Res]) run() {
	for q.isRunning.Load() {
		select {
		case wrapper := <-q.workQueue:
			if wrapper != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							err := fmt.Errorf("handler panic: %v", r)
							wrapper.CompleteExceptionally(err)
						}
					}()
					// Process the request using the handler.
					res := q.handler(wrapper.GetRequest())
					// Complete the future with the result.
					wrapper.Complete(res)
				}()
			}
		case <-time.After(2 * time.Millisecond):
			// Timeout after 2ms to allow checking isRunning flag periodically.
		}
	}
}

// Shutdown gracefully stops the SingularUpdateQueue. It sets the isRunning flag to false,
// allowing the run loop to exit after processing any ongoing request.
func (q *SingularUpdateQueue[Req, Res]) Shutdown() { q.isRunning.Store(false) }

// TaskCount returns the current number of tasks in the workQueue.
func (q *SingularUpdateQueue[Req, Res]) TaskCount() int { return len(q.workQueue) }

// IsRunning returns whether the SingularUpdateQueue is currently running.
func (q *SingularUpdateQueue[Req, Res]) IsRunning() bool { return q.isRunning.Load() }
