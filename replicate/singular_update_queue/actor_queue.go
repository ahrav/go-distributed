package singularupdatequeue

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

// Initialize a global executor with a fixed number of worker goroutines.
var (
	numWorkers = runtime.NumCPU()
	executor   = make(chan func(), 1000) // Buffered channel to hold tasks.

	// Initialize the executor by starting worker goroutines.
	// Each worker listens for tasks and executes them as they arrive.
)

func init() {
	for i := 0; i < numWorkers; i++ {
		go func() {
			for task := range executor {
				task()
			}
		}()
	}
}

// Execute submits a task to the global executor for execution.
func Execute(task func()) { executor <- task }

// ActorLikeSingularUpdateQueue manages a queue of requests and processes them in an actor-like manner.
// It ensures that only one task is processed at a time and schedules new tasks as needed.
type ActorLikeSingularUpdateQueue[Req any, Res any] struct {
	workQueue   chan *RequestWrapper[Req, Res]
	handler     func(Req) Res
	isRunning   int32 // Atomic flag indicating if the queue is running, int32 instead of bool for CAS.
	isScheduled int32 // Atomic flag indicating if a task is scheduled for execution.
}

// NewActorLikeSingularUpdateQueue creates a new ActorLikeSingularUpdateQueue with the given handler.
// It initializes the workQueue with a buffer size of 100.
func NewActorLikeSingularUpdateQueue[Req any, Res any](handler func(Req) Res) *ActorLikeSingularUpdateQueue[Req, Res] {
	return &ActorLikeSingularUpdateQueue[Req, Res]{
		workQueue: make(chan *RequestWrapper[Req, Res], 100),
		handler:   handler,
		isRunning: 1, // Set to true.
	}
}

// Submit adds a new request to the queue and schedules execution if necessary.
// It returns a RequestWrapper containing a future to receive the response.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) Submit(request Req) *RequestWrapper[Req, Res] {
	// Check if the queue is running.
	if atomic.LoadInt32(&a.isRunning) == 0 {
		panic("queue is not running")
	}

	// Create a new RequestWrapper for the request.
	wrapper := NewRequestWrapper[Req, Res](request)

	// Enqueue the request.
	a.workQueue <- wrapper

	// Register the task for execution.
	a.registerForExecution()

	return wrapper
}

// registerForExecution schedules the run method for execution if there are pending tasks
// and no task is currently scheduled.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) registerForExecution() {
	// Check if there are tasks in the queue.
	if len(a.workQueue) > 0 {
		// Attempt to set isScheduled to true.
		if a.setAsScheduled() {
			// Submit the run method to the executor.
			Execute(a.run)
		}
	}
}

// setAsScheduled atomically sets the isScheduled flag to true if it was false.
// It returns true if the flag was successfully set, false otherwise.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) setAsScheduled() bool {
	return atomic.CompareAndSwapInt32(&a.isScheduled, 0, 1)
}

// run is the method that processes a single request from the queue.
// It is executed by the global executor.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) run() {
	defer func() {
		// After processing, reset the isScheduled flag.
		atomic.StoreInt32(&a.isScheduled, 0)
		// Attempt to schedule the next task if available.
		a.registerForExecution()
	}()

	// Attempt to take a request from the queue with a timeout.
	wrapper := a.take()

	// If a request is present, process it.
	if wrapper != nil {
		func() {
			// Recover from any panic in the handler to prevent crashing.
			defer func() {
				if r := recover(); r != nil {
					var err error
					switch x := r.(type) {
					case string:
						err = fmt.Errorf(x)
					case error:
						err = x
					default:
						err = fmt.Errorf("unknown panic: %v", x)
					}
					wrapper.CompleteExceptionally(err)
				}
			}()

			// Apply the handler to the request to get the response.
			res := a.handler(wrapper.GetRequest())

			// Complete the future with the response.
			wrapper.Complete(res)
		}()
	}
}

// take attempts to retrieve a RequestWrapper from the workQueue with a 300ms timeout.
// It returns the RequestWrapper if successful, or nil otherwise.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) take() *RequestWrapper[Req, Res] {
	select {
	case wrapper := <-a.workQueue:
		return wrapper
	case <-time.After(300 * time.Millisecond):
		return nil
	}
}

// Shutdown gracefully stops the ActorLikeSingularUpdateQueue by setting the isRunning flag to false.
// It does not immediately terminate any ongoing processing but prevents new submissions.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) Shutdown() {
	atomic.StoreInt32(&a.isRunning, 0)
	// Optionally, close the workQueue to unblock any waiting goroutines.
	close(a.workQueue)
}

// TaskCount returns the current number of tasks in the workQueue.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) TaskCount() int { return len(a.workQueue) }

// IsRunning returns whether the ActorLikeSingularUpdateQueue is currently running.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) IsRunning() bool {
	return atomic.LoadInt32(&a.isRunning) != 0
}

// Start is a no-op in this implementation, similar to Java's empty start method.
// Initialization is handled in the constructor.
func (a *ActorLikeSingularUpdateQueue[Req, Res]) Start() {
	// No operation needed as tasks are scheduled upon submission.
}
