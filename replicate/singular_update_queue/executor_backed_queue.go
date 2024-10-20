package singularupdatequeue

import "sync"

// ExecutorBackedQueue manages the processing of requests using a single-threaded executor.
// It applies an update handler to each request and optionally forwards the result to the next queue.
type ExecutorBackedQueue[P any, R any] struct {
	updateHandler          func(P) R
	next                   *SingularUpdateQueue[R, any]
	singleThreadedExecutor chan P
	quit                   chan struct{}
	wg                     sync.WaitGroup
}

// NewExecutorBackedQueue creates a new ExecutorBackedQueue with the given update handler.
// It initializes the single-threaded executor with a buffered channel.
func NewExecutorBackedQueue[P any, R any](updateHandler func(P) R) *ExecutorBackedQueue[P, R] {
	return &ExecutorBackedQueue[P, R]{
		updateHandler:          updateHandler,
		singleThreadedExecutor: make(chan P, 100), // Buffer size of 100.
		quit:                   make(chan struct{}),
	}
}

// NewExecutorBackedQueueWithNext creates a new ExecutorBackedQueue with the given update handler
// and a reference to the next SingularUpdateQueue.
func NewExecutorBackedQueueWithNext[P any, R any](
	updateHandler func(P) R,
	next *SingularUpdateQueue[R, any],
) *ExecutorBackedQueue[P, R] {
	return &ExecutorBackedQueue[P, R]{
		updateHandler:          updateHandler,
		next:                   next,
		singleThreadedExecutor: make(chan P, 100), // Buffer size of 100
		quit:                   make(chan struct{}),
	}
}

// Start initializes the single-threaded executor and begins processing incoming requests.
func (e *ExecutorBackedQueue[P, R]) Start() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case request := <-e.singleThreadedExecutor:
				// Process the request using the update handler.
				update := e.updateHandler(request)

				// If a next queue is set, submit the result to it.
				if e.next != nil {
					e.next.Submit(update)
				}
			case <-e.quit:
				// Gracefully exit the goroutine.
				return
			}
		}
	}()
}

// Submit sends a new request to the single-threaded executor for processing.
func (e *ExecutorBackedQueue[P, R]) Submit(request P) {
	select {
	case e.singleThreadedExecutor <- request:
		// Request submitted successfully.
	default:
		// Handle the case where the executor's queue is full
		// We can choose to block, drop the request, or handle it as needed
		// Here, we'll block until there's space
		e.singleThreadedExecutor <- request
	}
}

// Shutdown gracefully stops the executor by signaling the goroutine to quit
// and waits for it to finish processing any ongoing requests.
func (e *ExecutorBackedQueue[P, R]) Shutdown() {
	close(e.quit)
	e.wg.Wait()
}
