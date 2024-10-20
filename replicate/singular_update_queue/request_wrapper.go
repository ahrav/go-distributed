package singularupdatequeue

import "fmt"

// Result represents the outcome of an asynchronous operation,
// containing either a value of type Res or an error.
type Result[Res any] struct {
	Value Res
	Err   error
}

// RequestWrapper wraps a request of type Req and provides a channel
// to receive the asynchronous result of type Res.
type RequestWrapper[Req any, Res any] struct {
	request Req
	future  chan Result[Res]
}

// NewRequestWrapper creates a new RequestWrapper with the given request.
// It initializes the future channel, which is buffered to prevent goroutine leaks.
func NewRequestWrapper[Req any, Res any](request Req) *RequestWrapper[Req, Res] {
	return &RequestWrapper[Req, Res]{
		request: request,
		future:  make(chan Result[Res], 1), // Buffered channel to ensure non-blocking sends
	}
}

// GetFuture returns a read-only channel that can be used to receive the Result.
func (rw *RequestWrapper[Req, Res]) GetFuture() <-chan Result[Res] {
	return rw.future
}

// Complete sends a successful result to the future channel.
func (rw *RequestWrapper[Req, Res]) Complete(response Res) {
	rw.future <- Result[Res]{Value: response, Err: nil}
	close(rw.future) // Close the channel to signal that no more values will be sent
}

// CompleteExceptionally sends an error to the future channel.
func (rw *RequestWrapper[Req, Res]) CompleteExceptionally(err error) {
	fmt.Println("Error:", err)
	rw.future <- Result[Res]{Value: *new(Res), Err: err}
	close(rw.future) // Close the channel to signal that no more values will be sent
}

// GetRequest returns the original request.
func (rw *RequestWrapper[Req, Res]) GetRequest() Req { return rw.request }
