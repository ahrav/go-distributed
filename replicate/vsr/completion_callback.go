package vsr

import (
	"sync"

	"github.com/ahrav/go-distributed/replicate/common"
)

// CompletionCallback bridges callback-based asynchronous responses to channels.
// It implements the RequestCallback[T] interface, allowing it to receive responses
// or errors from asynchronous operations and forward them through a channel.
// This enables consumers to wait for and handle the result using Go's channel mechanisms.
//
// Use CompletionCallback when you expect a single response or error from an asynchronous
// operation and want to handle it in a synchronous-like manner using channels.
type CompletionCallback[T any] struct {
	// once ensures that the result is sent through the channel only once,
	// preventing multiple sends which can cause panics.
	once sync.Once

	// result is a buffered channel that carries the QuorumResult[T].
	// It is buffered with a capacity of 1 to allow the sender to proceed
	// without blocking if the receiver isn't immediately ready.
	result chan common.CompletionResult[T]
}

// NewCompletionCallback initializes a new CompletionCallback.
// It creates a buffered channel to hold the result, allowing asynchronous operations
// to send the result without being blocked.
func NewCompletionCallback[T any]() *CompletionCallback[T] {
	return &CompletionCallback[T]{
		result: make(chan common.CompletionResult[T], 1), // Buffered to prevent goroutine leaks
	}
}

// OnResponse handles a successful response from an asynchronous operation.
// It sends the result through the channel and ensures that the channel is closed
// after sending to signal completion. The use of sync.Once guarantees that
// the result is sent only once, even if OnResponse is called multiple times.
func (cc *CompletionCallback[T]) OnResponse(response T, fromNode *common.InetAddressAndPort) {
	cc.once.Do(func() {
		cc.result <- common.CompletionResult[T]{Value: response, Err: nil}
		close(cc.result)
	})
}

// OnError handles an error from an asynchronous operation.
// It sends the error through the channel encapsulated in QuorumResult[T] and closes the channel.
// The sync.Once mechanism ensures that only the first error or response is sent,
// preventing multiple sends which would lead to runtime panics.
func (cc *CompletionCallback[T]) OnError(err error) {
	cc.once.Do(func() {
		var zero T // Initialize T to its zero value
		cc.result <- common.CompletionResult[T]{Value: zero, Err: err}
		close(cc.result)
	})
}

// GetFuture returns a read-only channel that will receive the QuorumResult[T].
// Consumers can receive from this channel to obtain the result or handle errors.
// This method allows integration with Go's select statements and other concurrency patterns.
func (cc *CompletionCallback[T]) GetFuture() <-chan common.CompletionResult[T] {
	return cc.result
}
