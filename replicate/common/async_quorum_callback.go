package common

import (
	"fmt"
	"sync"

	"github.com/ahrav/go-distributed/replicate/quorum/messages"
)

// Compile-time check to ensure that AsyncQuorumCallback implements RequestCallback.
var _ RequestCallback[messages.GetValueResponse] = (*AsyncQuorumCallback[messages.GetValueResponse])(nil)

// QuorumResult represents the result of the quorum operation.
type QuorumResult[T any] struct {
	Responses map[*InetAddressAndPort]T // Responses from the replicas.
	Err       error                     // Error encountered during the quorum operation.
}

// AsyncQuorumCallback completes the associated future once the quorum predicate succeeds.
type AsyncQuorumCallback[T any] struct {
	totalResponses int     // Total number of responses expected.
	exceptions     []error // List of errors encountered.

	mu               sync.Mutex
	responses        map[*InetAddressAndPort]T // Map of responses from replicas.
	quorumFuture     chan QuorumResult[T]      // Channel to send the quorum result.
	successCondition func(T) bool              // Predicate to check if the response is successful.

	once sync.Once // Ensures the quorum is completed only once.
}

// NewAsyncQuorumCallback initializes a new AsyncQuorumCallback with a default success condition.
func NewAsyncQuorumCallback[T any](totalResponses int) *AsyncQuorumCallback[T] {
	return NewAsyncQuorumCallbackWithCondition(totalResponses, func(_ T) bool {
		return true
	})
}

// NewAsyncQuorumCallbackWithCondition initializes a new AsyncQuorumCallback with a specific success condition.
func NewAsyncQuorumCallbackWithCondition[T any](totalResponses int, successCondition func(T) bool) *AsyncQuorumCallback[T] {
	if totalResponses <= 0 {
		panic("totalResponses must be greater than 0")
	}
	return &AsyncQuorumCallback[T]{
		totalResponses:   totalResponses,
		exceptions:       make([]error, 0),
		responses:        make(map[*InetAddressAndPort]T),
		quorumFuture:     make(chan QuorumResult[T], 1),
		successCondition: successCondition,
	}
}

// OnResponse handles a successful response from a replica.
func (aqc *AsyncQuorumCallback[T]) OnResponse(response T, fromAddress *InetAddressAndPort) {
	aqc.mu.Lock()
	aqc.responses[fromAddress] = response
	if aqc.quorumSucceeded() {
		aqc.completeQuorum()
	}
	aqc.mu.Unlock()
}

// OnError handles an error from a replica.
func (aqc *AsyncQuorumCallback[T]) OnError(err error) {
	aqc.mu.Lock()
	aqc.exceptions = append(aqc.exceptions, err)
	if aqc.quorumFailed() {
		aqc.completeQuorumWithError(fmt.Errorf("quorum condition not met after %d responses", aqc.totalResponses))
	}
	aqc.mu.Unlock()
}

// quorumSucceeded checks if the quorum condition is met.
func (aqc *AsyncQuorumCallback[T]) quorumSucceeded() bool {
	successCount := 0
	for _, response := range aqc.responses {
		if aqc.successCondition(response) {
			successCount++
			if successCount >= aqc.majorityQuorum() {
				return true
			}
		}
	}
	return false
}

// quorumFailed checks if all responses have been received and quorum condition is not met.
func (aqc *AsyncQuorumCallback[T]) quorumFailed() bool {
	return len(aqc.responses)+len(aqc.exceptions) == aqc.totalResponses && !aqc.quorumSucceeded()
}

// majorityQuorum calculates the majority quorum.
func (aqc *AsyncQuorumCallback[T]) majorityQuorum() int {
	return aqc.totalResponses/2 + 1
}

// completeQuorum completes the quorumFuture with the responses.
func (aqc *AsyncQuorumCallback[T]) completeQuorum() {
	aqc.once.Do(func() {
		// Make a copy to avoid race conditions.
		copyResponses := make(map[*InetAddressAndPort]T, len(aqc.responses))
		for k, v := range aqc.responses {
			copyResponses[k] = v
		}
		aqc.quorumFuture <- QuorumResult[T]{Responses: copyResponses, Err: nil}
		close(aqc.quorumFuture)
	})
}

// completeQuorumWithError completes the quorumFuture with an error.
func (aqc *AsyncQuorumCallback[T]) completeQuorumWithError(err error) {
	aqc.once.Do(func() {
		aqc.quorumFuture <- QuorumResult[T]{Responses: nil, Err: err}
		close(aqc.quorumFuture)
	})
}

// GetQuorumFuture returns the channel that will receive the quorum responses or an error.
func (aqc *AsyncQuorumCallback[T]) GetQuorumFuture() <-chan QuorumResult[T] {
	return aqc.quorumFuture
}
