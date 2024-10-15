package common

import (
	"log"
	"sync"
	"time"
)

// Compile time check to ensure that BlockingQuorumCallback implements RequestCallback.
var _ RequestCallback[any] = (*BlockingQuorumCallback[any])(nil)

// BlockingQuorumCallback handles quorum-based responses and allows blocking until quorum is met.
type BlockingQuorumCallback[T any] struct {
	logger         *log.Logger
	quorum         int // Number of responses required to reach quorum.
	totalResponses int // Total number of responses expected.

	exceptions []error                   // List of errors encountered.
	responses  map[*InetAddressAndPort]T // Map of responses from replicas.

	mu            sync.Mutex    // Mutex to protect shared resources.
	quorumReached chan struct{} // Channel to signal when quorum is reached.
	once          sync.Once     // Ensures quorum is signaled only once.
}

// NewBlockingQuorumCallback initializes a new BlockingQuorumCallback.
// - totalResponses: The total number of responses expected.
// - logger: A logger instance for logging events.
func NewBlockingQuorumCallback[T any](totalResponses int, logger *log.Logger) *BlockingQuorumCallback[T] {
	quorum := totalResponses/2 + 1
	return &BlockingQuorumCallback[T]{
		logger:         logger,
		quorum:         quorum,
		totalResponses: totalResponses,
		exceptions:     make([]error, 0),
		responses:      make(map[*InetAddressAndPort]T),
		quorumReached:  make(chan struct{}),
	}
}

// OnResponse handles a successful response from a replica.
// It logs the response, stores it, and checks if quorum is reached.
func (bqc *BlockingQuorumCallback[T]) OnResponse(response T, address *InetAddressAndPort) {
	bqc.logger.Printf("Received %v from %v", response, address)
	bqc.mu.Lock()
	bqc.responses[address] = response
	currentResponses := len(bqc.responses)
	bqc.mu.Unlock()

	if currentResponses >= bqc.quorum {
		bqc.once.Do(func() {
			close(bqc.quorumReached)
		})
	}
}

// OnError handles an error from a replica.
// It stores the error and checks if quorum of errors is reached without sufficient successful responses.
func (bqc *BlockingQuorumCallback[T]) OnError(err error) {
	bqc.mu.Lock()
	bqc.exceptions = append(bqc.exceptions, err)
	totalReceived := len(bqc.responses) + len(bqc.exceptions)
	bqc.mu.Unlock()

	if totalReceived >= bqc.quorum && len(bqc.responses) < bqc.quorum {
		bqc.once.Do(func() {
			close(bqc.quorumReached)
		})
	}
}

// BlockAndGetQuorumResponses waits until quorum is met or a timeout occurs.
// It returns the collected successful responses.
func (bqc *BlockingQuorumCallback[T]) BlockAndGetQuorumResponses() map[*InetAddressAndPort]T {
	select {
	case <-bqc.quorumReached:
		// Quorum reached.
		bqc.logger.Println("BlockingQuorumCallback: quorum reached")
	case <-time.After(5 * time.Second):
		// Timeout.
		bqc.logger.Println("BlockingQuorumCallback: timeout reached while waiting for quorum")
	}
	bqc.mu.Lock()
	defer bqc.mu.Unlock()
	return bqc.responses
}
