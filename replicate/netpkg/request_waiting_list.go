package netpkg

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/ahrav/go-distributed/replicate/common"
)

// RequestWaitingList manages pending requests and handles their callbacks.
type RequestWaitingList[Key comparable, Response any] struct {
	mu                 sync.RWMutex
	pendingRequests    map[Key]*CallbackDetails[Response]
	clock              *common.SystemClock
	expirationDuration time.Duration
	executor           *time.Ticker
	stopChan           chan struct{}
	logger             *log.Logger
	AddressAndPort     *common.InetAddressAndPort
}

// NewRequestWaitingList initializes and returns a new RequestWaitingList instance.
func NewRequestWaitingList[Key comparable, Response any](
	clock *common.SystemClock,
	expirationDuration time.Duration,
	logger *log.Logger,
) *RequestWaitingList[Key, Response] {
	rwl := &RequestWaitingList[Key, Response]{
		pendingRequests:    make(map[Key]*CallbackDetails[Response]),
		clock:              clock,
		expirationDuration: expirationDuration,
		executor:           time.NewTicker(expirationDuration),
		stopChan:           make(chan struct{}),
		logger:             logger,
		AddressAndPort:     nil,
	}

	go rwl.expirationWorker()

	return rwl
}

// expirationWorker runs in a goroutine and periodically checks for expired requests.
func (rwl *RequestWaitingList[Key, Response]) expirationWorker() {
	for {
		select {
		case <-rwl.executor.C:
			rwl.expire()
		case <-rwl.stopChan:
			rwl.executor.Stop()
			return
		}
	}
}

// Add registers a new request with the given key and callback.
func (rwl *RequestWaitingList[Key, Response]) Add(key Key, callback RequestCallback[Response]) {
	now := rwl.clock.NanoTime()
	rwl.logger.Printf("RequestWaitingList adding key %v at %d", key, now)

	rwl.mu.Lock()
	defer rwl.mu.Unlock()

	rwl.pendingRequests[key] = NewCallbackDetails(callback, now)
}

// expire checks for expired requests and invokes their error callbacks.
func (rwl *RequestWaitingList[Key, Response]) expire() {
	expiredKeys := rwl.getExpiredRequestKeys()
	if len(expiredKeys) == 0 {
		return
	}

	rwl.logger.Printf("Expiring keys: %v", expiredKeys)

	for _, key := range expiredKeys {
		rwl.mu.Lock()
		cb, exists := rwl.pendingRequests[key]
		if exists {
			delete(rwl.pendingRequests, key)
		}
		rwl.mu.Unlock()

		if exists {
			cb.RequestCallback.OnError(errors.New("request expired"))
		}
	}
}

// getExpiredRequestKeys retrieves the keys of all expired requests.
func (rwl *RequestWaitingList[Key, Response]) getExpiredRequestKeys() []Key {
	now := rwl.clock.NanoTime()
	var expired []Key

	rwl.mu.RLock()
	defer rwl.mu.RUnlock()

	for key, cb := range rwl.pendingRequests {
		if cb.IsExpired(rwl.expirationDuration, now) {
			expired = append(expired, key)
		}
	}

	return expired
}

// HandleResponse processes a received response for the given key.
func (rwl *RequestWaitingList[Key, Response]) HandleResponse(key Key, response Response) {
	rwl.logger.Printf("RequestWaitingList received response for key %v at %d", key, rwl.clock.NanoTime())

	rwl.mu.Lock()
	cb, exists := rwl.pendingRequests[key]
	if exists {
		delete(rwl.pendingRequests, key)
	}
	rwl.mu.Unlock()

	if exists {
		cb.RequestCallback.OnResponse(response, rwl.AddressAndPort)
	}
}

// HandleResponseFromNode processes a received response for the given key from a specific node.
func (rwl *RequestWaitingList[Key, Response]) HandleResponseFromNode(key Key, response Response, fromNode *common.InetAddressAndPort) {
	rwl.logger.Printf("RequestWaitingList received response for key %v at %d from node %v", key, rwl.clock.NanoTime(), fromNode)

	rwl.mu.Lock()
	cb, exists := rwl.pendingRequests[key]
	if exists {
		delete(rwl.pendingRequests, key)
	}
	rwl.mu.Unlock()

	if exists {
		rwl.logger.Printf("Invoking OnResponse for key %v", key)
		cb.RequestCallback.OnResponse(response, fromNode)
	}
}

// HandleError processes an error for the given request ID.
func (rwl *RequestWaitingList[Key, Response]) HandleError(key Key, err error) {
	rwl.logger.Printf("RequestWaitingList handling error for key %v: %v", key, err)

	rwl.mu.Lock()
	cb, exists := rwl.pendingRequests[key]
	if exists {
		delete(rwl.pendingRequests, key)
	}
	rwl.mu.Unlock()

	if exists {
		cb.RequestCallback.OnError(err)
	}
}

// Close stops the expiration worker and cleans up resources.
func (rwl *RequestWaitingList[Key, Response]) Close() { close(rwl.stopChan) }
