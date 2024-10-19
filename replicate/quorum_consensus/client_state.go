package quorumconsensus

import (
	"sync/atomic"

	"github.com/ahrav/go-distributed/replicate/common"
)

// ClientState maintains the state for a client, ensuring strictly monotonic timestamps.
type ClientState struct {
	clock               *common.SystemClock
	lastTimestampMicros int64 // accessed atomically
}

// NewClientState creates a new ClientState instance with the provided SystemClock.
func NewClientState(clock *common.SystemClock) *ClientState {
	return &ClientState{clock: clock, lastTimestampMicros: 0}
}

// GetTimestamp returns a strictly monotonically increasing timestamp in microseconds.
// It ensures that even if multiple calls occur within the same millisecond, each
// returned timestamp is unique and greater than the previous.
func (cs *ClientState) GetTimestamp() int64 {
	for {
		currentMillis := cs.CurrentTimeMillis()
		currentMicros := currentMillis * 1000
		last := atomic.LoadInt64(&cs.lastTimestampMicros)
		var newTimestamp int64

		if last >= currentMicros {
			newTimestamp = last + 1
		} else {
			newTimestamp = currentMicros
		}

		// Attempt to atomically set the new timestamp.
		if atomic.CompareAndSwapInt64(&cs.lastTimestampMicros, last, newTimestamp) {
			return newTimestamp
		}
		// If the CAS failed, another goroutine updated the timestamp. Retry.
	}
}

// CurrentTimeMillis returns the current time in milliseconds using the SystemClock.
func (cs *ClientState) CurrentTimeMillis() int64 { return cs.clock.Now() }
