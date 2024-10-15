package quorum

import (
	"sync/atomic"

	"github.com/ahrav/go-distributed/replicate/common"
)

// ClientState maintains client-specific state, including the last timestamp used.
// It ensures that timestamps are strictly increasing for a given client.
type ClientState struct {
	clock               *common.SystemClock
	lastTimestampMicros int64
}

// NewClientState creates a new ClientState with the provided SystemClock.
func NewClientState(clock *common.SystemClock) *ClientState {
	return &ClientState{clock: clock}
}

// GetTimestamp returns a strictly increasing timestamp in microseconds.
// It ensures that each timestamp is greater than the previous one.
func (cs *ClientState) GetTimestamp() int64 {
	for {
		current := cs.CurrentTimeMillis() * 1000
		last := atomic.LoadInt64(&cs.lastTimestampMicros)
		var tstamp int64
		if last >= current {
			tstamp = last + 1
		} else {
			tstamp = current
		}
		if atomic.CompareAndSwapInt64(&cs.lastTimestampMicros, last, tstamp) {
			return tstamp
		}
	}
}

// CurrentTimeMillis returns the current time in milliseconds from the SystemClock.
func (cs *ClientState) CurrentTimeMillis() int64 { return cs.clock.Now() }
