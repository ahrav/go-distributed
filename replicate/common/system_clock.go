package common

import (
	"sync"
	"time"
)

// SystemClock provides methods to retrieve the current time with an optional clock skew.
type SystemClock struct {
	mu        sync.RWMutex
	clockSkew time.Duration
}

// NewSystemClock initializes and returns a new SystemClock instance.
func NewSystemClock() *SystemClock { return &SystemClock{clockSkew: 0} }

// NanoTime returns the current time in nanoseconds since the Unix epoch.
func (sc *SystemClock) NanoTime() int64 {
	return time.Now().UnixNano()
}

// Now returns the current time in milliseconds since the Unix epoch, adjusted by the clock skew.
func (sc *SystemClock) Now() int64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return time.Now().UnixMilli() + sc.clockSkew.Milliseconds()
}

// AddClockSkew sets the clock skew to the specified duration.
func (sc *SystemClock) AddClockSkew(skew time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.clockSkew = skew
}
