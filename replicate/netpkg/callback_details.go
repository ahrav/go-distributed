package netpkg

import "time"

// CallbackDetails holds the callback and the timestamp when the request was added.
type CallbackDetails[Response any] struct {
	RequestCallback RequestCallback[Response]
	CreateTimeNanos int64 // NanoTime when the request was added
}

// NewCallbackDetails creates a new CallbackDetails instance.
func NewCallbackDetails[Response any](callback RequestCallback[Response], createTimeNanos int64) *CallbackDetails[Response] {
	return &CallbackDetails[Response]{RequestCallback: callback, CreateTimeNanos: createTimeNanos}
}

// ElapsedTimeNanos returns the elapsed time in nanoseconds since the callback was created.
func (cb *CallbackDetails[Response]) ElapsedTimeNanos(nowNanos int64) int64 {
	return nowNanos - cb.CreateTimeNanos
}

// IsExpired checks if the callback has expired based on the given timeout duration.
func (cb *CallbackDetails[Response]) IsExpired(timeout time.Duration, nowNanos int64) bool {
	return cb.ElapsedTimeNanos(nowNanos) >= timeout.Nanoseconds()
}
