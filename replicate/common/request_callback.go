package common

// RequestCallback defines the interface for handling responses and errors.
// It uses generics to allow flexibility in the type of the response.
type RequestCallback[T any] interface {
	OnResponse(response T, fromNode *InetAddressAndPort)
	OnError(err error)
}
