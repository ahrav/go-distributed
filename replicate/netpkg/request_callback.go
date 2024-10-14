package netpkg

import "github.com/ahrav/go-distributed/replicate/common"

// RequestCallback defines the interface for handling responses and errors.
// It uses generics to allow flexibility in the type of the response.
type RequestCallback[Response any] interface {
	OnResponse(response Response, fromNode *common.InetAddressAndPort)
	OnError(err error)
}
