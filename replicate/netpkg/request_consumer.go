package netpkg

import "github.com/ahrav/go-distributed/replicate/common"

// RequestConsumer handles incoming requests and connection closures.
type RequestConsumer interface {
	Accept(request common.Message[common.RequestOrResponse])
	Close(connection common.ClientConnection)
}
