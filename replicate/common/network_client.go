package common

import (
	"fmt"
	"log"

	"github.com/ahrav/go-distributed/replicate/netpkg"
)

// NetworkClient facilitates sending messages and receiving responses over the network.
type NetworkClient[Req MessagePayloader, Res any] struct {
	logger *log.Logger
}

// NewNetworkClient creates a new NetworkClient instance with the provided logger.
func NewNetworkClient[Req MessagePayloader, Res any](logger *log.Logger) *NetworkClient[Req, Res] {
	return &NetworkClient[Req, Res]{logger: logger}
}

// SendAndReceive sends a request of type Req to the specified address and waits for a response of type Res.
// It returns a Response encapsulating either the result or an error message.
func (nc *NetworkClient[Req, Res]) SendAndReceive(request Req, address InetAddressAndPort) (Response[Res], error) {
	socketClient, err := netpkg.NewSocketClient[RequestOrResponse](&address, 5000) // 5000ms read timeout
	if err != nil {
		return Response[Res]{}, fmt.Errorf("failed to create SocketClient: %w", err)
	}
	defer func() {
		closeErr := socketClient.Close()
		if closeErr != nil {
			nc.logger.Printf("Failed to close SocketClient: %v", closeErr)
		}
	}()

	serializedRequest, err := Serialize(request)
	if err != nil {
		return Response[Res]{}, fmt.Errorf("failed to serialize request: %w", err)
	}

	requestId := request.GetMessageId().GetId()
	requestOrResponse := RequestOrResponse{
		RequestID:       &requestId,
		MessageBodyJSON: serializedRequest,
		CorrelationID:   nil, // Set if needed
		Generation:      1,
		IsErr:           false,
	}

	// Send the message and receive the response using BlockingSend.
	response, err := socketClient.BlockingSend(requestOrResponse)
	if err != nil {
		return Response[Res]{}, fmt.Errorf("failed to perform blocking send: %w", err)
	}

	if response.IsError() {
		// Deserialize the error message.
		var errMsg string
		err = Deserialize(response.MessageBodyJSON, &errMsg)
		if err != nil {
			return Response[Res]{}, fmt.Errorf("failed to deserialize error message: %w", err)
		}
		return NewErrorResponse[Res](errMsg), nil
	}

	// Deserialize the successful response into the expected type Res
	var result Res
	err = Deserialize(response.MessageBodyJSON, &result)
	if err != nil {
		return Response[Res]{}, fmt.Errorf("failed to deserialize result: %w", err)
	}

	return NewSuccessResponse[Res](result), nil
}
