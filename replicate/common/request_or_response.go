package common

import (
	"bytes"
	"fmt"
)

// RequestOrResponse represents a request or response message.
type RequestOrResponse struct {
	RequestID       *int                `json:"requestId,omitempty"`
	MessageBodyJSON []byte              `json:"messageBodyJson"`
	CorrelationID   *int                `json:"correlationId,omitempty"`
	Generation      int                 `json:"generation"`
	FromAddress     *InetAddressAndPort `json:"fromAddress,omitempty"`
	IsErr           bool                `json:"isError"`
}

// NewRequestOrResponse creates a new RequestOrResponse with all fields specified.
func NewRequestOrResponse(
	generation int,
	requestID *int,
	messageBodyJSON []byte,
	correlationID *int,
	fromAddress *InetAddressAndPort,
) *RequestOrResponse {
	return &RequestOrResponse{
		Generation:      generation,
		RequestID:       requestID,
		MessageBodyJSON: messageBodyJSON,
		CorrelationID:   correlationID,
		FromAddress:     fromAddress,
		IsErr:           false,
	}
}

// NewRequestOrResponseForClient creates a RequestOrResponse used by clients.
func NewRequestOrResponseForClient(requestID *int, correlationID *int) *RequestOrResponse {
	emptyJSON := []byte("")
	return NewRequestOrResponse(-1, requestID, emptyJSON, correlationID, nil)
}

// NewRequestOrResponseWithBody creates a RequestOrResponse with a message body, used by clients and notifications.
func NewRequestOrResponseWithBody(requestID *int, messageBodyJSON []byte) *RequestOrResponse {
	return NewRequestOrResponse(-1, requestID, messageBodyJSON, nil, nil)
}

// NewRequestOrResponseWithCorrelation creates a RequestOrResponse with a message body and correlation ID,
// used by clients and gossip.
func NewRequestOrResponseWithCorrelation(requestID *int, messageBodyJSON []byte, correlationID *int) *RequestOrResponse {
	return NewRequestOrResponse(-1, requestID, messageBodyJSON, correlationID, nil)
}

// NewRequestOrResponseByReplicatedLog creates a RequestOrResponse used by replicated logs.
func NewRequestOrResponseByReplicatedLog(
	generation int,
	requestID *int,
	messageBodyJSON []byte,
	correlationID *int,
) *RequestOrResponse {
	return NewRequestOrResponse(generation, requestID, messageBodyJSON, correlationID, nil)
}

// NewRequestOrResponseWithAddress creates a RequestOrResponse with a message body, correlation ID, and fromAddress.
func NewRequestOrResponseWithAddress(
	requestID *int,
	messageBodyJSON []byte,
	correlationID *int,
	fromAddress *InetAddressAndPort,
) *RequestOrResponse {
	return NewRequestOrResponse(-1, requestID, messageBodyJSON, correlationID, fromAddress)
}

// SetError marks the RequestOrResponse as an error.
func (ror *RequestOrResponse) SetError() *RequestOrResponse {
	ror.IsErr = true
	return ror
}

// GetRequestID returns the request ID.
func (ror *RequestOrResponse) GetRequestID() *int { return ror.RequestID }

// GetMessageBodyJSON returns the message body JSON.
func (ror *RequestOrResponse) GetMessageBodyJSON() []byte { return ror.MessageBodyJSON }

// GetCorrelationID returns the correlation ID.
func (ror *RequestOrResponse) GetCorrelationID() *int { return ror.CorrelationID }

// GetGeneration returns the generation.
func (ror *RequestOrResponse) GetGeneration() int { return ror.Generation }

// GetFromAddress returns the fromAddress.
func (ror *RequestOrResponse) GetFromAddress() *InetAddressAndPort {
	return ror.FromAddress
}

// IsErr returns whether this is an error.
func (ror *RequestOrResponse) IsError() bool { return ror.IsErr }

// Equals checks if two RequestOrResponse instances are equal.
func (ror *RequestOrResponse) Equals(other *RequestOrResponse) bool {
	if ror == other {
		return true
	}
	if other == nil {
		return false
	}

	// Compare RequestID
	if (ror.RequestID == nil) != (other.RequestID == nil) {
		return false
	}
	if ror.RequestID != nil && other.RequestID != nil && *ror.RequestID != *other.RequestID {
		return false
	}

	// Compare MessageBodyJSON
	if !bytes.Equal(ror.MessageBodyJSON, other.MessageBodyJSON) {
		return false
	}

	// Compare CorrelationID
	if (ror.CorrelationID == nil) != (other.CorrelationID == nil) {
		return false
	}
	if ror.CorrelationID != nil && other.CorrelationID != nil && *ror.CorrelationID != *other.CorrelationID {
		return false
	}

	// Compare Generation
	if ror.Generation != other.Generation {
		return false
	}

	// Compare FromAddress
	if !ror.FromAddress.Equals(other.FromAddress) {
		return false
	}

	// Compare IsError
	if ror.IsErr != other.IsErr {
		return false
	}

	return true
}

// String returns a string representation of RequestOrResponse.
func (ror *RequestOrResponse) String() string {
	return fmt.Sprintf("RequestOrResponse{requestId=%v, messageBodyJson=%v, correlationId=%v, generation=%d, fromAddress=%v, isError=%v}",
		ror.RequestID, ror.MessageBodyJSON, ror.CorrelationID, ror.Generation, ror.FromAddress, ror.IsError)
}
