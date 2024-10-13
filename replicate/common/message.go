package common

import "fmt"

// Message represents a generic message with a payload and metadata.
type Message[T any] struct {
	Payload      T
	ClientSocket ClientConnection
	Header       Header
}

// NewMessage creates a new Message instance without a ClientConnection.
func NewMessage[T any](payload T, header Header) *Message[T] {
	return NewMessageWithClient(payload, header, nil)
}

// NewMessageWithClient creates a new Message instance with a ClientConnection.
func NewMessageWithClient[T any](payload T, header Header, clientSocket ClientConnection) *Message[T] {
	return &Message[T]{
		Payload:      payload,
		Header:       header,
		ClientSocket: clientSocket,
	}
}

// MessagePayload returns the payload of the message.
func (m *Message[T]) MessagePayload() T {
	return m.Payload
}

// GetClientConnection returns the ClientConnection associated with the message.
func (m *Message[T]) GetClientConnection() ClientConnection {
	return m.ClientSocket
}

// GetMessageId returns the MessageId from the header.
func (m *Message[T]) GetMessageId() MessageId {
	return m.Header.MessageId
}

// GetCorrelationId returns the CorrelationId from the header.
func (m *Message[T]) GetCorrelationId() int {
	return m.Header.CorrelationId
}

// GetFromAddress returns the FromAddress from the header.
func (m *Message[T]) GetFromAddress() *InetAddressAndPort {
	return m.Header.FromAddress
}

// Header contains metadata for the message.
type Header struct {
	FromAddress   *InetAddressAndPort `json:"fromAddress,omitempty"`
	CorrelationId int                 `json:"correlationId"`
	MessageId     MessageId           `json:"messageId"`
}

// MessageId represents a unique identifier for a message.
type MessageId int

// String implements the fmt.Stringer interface for Header.
func (h Header) String() string {
	return fmt.Sprintf("Header{FromAddress: %v, CorrelationId: %d, MessageId: %d}", h.FromAddress, h.CorrelationId, h.MessageId)
}

// String implements the fmt.Stringer interface for Message.
func (m Message[T]) String() string {
	return fmt.Sprintf("Message{Payload: %v, ClientSocket: %v, Header: %v}", m.Payload, m.ClientSocket, m.Header)
}
