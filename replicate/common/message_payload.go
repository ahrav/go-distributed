package common

// MessagePayloader is an interface for message payloads.
type MessagePayloader interface {
	GetMessageId() MessageId
}

// MessagePayload serves as the base struct for all message payloads.
type MessagePayload struct{ MessageId MessageId }

// NewMessagePayload creates a new MessagePayload instance.
func NewMessagePayload(messageId MessageId) MessagePayload {
	return MessagePayload{MessageId: messageId}
}

// GetMessageId returns the MessageId of the payload.
func (mp *MessagePayload) GetMessageId() MessageId { return mp.MessageId }
