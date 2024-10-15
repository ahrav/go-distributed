package common

import "fmt"

// MessageID represents a unique identifier for a message.
type MessageID int

const (
	VoteRequest MessageID = iota // 0
	VoteResponse
	HeartBeatRequest
	HeartBeatResponse
	ReplicationRequest
	ReplicationResponse
	StartElection
	HeartbeatTick
	ElectionTick
	HandleVoteResponse
	ProposeRequest
	SetValueRequest
	ConnectRequest
	GetValueRequest
	RedirectToLeader
	HighWaterMarkTransmitted
	WatchRequest
	SetWatchRequest
	LookingForLeader
	RegisterLeaseRequest
	RegisterClientRequest
	ClientHeartbeat
	PushPullGossipState
	GossipVersions
	RefreshLeaseRequest
	GetPartitionTable
	PartitionPutKV
	PartitionGetKV
	PartitionGetRangeKV
	SetValueResponse
	SetValueRequiringQuorum
	BatchRequest
	BatchResponse
	GetValueResponse
	Prepare
	Promise
	Propose
	Commit
	ProposeResponse
	CommitResponse
	FullPaxosLogPrepare
	FullLogPrepareResponse
	GetVersion
	GetVersionResponse
	NextNumberRequest
	NextNumberResponse
	VersionedSetValueRequest
	VersionedGetValueRequest
	ExcuteCommandRequest
	ExcuteCommandResponse
	SetValue
	PrepareOK
	PrepareNAK
	StartViewChange
	DoViewChange
	StartView // 55
)

var (
	messageIdNames = []string{
		"VoteRequest",              // 0
		"VoteResponse",             // 1
		"HeartBeatRequest",         // 2
		"HeartBeatResponse",        // 3
		"ReplicationRequest",       // 4
		"ReplicationResponse",      // 5
		"StartElection",            // 6
		"HeartbeatTick",            // 7
		"ElectionTick",             // 8
		"HandleVoteResponse",       // 9
		"ProposeRequest",           // 10
		"SetValueRequest",          // 11
		"ConnectRequest",           // 12
		"GetValueRequest",          // 13
		"RedirectToLeader",         // 14
		"HighWaterMarkTransmitted", // 15
		"WatchRequest",             // 16
		"SetWatchRequest",          // 17
		"LookingForLeader",         // 18
		"RegisterLeaseRequest",     // 19
		"RegisterClientRequest",    // 20
		"ClientHeartbeat",          // 21
		"PushPullGossipState",      // 22
		"GossipVersions",           // 23
		"RefreshLeaseRequest",      // 24
		"GetPartitionTable",        // 25
		"PartitionPutKV",           // 26
		"PartitionGetKV",           // 27
		"PartitionGetRangeKV",      // 28
		"SetValueResponse",         // 29
		"SetValueRequiringQuorum",  // 30
		"BatchRequest",             // 31
		"BatchResponse",            // 32
		"GetValueResponse",         // 33
		"Prepare",                  // 34
		"Promise",                  // 35
		"Propose",                  // 36
		"Commit",                   // 37
		"ProposeResponse",          // 38
		"CommitResponse",           // 39
		"FullPaxosLogPrepare",      // 40
		"FullLogPrepareResponse",   // 41
		"GetVersion",               // 42
		"GetVersionResponse",       // 43
		"NextNumberRequest",        // 44
		"NextNumberResponse",       // 45
		"VersionedSetValueRequest", // 46
		"VersionedGetValueRequest", // 47
		"ExcuteCommandRequest",     // 48
		"ExcuteCommandResponse",    // 49
		"SetValue",                 // 50
		"PrepareOK",                // 51
		"PrepareNAK",               // 52
		"StartViewChange",          // 53
		"DoViewChange",             // 54
		"StartView",                // 55
	}

	messageIdMap = make(map[int]MessageID)
)

func init() {
	for i := range messageIdNames {
		messageIdMap[i] = MessageID(i)
	}
}

// ValueOf returns the MessageID corresponding to the given id.
// Returns an error if the id does not correspond to any MessageID.
func ValueOf(id int) (MessageID, error) {
	msgId, exists := messageIdMap[id]
	if !exists {
		return -1, fmt.Errorf("invalid MessageID: %d", id)
	}
	return msgId, nil
}

// String returns the name of the MessageID.
func (m MessageID) String() string {
	if int(m) < 0 || int(m) >= len(messageIdNames) {
		return fmt.Sprintf("Unknown MessageID (%d)", m)
	}
	return messageIdNames[m]
}

// GetId returns the integer ID of the MessageID.
func (m MessageID) GetId() int { return int(m) }

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

// GetMessageId returns the MessageID from the header.
func (m *Message[T]) GetMessageId() MessageID {
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
	MessageId     MessageID           `json:"messageId"`
}

// String implements the fmt.Stringer interface for Header.
func (h Header) String() string {
	return fmt.Sprintf("Header{FromAddress: %v, CorrelationId: %d, MessageID: %d}", h.FromAddress, h.CorrelationId, h.MessageId)
}

// String implements the fmt.Stringer interface for Message.
func (m Message[T]) String() string {
	return fmt.Sprintf("Message{Payload: %v, ClientSocket: %v, Header: %v}", m.Payload, m.ClientSocket, m.Header)
}

// MessagePayloader is an interface for message payloads.
type MessagePayloader interface {
	GetMessageId() MessageID
}

// MessagePayload serves as the base struct for all message payloads.
type MessagePayload struct{ MessageId MessageID }

// NewMessagePayload creates a new MessagePayload instance.
func NewMessagePayload(messageId MessageID) MessagePayload {
	return MessagePayload{MessageId: messageId}
}

// GetMessageId returns the MessageID of the payload.
func (mp *MessagePayload) GetMessageId() MessageID { return mp.MessageId }
