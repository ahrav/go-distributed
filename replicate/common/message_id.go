package common

import (
	"fmt"
)

// MessageId represents a unique identifier for a message.
type MessageId int

const (
	VoteRequest MessageId = iota // 0
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

	messageIdMap = make(map[int]MessageId)
)

func init() {
	for i := range messageIdNames {
		messageIdMap[i] = MessageId(i)
	}
}

// ValueOf returns the MessageId corresponding to the given id.
// Returns an error if the id does not correspond to any MessageId.
func ValueOf(id int) (MessageId, error) {
	msgId, exists := messageIdMap[id]
	if !exists {
		return -1, fmt.Errorf("invalid MessageId: %d", id)
	}
	return msgId, nil
}

// String returns the name of the MessageId.
func (m MessageId) String() string {
	if int(m) < 0 || int(m) >= len(messageIdNames) {
		return fmt.Sprintf("Unknown MessageId (%d)", m)
	}
	return messageIdNames[m]
}

// GetId returns the integer ID of the MessageId.
func (m MessageId) GetId() int { return int(m) }
