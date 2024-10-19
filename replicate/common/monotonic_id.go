package common

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MonotonicID represents a monotonically increasing identifier with a request ID and server ID.
type MonotonicID struct {
	RequestID int32
	ServerID  int32
}

// NewMonotonicId creates a new MonotonicID with the given requestID and serverID.
func NewMonotonicId(requestID, serverID int32) MonotonicID {
	return MonotonicID{RequestID: requestID, ServerID: serverID}
}

// EmptyMonotonicId returns an empty MonotonicID with requestID and serverID set to -1.
func EmptyMonotonicId() MonotonicID {
	return MonotonicID{RequestID: -1, ServerID: -1}
}

// IsAfter determines if the current MonotonicID is after the other MonotonicID.
func (m MonotonicID) IsAfter(other MonotonicID) bool {
	if m.RequestID == other.RequestID {
		return m.ServerID > other.ServerID
	}
	return m.RequestID > other.RequestID
}

// Equals checks if two MonotonicIds are equal.
func (m MonotonicID) Equals(other MonotonicID) bool {
	return m.RequestID == other.RequestID && m.ServerID == other.ServerID
}

// String implements the fmt.Stringer interface for MonotonicID.
func (m MonotonicID) String() string {
	return fmt.Sprintf("MonotonicUUID{id=%d, serverId=%d}", m.RequestID, m.ServerID)
}

// Serialize writes the MonotonicID to the provided io.Writer in big-endian format.
func (m MonotonicID) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.RequestID); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, m.ServerID)
}

// DeserializeMonotonicId reads a MonotonicID from the provided io.Reader in big-endian format.
func DeserializeMonotonicId(r io.Reader) (MonotonicID, error) {
	var (
		requestID int32
		serverID  int32
	)

	if err := binary.Read(r, binary.BigEndian, &requestID); err != nil {
		return MonotonicID{}, err
	}
	if err := binary.Read(r, binary.BigEndian, &serverID); err != nil {
		return MonotonicID{}, err
	}

	return MonotonicID{RequestID: requestID, ServerID: serverID}, nil
}

// Compare compares the current MonotonicID with another.
// Returns -1 if m < other, 0 if m == other, and 1 if m > other.
func (m MonotonicID) Compare(other MonotonicID) int {
	if m.RequestID < other.RequestID {
		return -1
	}
	if m.RequestID > other.RequestID {
		return 1
	}
	if m.ServerID < other.ServerID {
		return -1
	}
	if m.ServerID > other.ServerID {
		return 1
	}
	return 0
}

// IsEmpty checks if the MonotonicID is empty (both RequestID and ServerID are -1).
func (m MonotonicID) IsEmpty() bool { return m.Equals(EmptyMonotonicId()) }

// IsBefore determines if the current MonotonicID is before the other MonotonicID.
func (m MonotonicID) IsBefore(other MonotonicID) bool { return m.Compare(other) < 0 }

// NextID generates the next MonotonicID by incrementing the RequestID and setting the ServerID.
func (m MonotonicID) NextID(serverID int32) MonotonicID {
	return MonotonicID{
		RequestID: m.RequestID + 1,
		ServerID:  serverID,
	}
}
