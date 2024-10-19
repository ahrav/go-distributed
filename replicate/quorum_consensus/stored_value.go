package quorumconsensus

import "github.com/ahrav/go-distributed/replicate/common"

// StoredValue represents a key-value pair along with its version (MonotonicId).
type StoredValue struct {
	Key     string             `json:"key"`
	Value   string             `json:"value"`
	Version common.MonotonicID `json:"version"`
}

// EmptyStoredValue is a predefined empty StoredValue.
var EmptyStoredValue = StoredValue{Version: common.EmptyMonotonicId()}

// NewStoredValue creates a new StoredValue instance with the given key, value, and version.
func NewStoredValue(key, value string, version common.MonotonicID) StoredValue {
	return StoredValue{
		Key:     key,
		Value:   value,
		Version: version,
	}
}
