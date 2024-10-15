package quorum

import "fmt"

// StoredValue represents a key-value pair with associated metadata.
type StoredValue struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	Timestamp  int64  `json:"timestamp"`
	Generation int    `json:"generation"`
}

// MinInt64 defines the minimum value for an int64.
const MinInt64 = int64(-1) << 63

// Empty is the equivalent of Java's public static final StoredValue EMPTY.
// It represents an empty or default StoredValue.
var Empty = StoredValue{Timestamp: MinInt64}

// NewStoredValue creates a new StoredValue with the specified parameters.
// It acts as a constructor for StoredValue.
func NewStoredValue(key, value string, timestamp int64, generation int) StoredValue {
	return StoredValue{
		Key:        key,
		Value:      value,
		Timestamp:  timestamp,
		Generation: generation,
	}
}

// String implements the fmt.Stringer interface for StoredValue.
// It provides a string representation of the StoredValue instance.
func (sv StoredValue) String() string {
	return fmt.Sprintf(
		"StoredValue{key='%s', value='%s', timestamp=%d, generation=%d}",
		sv.Key,
		sv.Value,
		sv.Timestamp,
		sv.Generation,
	)
}
