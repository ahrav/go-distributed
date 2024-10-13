package common

import (
	"encoding/json"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// ToJSON serializes an object to a JSON string.
func ToJSON(obj any) (string, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("ToJSON failed: %w", err)
	}
	return string(data), nil
}

// FromJSON deserializes JSON bytes into the provided object.
func FromJSON(data []byte, obj any) error {
	if err := json.Unmarshal(data, obj); err != nil {
		return fmt.Errorf("FromJSON failed: %w", err)
	}
	return nil
}

// Serialize serializes an object to CBOR bytes.
func Serialize(obj any) ([]byte, error) { return cbor.Marshal(obj) }

// Deserialize deserializes CBOR bytes into the provided object.
func Deserialize(data []byte, obj any) error { return cbor.Unmarshal(data, obj) }
