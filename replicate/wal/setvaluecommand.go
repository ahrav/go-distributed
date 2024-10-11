package wal

import (
	"encoding/binary"
	"io"
)

// SetValueCommand represents a command to set a value with an optional lease.
type SetValueCommand struct {
	BaseCommand
	key         string // The key to set.
	value       string // The value to set.
	attachLease string // The optional lease to attach.
}

// NewSetValueCommand creates a new SetValueCommand with the specified key and value.
// The attachLease is set to an empty string.
func NewSetValueCommand(key, value string) *SetValueCommand {
	return &SetValueCommand{key: key, value: value, attachLease: ""}
}

// NewSetValueCommandWithLease creates a new SetValueCommand with the specified key, value, and attachLease.
func NewSetValueCommandWithLease(key, value, attachLease string) *SetValueCommand {
	return &SetValueCommand{key: key, value: value, attachLease: attachLease}
}

// Serialize writes the SetValueCommand to the provided writer.
// It serializes the command type, key, value, and attachLease.
// Returns an error if any serialization step fails.
func (svc *SetValueCommand) Serialize(writer io.Writer) error {
	if err := binary.Write(writer, binary.BigEndian, int32(SetValueType)); err != nil {
		return err
	}
	if err := writeString(writer, svc.key); err != nil {
		return err
	}
	if err := writeString(writer, svc.value); err != nil {
		return err
	}
	if err := writeString(writer, svc.attachLease); err != nil {
		return err
	}
	return nil
}

// DeserializeSetValueCommand reads a SetValueCommand from the provided reader.
// It deserializes the key, value, and attachLease.
// Returns the deserialized SetValueCommand and an error if any deserialization step fails.
func DeserializeSetValueCommand(reader io.Reader) (*SetValueCommand, error) {
	key, err := readString(reader)
	if err != nil {
		return nil, err
	}
	value, err := readString(reader)
	if err != nil {
		return nil, err
	}
	attachLease, err := readString(reader)
	if err != nil {
		return nil, err
	}
	return &SetValueCommand{key: key, value: value, attachLease: attachLease}, nil
}

// GetKey returns the key of the SetValueCommand.
func (svc *SetValueCommand) GetKey() string {
	return svc.key
}

// GetValue returns the value of the SetValueCommand.
func (svc *SetValueCommand) GetValue() string {
	return svc.value
}

// HasLease checks if the SetValueCommand has an attached lease.
// Returns true if attachLease is not an empty string.
func (svc *SetValueCommand) HasLease() bool {
	return svc.attachLease != ""
}

// GetAttachedLease returns the attached lease of the SetValueCommand.
func (svc *SetValueCommand) GetAttachedLease() string {
	return svc.attachLease
}

// IsEmpty checks if the SetValueCommand is empty.
// Returns true if both key and value are empty strings.
func (svc *SetValueCommand) IsEmpty() bool {
	return svc.key == "" && svc.value == ""
}

// writeString writes a string to the provided writer.
// It first writes the length of the string, followed by the string data.
// Returns an error if any write step fails.
func writeString(writer io.Writer, s string) error {
	if err := binary.Write(writer, binary.BigEndian, int32(len(s))); err != nil {
		return err
	}
	_, err := writer.Write([]byte(s))
	return err
}

// readString reads a string from the provided reader.
// It first reads the length of the string, followed by the string data.
// Returns the read string and an error if any read step fails.
func readString(reader io.Reader) (string, error) {
	var length int32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", err
	}
	return string(data), nil
}
