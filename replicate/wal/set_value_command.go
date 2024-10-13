package wal

import (
	"bytes"
	"encoding/binary"
	"io"
)

// SetValueCommand represents a command to set a value with an optional lease.
type SetValueCommand struct {
	BaseCommand
	Key         string // The Key to set.
	Value       string // The Value to set.
	AttachLease string // The optional lease to attach.
}

// NewSetValueCommand creates a new SetValueCommand with the specified key and value.
// The attachLease is set to an empty string.
func NewSetValueCommand(key, value string) *SetValueCommand {
	return &SetValueCommand{Key: key, Value: value, AttachLease: ""}
}

// NewSetValueCommandWithLease creates a new SetValueCommand with the specified key, value, and attachLease.
func NewSetValueCommandWithLease(key, value, attachLease string) *SetValueCommand {
	return &SetValueCommand{Key: key, Value: value, AttachLease: attachLease}
}

// Serialize writes the SetValueCommand to the provided writer.
// It serializes the command type, key, value, and attachLease.
// Returns an error if any serialization step fails.
func (svc *SetValueCommand) Serialize(writer io.Writer) error {
	if err := binary.Write(writer, binary.BigEndian, int32(SetValueType)); err != nil {
		return err
	}
	if err := writeString(writer, svc.Key); err != nil {
		return err
	}
	if err := writeString(writer, svc.Value); err != nil {
		return err
	}
	if err := writeString(writer, svc.AttachLease); err != nil {
		return err
	}
	return nil
}

// SerializeToBytes serializes the SetValueCommand into a byte slice
func (svc *SetValueCommand) SerializeToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int32(SetValueType)); err != nil {
		return nil, err
	}
	if err := writeString(buf, svc.Key); err != nil {
		return nil, err
	}
	if err := writeString(buf, svc.Value); err != nil {
		return nil, err
	}
	if err := writeString(buf, svc.AttachLease); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
	return &SetValueCommand{Key: key, Value: value, AttachLease: attachLease}, nil
}

// GetKey returns the key of the SetValueCommand.
func (svc *SetValueCommand) GetKey() string {
	return svc.Key
}

// GetValue returns the value of the SetValueCommand.
func (svc *SetValueCommand) GetValue() string {
	return svc.Value
}

// HasLease checks if the SetValueCommand has an attached lease.
// Returns true if attachLease is not an empty string.
func (svc *SetValueCommand) HasLease() bool {
	return svc.AttachLease != ""
}

// GetAttachedLease returns the attached lease of the SetValueCommand.
func (svc *SetValueCommand) GetAttachedLease() string {
	return svc.AttachLease
}

// IsEmpty checks if the SetValueCommand is empty.
// Returns true if both key and value are empty strings.
func (svc *SetValueCommand) IsEmpty() bool {
	return svc.Key == "" && svc.Value == ""
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
