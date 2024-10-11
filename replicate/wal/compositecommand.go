package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// CompositeCommand represents a command composed of multiple sub-commands.
type CompositeCommand struct {
	BaseCommand
	commands []Command // A slice of sub-commands.
}

// Serialize writes the CompositeCommand to the provided writer.
// It serializes the command type, the number of sub-commands, and each sub-command.
// Returns an error if any serialization step fails.
func (cc *CompositeCommand) Serialize(writer io.Writer) error {
	if err := binary.Write(writer, binary.BigEndian, int32(CompositeCommandType)); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, int32(len(cc.commands))); err != nil {
		return err
	}
	for _, command := range cc.commands {
		buf := new(bytes.Buffer)
		if err := command.Serialize(buf); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.BigEndian, int32(buf.Len())); err != nil {
			return err
		}
		if _, err := writer.Write(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// Add appends a sub-command to the CompositeCommand.
func (cc *CompositeCommand) Add(command Command) { cc.commands = append(cc.commands, command) }

// DeserializeCompositeCommand reads a CompositeCommand from the provided reader.
// It deserializes the number of sub-commands and each sub-command.
// Returns the deserialized CompositeCommand and an error if any deserialization step fails.
func DeserializeCompositeCommand(reader io.Reader) (*CompositeCommand, error) {
	var numCommands int32
	if err := binary.Read(reader, binary.BigEndian, &numCommands); err != nil {
		return nil, err
	}
	cc := &CompositeCommand{}
	for range numCommands {
		var size int32
		if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		command, err := Deserialize(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		cc.Add(command)
	}
	return cc, nil
}

// Deserialize reads a Command from the provided reader.
// It determines the command type and deserializes the appropriate command.
// Returns the deserialized Command and an error if any deserialization step fails.
func Deserialize(reader io.Reader) (Command, error) {
	var clientId int64
	if err := binary.Read(reader, binary.BigEndian, &clientId); err != nil {
		return nil, err
	}
	var requestNumber int32
	if err := binary.Read(reader, binary.BigEndian, &requestNumber); err != nil {
		return nil, err
	}
	var commandType int32
	if err := binary.Read(reader, binary.BigEndian, &commandType); err != nil {
		return nil, err
	}

	switch commandType {
	case SetValueType:
		cmd, err := DeserializeSetValueCommand(reader)
		if err != nil {
			return nil, err
		}
		cmd.WithClientId(clientId).WithRequestNumber(int(requestNumber))
		return cmd, nil
	case CompositeCommandType:
		cc, err := DeserializeCompositeCommand(reader)
		if err != nil {
			return nil, err
		}
		cc.WithClientId(clientId).WithRequestNumber(int(requestNumber))
		return cc, nil
	case CasCommandType:
		// TODO: Implement DeserializeCompareAndSwap function after covering two phase execution.
		// cmd, err := DeserializeCompareAndSwap(reader)
		// if err != nil {
		// 	return nil, err
		// }
		// cmd.WithClientId(clientId).WithRequestNumber(int(requestNumber))
		// return cmd, nil
	default:
		return nil, errors.New("unknown command type")
	}

	return nil, nil
}
