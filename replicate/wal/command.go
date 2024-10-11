package wal

import "io"

// Command represents an interface for commands that can be serialized.
type Command interface {
	// Serialize writes the command to the provided writer.
	// Returns an error if the serialization fails.
	Serialize(writer io.Writer) error
}

const (
	NoClientId           = -1 // NoClientId is the default value for an unassigned client ID.
	SetValueType         = 1  // SetValueType represents a command type for setting a value.
	CompositeCommandType = 16 // CompositeCommandType represents a composite command type.
	CasCommandType       = 17 // CasCommandType represents a Compare-And-Swap command type.
)

// BaseCommand represents the base structure for a command.
type BaseCommand struct {
	clientId      int64 // The ID of the client issuing the command.
	requestNumber int   // The request number of the command.
}

// GetClientId returns the client ID of the command.
func (bc *BaseCommand) GetClientId() int64 { return bc.clientId }

// GetRequestNumber returns the request number of the command.
func (bc *BaseCommand) GetRequestNumber() int { return bc.requestNumber }

// WithClientId sets the client ID of the command and returns the updated BaseCommand.
func (bc *BaseCommand) WithClientId(clientId int64) *BaseCommand {
	bc.clientId = clientId
	return bc
}

// WithRequestNumber sets the request number of the command and returns the updated BaseCommand.
func (bc *BaseCommand) WithRequestNumber(requestNumber int) *BaseCommand {
	bc.requestNumber = requestNumber
	return bc
}

// HasClientId checks if the command has a valid client ID.
// Returns true if the client ID is not equal to NoClientId.
func (bc *BaseCommand) HasClientId() bool { return bc.clientId != NoClientId }
