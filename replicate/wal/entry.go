package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// EntryType represents the type of an entry in the Write-Ahead Log (WAL).
type EntryType int

const (
	// EntryTypeData indicates a data entry type.
	EntryTypeData EntryType = iota
)

const (
	// sizeOfInt represents the size of an integer in bytes.
	sizeOfInt = 4
	// sizeOfLong represents the size of a long integer in bytes.
	sizeOfLong = 8
)

// Entry represents a single entry in the Write-Ahead Log (WAL).
type Entry struct {
	EntryIndex int64     // The index of the entry.
	Data       []byte    // The data contained in the entry.
	EntryType  EntryType // The type of the entry.
	Timestamp  int64     // The timestamp when the entry was created.
	Generation int64     // The generation of the entry.
}

// NewWALEntry creates a new WAL entry with the given data and default values for other fields.
func NewWALEntry(data []byte) *Entry {
	return &Entry{
		EntryIndex: -1,
		Data:       data,
		EntryType:  EntryTypeData,
		Timestamp:  time.Now().UnixMilli(),
		Generation: 0,
	}
}

// NewWALEntryWithParams creates a new WAL entry with the specified parameters.
func NewWALEntryWithParams(entryIndex int64, data []byte, entryType EntryType, generation int64) *Entry {
	return &Entry{
		EntryIndex: entryIndex,
		Data:       data,
		EntryType:  entryType,
		Timestamp:  time.Now().UnixMilli(),
		Generation: generation,
	}
}

// Serialize serializes the entry into a byte slice.
func (e *Entry) Serialize() []byte {
	entrySize := e.SerializedSize()
	bufferSize := e.LogEntrySize()
	buffer := bytes.NewBuffer(make([]byte, 0, bufferSize))

	binary.Write(buffer, binary.BigEndian, int32(entrySize))
	binary.Write(buffer, binary.BigEndian, int32(e.EntryType))
	binary.Write(buffer, binary.BigEndian, e.Generation)
	binary.Write(buffer, binary.BigEndian, e.EntryIndex)
	binary.Write(buffer, binary.BigEndian, e.Timestamp)
	buffer.Write(e.Data)

	return buffer.Bytes()
}

// LogEntrySize returns the total size of the log entry including metadata.
func (e *Entry) LogEntrySize() int {
	return sizeOfInt + e.SerializedSize()
}

// SerializedSize returns the size of the serialized entry.
func (e *Entry) SerializedSize() int {
	return e.sizeOfData() + e.sizeOfIndex() + e.sizeOfGeneration() + e.sizeOfEntryType() + e.sizeOfTimestamp()
}

// sizeOfData returns the size of the data in the entry.
func (e *Entry) sizeOfData() int {
	return len(e.Data)
}

// sizeOfEntryType returns the size of the entry type field.
func (e *Entry) sizeOfEntryType() int {
	return sizeOfInt
}

// sizeOfTimestamp returns the size of the timestamp field.
func (e *Entry) sizeOfTimestamp() int {
	return sizeOfLong
}

// sizeOfGeneration returns the size of the generation field.
func (e *Entry) sizeOfGeneration() int {
	return sizeOfLong
}

// sizeOfIndex returns the size of the index field.
func (e *Entry) sizeOfIndex() int {
	return sizeOfLong
}

// MatchEntry checks if the current entry matches another entry based on generation, index, and data.
func (e *Entry) MatchEntry(entry *Entry) bool {
	return e.Generation == entry.Generation &&
		e.EntryIndex == entry.EntryIndex &&
		bytes.Equal(e.Data, entry.Data)
}

// String returns a string representation of the entry.
func (e *Entry) String() string {
	return fmt.Sprintf("Entry{entryId=%d, data=%v, entryType=%d, timeStamp=%d, generation=%d}",
		e.EntryIndex, e.Data, e.EntryType, e.Timestamp, e.Generation)
}
