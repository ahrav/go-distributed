package wal

import (
	"encoding/binary"
	"io"
	"os"
)

// EntryDeserializer helps in reading and deserializing WALEntries from a file channel.
type EntryDeserializer struct {
	intBuffer  []byte
	longBuffer []byte
	logChannel *os.File
}

// NewWALEntryDeserializer creates a new deserializer here helps encapsulate the logic for reading WALEntries,
// making it reusable and simplifying the handling of file channels.
func NewWALEntryDeserializer(logChannel *os.File) *EntryDeserializer {
	return &EntryDeserializer{
		intBuffer:  make([]byte, sizeOfInt),
		longBuffer: make([]byte, sizeOfLong),
		logChannel: logChannel,
	}
}

func (d *EntryDeserializer) ReadEntry() (*Entry, error) {
	position, err := d.logChannel.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return d.ReadEntryAt(position)
}

// Header represents the metadata of a WALEntry, which includes the type, generation, ID, and timestamp.

type Header struct {
	headerStartOffset int64
	deserializer      *EntryDeserializer
}

// NewHeader creates a new header instance, which is used to read the metadata of a WALEntry.
func NewHeader(startOffset int64, deserializer *EntryDeserializer) *Header {
	return &Header{
		headerStartOffset: startOffset,
		deserializer:      deserializer,
	}
}

// ReadEntryType reads the type of the entry, which is used to distinguish between data, metadata, and CRC entries.
func (h *Header) ReadEntryType() (int32, error) {
	return h.deserializer.ReadInteger(h.headerStartOffset)
}

// ReadGeneration reads the generation of the entry, which is used to track the version of the data.
func (h *Header) ReadGeneration() (int64, error) {
	return h.deserializer.ReadLong(h.headerStartOffset + int64(sizeOfInt))
}

// ReadEntryId reads the unique identifier of the entry, which helps in tracking the order of entries.
func (h *Header) ReadEntryId() (int64, error) {
	return h.deserializer.ReadLong(h.headerStartOffset + int64(sizeOfInt+sizeOfLong))
}

// ReadEntryTimestamp reads the timestamp of the entry, which indicates when the entry was created.
func (h *Header) ReadEntryTimestamp() (int64, error) {
	return h.deserializer.ReadLong(h.headerStartOffset + int64(sizeOfInt+2*sizeOfLong))
}

// GetSize calculates the size of the header, which is used to
// determine the starting position of the actual data within a WALEntry.
// This helps in correctly locating and reading the data.
func (h *Header) GetSize() int {
	return sizeOfInt + 3*sizeOfLong
}

// ReadEntryAt reads an entry at the specified position in the file.
func (d *EntryDeserializer) ReadEntryAt(startPosition int64) (*Entry, error) {
	entrySize, err := d.ReadInteger(startPosition)
	if err != nil {
		return nil, err
	}

	header := NewHeader(startPosition+int64(sizeOfInt), d)
	entryType, err := header.ReadEntryType()
	if err != nil {
		return nil, err
	}
	generation, err := header.ReadGeneration()
	if err != nil {
		return nil, err
	}
	entryId, err := header.ReadEntryId()
	if err != nil {
		return nil, err
	}
	entryTimestamp, err := header.ReadEntryTimestamp()
	if err != nil {
		return nil, err
	}

	headerSize := header.GetSize()
	dataSize := int(entrySize) - headerSize
	buffer := make([]byte, dataSize)
	_, err = d.readFromChannel(buffer, startPosition+int64(headerSize+sizeOfInt))
	if err != nil {
		return nil, err
	}

	return &Entry{
		EntryIndex: entryId,
		Data:       buffer,
		EntryType:  EntryType(entryType),
		Generation: generation,
		Timestamp:  entryTimestamp,
	}, nil
}

// ReadLong reads a long integer value from the file channel at the specified position.
func (d *EntryDeserializer) ReadLong(position int64) (int64, error) {
	_, err := d.readFromChannel(d.longBuffer, position)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(d.longBuffer)), nil
}

// ReadInteger reads an integer value from the file channel at the specified position.
func (d *EntryDeserializer) ReadInteger(position int64) (int32, error) {
	_, err := d.readFromChannel(d.intBuffer, position)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(d.intBuffer)), nil
}

// readFromChannel reads the specified number of bytes from the log channel at the given file position.
// It returns the number of bytes read and any error that occurred during the read operation.
func (d *EntryDeserializer) readFromChannel(buffer []byte, filePosition int64) (int, error) {
	if _, err := d.logChannel.Seek(filePosition, io.SeekStart); err != nil {
		return 0, err
	}

	bytesRead, err := d.logChannel.Read(buffer)
	if err != nil && err != io.EOF {
		return 0, err
	}

	return bytesRead, nil
}
