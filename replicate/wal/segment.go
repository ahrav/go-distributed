package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Segment represents a segment in the Write-Ahead Log (WAL).
// It contains a file handle and an index of entry offsets.
type Segment struct {
	mu           sync.Mutex
	file         *os.File        // File handle for the segment file.
	entryOffsets map[int64]int64 // Map of entry indices to their offsets in the file.
}

// NewWALSegment creates a new WAL segment starting at the given index and file path.
// It returns a pointer to the Segment and an error if any occurs during file operations.
func NewWALSegment(startIndex int64, filepath string) (*Segment, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	segment := &Segment{
		file:         file,
		entryOffsets: make(map[int64]int64),
	}
	if err := segment.buildOffsetIndex(); err != nil {
		return nil, err
	}
	return segment, nil
}

// Open creates a WAL segment from the provided directory and start index, ensuring that the appropriate WAL segment file is returned.
func Open(startIndex int64, walDir string) (*Segment, error) {
	fileName := fmt.Sprintf("wal_%d.log", startIndex)
	filePath := filepath.Join(walDir, fileName)
	return NewWALSegment(startIndex, filePath)
}

// OpenFromFile initializes a WAL segment from an existing file, which can be used for reading or appending new entries.
func OpenFromFile(file *os.File) (*Segment, error) {
	baseOffset := getBaseOffsetFromFileName(filepath.Base(file.Name()))
	return NewWALSegment(baseOffset, file.Name())
}

// GetFileName returns the name of the segment file.
func (w *Segment) GetFileName() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Name()
}

// GetBaseOffset returns the base offset of the segment derived from its file name.
func (w *Segment) GetBaseOffset() int64 {
	_, filename := filepath.Split(w.file.Name())
	return getBaseOffsetFromFileName(filename)
}

// buildOffsetIndex builds an index of entry offsets by reading the segment file.
func (w *Segment) buildOffsetIndex() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.entryOffsets = make(map[int64]int64)
	var totalBytesRead int64 = 0

	for {
		entry, err := readWALEntryAt(w.file, totalBytesRead)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		w.entryOffsets[entry.EntryIndex] = totalBytesRead
		totalBytesRead += int64(entry.LogEntrySize())
	}

	return nil
}

// ReadFrom reads entries from the segment starting from the given index.
// It returns a slice of entries and an error if any occurs during reading.
func (w *Segment) ReadFrom(startIndex int64) ([]*Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entries []*Entry
	for index, offset := range w.entryOffsets {
		if index >= startIndex {
			entry, err := readWALEntryAt(w.file, offset)
			if err != nil {
				return nil, fmt.Errorf("error reading entry at offset %d: %w", offset, err)
			}
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// WriteEntry writes an entry to the segment and updates the entry offset index.
// It returns the index of the written entry and an error if any occurs during writing.
func (w *Segment) WriteEntry(entry *Entry) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to the end of the file to append the entry.
	offset, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return -1, err
	}

	data := entry.Serialize()
	if _, err := w.file.Write(data); err != nil {
		return -1, err
	}

	w.entryOffsets[entry.EntryIndex] = offset
	return entry.EntryIndex, nil
}

// Close closes the segment file.
func (w *Segment) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Truncate truncates the segment file at the given log index and updates the entry offset index.
// It returns an error if any occurs during truncation.
func (w *Segment) Truncate(logIndex int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	offset, exists := w.entryOffsets[logIndex]
	if !exists {
		return fmt.Errorf("no file position available for logIndex=%d", logIndex)
	}

	if err := w.file.Truncate(offset); err != nil {
		return err
	}
	w.truncateIndex(logIndex)
	return nil
}

// truncateIndex removes entries from the entry offset index starting from the given log index.
func (w *Segment) truncateIndex(logIndex int64) {
	for index := range w.entryOffsets {
		if index >= logIndex {
			delete(w.entryOffsets, index)
		}
	}
}

// ReadAt reads an entry at the given index from the segment.
// It returns the entry and an error if any occurs during reading.
func (w *Segment) ReadAt(index int64) (*Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	offset, exists := w.entryOffsets[index]
	if !exists {
		return nil, fmt.Errorf("no file position available for logIndex=%d", index)
	}

	return readWALEntryAt(w.file, offset)
}

// Flush flushes the segment file to ensure all data is written to disk.
// It returns an error if any occurs during flushing.
func (w *Segment) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

func readWALEntryAt(file *os.File, offset int64) (*Entry, error) {
	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var entry Entry
	if err := binary.Read(file, binary.BigEndian, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// getBaseOffsetFromFileName extracts the base offset from the segment file name.
// The file name is expected to be in the format "wal_<offset>.log".
// This can be used to determine the base offset of the segment.
func getBaseOffsetFromFileName(fileName string) int64 {
	var offset int64
	fmt.Sscanf(fileName, "wal_%d.log", &offset)
	return offset
}

// Delete removes the segment file from the filesystem.
// It first closes the file handle and then deletes the file.
// Returns an error if any occurs during the file close or delete operations.
func (w *Segment) Delete() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}
	return os.Remove(w.file.Name())
}

// Size returns the size of the segment file in bytes.
// It retrieves the file information and extracts the size.
// Returns the file size and an error if any occurs during the file stat operation.
func (w *Segment) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	fileInfo, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

// GetLastLogEntryTimestamp returns the timestamp of the last log entry in the segment.
// It reads the last entry based on the highest index in the entryOffsets map.
// Returns the timestamp of the last log entry and an error if any occurs during the read operation.
func (w *Segment) GetLastLogEntryTimestamp() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.entryOffsets) == 0 {
		return 0, nil
	}

	lastIndex := int64(0)
	for index := range w.entryOffsets {
		if index > lastIndex {
			lastIndex = index
		}
	}

	entry, err := w.ReadAt(lastIndex)
	if err != nil {
		return 0, err
	}
	return entry.Timestamp, nil
}

// WriteToChannel writes the provided data to the segment file and flushes it to disk.
// It ensures that the data is written and synchronized to the filesystem.
// Returns an error if any occurs during the write or sync operations.
func (w *Segment) WriteToChannel(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.Write(data)
	if err != nil {
		return err
	}
	return w.file.Sync() // Flush the data to disk.
}

// GetLastLogEntryIndex returns the index of the last log entry in the segment.
// It finds the highest index in the entryOffsets map.
// Returns the index of the last log entry and an error if any occurs during the read operation.
func (w *Segment) GetLastLogEntryIndex() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.entryOffsets) == 0 {
		return 0, nil
	}

	lastIndex := int64(0)
	for index := range w.entryOffsets {
		if index > lastIndex {
			lastIndex = index
		}
	}
	return lastIndex, nil
}

// ReadAll reads all entries from the segment file.
// It iterates through the file and reads each entry until the end of the file is reached.
// Returns a slice of entries and an error if any occurs during the read operations.
func (w *Segment) ReadAll() ([]*Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entries []*Entry
	for offset := int64(0); ; {
		entry, err := readWALEntryAt(w.file, offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
		offset += int64(entry.LogEntrySize())
	}
	return entries, nil
}
