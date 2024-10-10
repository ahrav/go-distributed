package wal

import (
	"os"
	"sort"
	"sync"

	"github.com/ahrav/go-distributed/replicate/common"
)

// WriteAheadLog represents the Write-Ahead Log (WAL) structure.
type WriteAheadLog struct {
	mu                  sync.Mutex // Mutex to ensure thread-safe access to the WAL.
	logCleaner          *TimeBasedLogCleaner
	openSegment         *Segment      // The currently open segment for writing.
	sortedSavedSegments []*Segment    // A sorted list of saved segments.
	config              common.Config // The configuration settings for the WAL.
}

// OpenWAL opens the Write-Ahead Log (WAL) with the given configuration.
// It returns a pointer to the WriteAheadLog.
func OpenWAL(config common.Config) *WriteAheadLog {
	segments := openAllSegments(config.GetWalDir())
	return NewWriteAheadLog(segments, config)
}

// openAllSegments opens all segments in the specified WAL directory.
// It returns a slice of pointers to the opened segments.
func openAllSegments(walDir string) []*Segment {
	files, err := os.ReadDir(walDir)
	if err != nil {
		panic(err)
	}

	var segments []*Segment
	for _, file := range files {
		name := file.Name()
		baseOffset := getBaseOffsetFromFileName(name)
		segment, err := Open(baseOffset, file.Name())
		if err != nil {
			panic(err)
		}
		segments = append(segments, segment)
	}

	if len(segments) == 0 {
		segment, err := Open(0, walDir)
		if err != nil {
			panic(err)
		}
		segments = append(segments, segment)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetBaseOffset() < segments[j].GetBaseOffset()
	})

	return segments
}

// NewWriteAheadLog creates a new WriteAheadLog with the given segments and configuration.
// It returns a pointer to the WriteAheadLog.
func NewWriteAheadLog(segments []*Segment, config common.Config) *WriteAheadLog {
	lastSegment := segments[len(segments)-1]
	segments = segments[:len(segments)-1]

	wal := &WriteAheadLog{
		openSegment:         lastSegment,
		sortedSavedSegments: segments,
		config:              config,
	}

	wal.logCleaner = NewTimeBasedLogCleaner(&config, wal)
	wal.logCleaner.Startup()

	return wal
}

// WriteEntryData writes the provided data as an entry to the WAL.
// It returns the offset of the written entry.
func (wal *WriteAheadLog) WriteEntryData(data []byte, generation int64) int64 {
	entryID := wal.GetLastLogIndex() + 1
	entry := &Entry{EntryIndex: entryID, Data: data, EntryType: DATA, Generation: generation}
	return wal.WriteEntry(entry)
}

// WriteEntry writes an entry to the WAL.
// It returns the offset of the written entry.
func (wal *WriteAheadLog) WriteEntry(entry *Entry) int64 {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wal.maybeRoll()
	entryOffset, _ := wal.openSegment.WriteEntry(entry)
	return entryOffset
}

// maybeRoll checks if the current segment needs to be rolled over based on its size.
// If the segment size exceeds the maximum log size, it flushes the current segment and opens a new one.
func (wal *WriteAheadLog) maybeRoll() {
	segmentSize, _ := wal.openSegment.Size()
	if segmentSize >= wal.config.GetMaxLogSize() {
		wal.openSegment.Flush()
		wal.sortedSavedSegments = append(wal.sortedSavedSegments, wal.openSegment)
		lastID, _ := wal.openSegment.GetLastLogEntryIndex()
		segment, err := Open(lastID, wal.config.GetWalDir())
		if err != nil {
			panic(err)
		}
		wal.openSegment = segment
	}
}

// ReadAll reads all entries from the WAL.
// It returns a slice of pointers to the entries.
func (wal *WriteAheadLog) ReadAll() []*Entry {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	var walEntries []*Entry
	for _, segment := range wal.sortedSavedSegments {
		sortedEntries, _ := segment.ReadAll()
		walEntries = append(walEntries, sortedEntries...)
	}
	openEntries, _ := wal.openSegment.ReadAll()
	walEntries = append(walEntries, openEntries...)
	return walEntries
}

// Flush flushes the current segment to ensure all data is written to disk.
func (wal *WriteAheadLog) Flush() {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wal.openSegment.Flush()
}

// Close closes the current segment.
func (wal *WriteAheadLog) Close() {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wal.openSegment.Close()
}

// Truncate truncates the WAL at the given log index.
func (wal *WriteAheadLog) Truncate(logIndex int64) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wal.openSegment.Truncate(logIndex)
}

// ReadAt reads an entry at the given index from the WAL.
// It returns a pointer to the entry.
func (wal *WriteAheadLog) ReadAt(index int64) *Entry {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	entry, _ := wal.openSegment.ReadAt(index)
	return entry
}

// ReadFrom reads entries from the WAL starting from the given index.
// It returns a slice of pointers to the entries.
func (wal *WriteAheadLog) ReadFrom(startIndex int64) []*Entry {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	segments := wal.getAllSegmentsContainingLogGreaterThan(startIndex)
	return wal.readWalEntriesFrom(startIndex, segments)
}

// getAllSegmentsContainingLogGreaterThan returns all segments containing logs greater than the specified index.
// It returns a slice of pointers to the segments.
func (wal *WriteAheadLog) getAllSegmentsContainingLogGreaterThan(startIndex int64) []*Segment {
	var segments []*Segment

	for i := len(wal.sortedSavedSegments) - 1; i >= 0; i-- {
		segment := wal.sortedSavedSegments[i]
		segments = append(segments, segment)
		if segment.GetBaseOffset() <= startIndex {
			break
		}
	}

	if wal.openSegment.GetBaseOffset() <= startIndex {
		segments = append(segments, wal.openSegment)
	}

	return segments
}

// readWalEntriesFrom reads entries from the given segments starting from the specified index.
// It returns a slice of pointers to the entries.
func (wal *WriteAheadLog) readWalEntriesFrom(startIndex int64, segments []*Segment) []*Entry {
	var allEntries []*Entry
	for _, segment := range segments {
		entries, _ := segment.ReadFrom(startIndex)
		allEntries = append(allEntries, entries...)
	}
	return allEntries
}

// RemoveAndDeleteSegment removes and deletes the specified segment from the WAL.
func (wal *WriteAheadLog) RemoveAndDeleteSegment(segment *Segment) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	index := wal.indexOf(segment)
	wal.sortedSavedSegments = append(wal.sortedSavedSegments[:index], wal.sortedSavedSegments[index+1:]...)
	segment.Delete()
}

// indexOf returns the index of the specified segment in the sortedSavedSegments slice.
// It panics if the segment is not found.
func (wal *WriteAheadLog) indexOf(segment *Segment) int {
	for i, s := range wal.sortedSavedSegments {
		if s.GetBaseOffset() == segment.GetBaseOffset() {
			return i
		}
	}
	panic("No log segment found")
}

// GetLastLogIndex returns the index of the last log entry in the WAL.
func (wal *WriteAheadLog) GetLastLogIndex() int64 {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	idx, _ := wal.openSegment.GetLastLogEntryIndex()
	return idx
}

// GetLastLogEntry returns the last log entry in the WAL.
// It returns a pointer to the entry.
func (wal *WriteAheadLog) GetLastLogEntry() *Entry {
	return wal.ReadAt(wal.GetLastLogIndex())
}

// IsEmpty checks if the WAL is empty.
// It returns true if the WAL is empty, otherwise false.
func (wal *WriteAheadLog) IsEmpty() bool {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	size, _ := wal.openSegment.Size()
	return size == 0 && len(wal.sortedSavedSegments) == 0
}

// GetLastLogEntryGeneration returns the generation of the last log entry in the WAL.
func (wal *WriteAheadLog) GetLastLogEntryGeneration() int64 {
	if wal.IsEmpty() {
		return 0
	}
	return wal.GetLastLogEntry().Generation
}

// Exists checks if the specified entry exists in the WAL.
// It returns true if the entry exists, otherwise false.
func (wal *WriteAheadLog) Exists(entry *Entry) bool {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.GetLastLogIndex() >= entry.EntryIndex
}

// GetLogStartIndex returns the start index of the log in the WAL.
func (wal *WriteAheadLog) GetLogStartIndex() int64 {
	if wal.IsEmpty() {
		return 0
	}
	return wal.ReadAt(1).EntryIndex
}
