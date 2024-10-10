package wal

import (
	"time"

	"github.com/ahrav/go-distributed/replicate/common"
)

// TimeBasedLogCleaner represents a log cleaner that deletes segments based on time duration.
type TimeBasedLogCleaner struct {
	config               *common.Config
	wal                  *WriteAheadLog
	singleThreadedTicker *time.Ticker
}

// NewTimeBasedLogCleaner returns an instance of TimeBasedLogCleaner.
func NewTimeBasedLogCleaner(config *common.Config, wal *WriteAheadLog) *TimeBasedLogCleaner {
	return &TimeBasedLogCleaner{
		config:               config,
		wal:                  wal,
		singleThreadedTicker: time.NewTicker(time.Duration(config.GetCleanTaskIntervalMs()) * time.Millisecond),
	}
}

// CleanLogs performs the log cleaning based on the segments to be deleted.
func (tlc *TimeBasedLogCleaner) CleanLogs() {
	segmentsToBeDeleted := tlc.GetSegmentsToBeDeleted()
	for _, walSegment := range segmentsToBeDeleted {
		tlc.wal.RemoveAndDeleteSegment(walSegment)
	}
}

// Startup starts the log cleaning scheduling.
func (tlc *TimeBasedLogCleaner) Startup() {
	tlc.scheduleLogCleaning()
}

// scheduleLogCleaning schedules the log cleaning task to run at intervals specified in the configuration.
func (tlc *TimeBasedLogCleaner) scheduleLogCleaning() {
	go func() {
		for range tlc.singleThreadedTicker.C {
			tlc.CleanLogs()
		}
	}()
}

// GetSegmentsToBeDeleted returns the segments that should be deleted based on log max duration.
func (tlc *TimeBasedLogCleaner) GetSegmentsToBeDeleted() []*Segment {
	return tlc.getSegmentsPast(tlc.config.GetLogMaxDurationMs())
}

// getSegmentsPast returns the segments that are past the given duration.
func (tlc *TimeBasedLogCleaner) getSegmentsPast(logMaxDurationMs int64) []*Segment {
	now := time.Now().UnixMilli()
	var markedForDeletion []*Segment
	sortedSavedSegments := tlc.wal.sortedSavedSegments
	for _, sortedSavedSegment := range sortedSavedSegments {
		ts, _ := sortedSavedSegment.GetLastLogEntryTimestamp()
		if tlc.timeElapsedSince(now, ts) > logMaxDurationMs {
			markedForDeletion = append(markedForDeletion, sortedSavedSegment)
		}
	}
	return markedForDeletion
}

// timeElapsedSince returns the time elapsed since the given timestamp.
func (tlc *TimeBasedLogCleaner) timeElapsedSince(now, lastLogEntryTimestamp int64) int64 {
	return now - lastLogEntryTimestamp
}
