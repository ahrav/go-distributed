package wal

import (
	"sync"
	"time"

	"github.com/ahrav/go-distributed/replicate/common"
)

// SegmentRetriever defines an interface for retrieving segments to be deleted.
type SegmentRetriever interface {
	GetSegmentsToBeDeleted() []*Segment
}

// LogCleaner represents the structure for cleaning logs.
type LogCleaner struct {
	config               *common.Config
	wal                  *WriteAheadLog
	cleanMutex           sync.Mutex
	singleThreadedTicker *time.Ticker
}

// NewLogCleaner returns an instance of LogCleaner.
func NewLogCleaner(config *common.Config, wal *WriteAheadLog) *LogCleaner {
	return &LogCleaner{
		config:               config,
		wal:                  wal,
		singleThreadedTicker: time.NewTicker(time.Duration(config.GetCleanTaskIntervalMs()) * time.Millisecond),
	}
}

// CleanLogs performs the log cleaning based on the segments to be deleted.
func (lc *LogCleaner) CleanLogs(retriever SegmentRetriever) {
	// Ensure only one log cleaning task is running at a time.
	lc.cleanMutex.Lock()
	defer lc.cleanMutex.Unlock()

	segmentsToBeDeleted := retriever.GetSegmentsToBeDeleted()
	for _, walSegment := range segmentsToBeDeleted {
		lc.wal.RemoveAndDeleteSegment(walSegment)
	}
	lc.scheduleLogCleaning(retriever)
}

// Startup starts the log cleaning scheduling.
func (lc *LogCleaner) Startup(retriever SegmentRetriever) {
	lc.scheduleLogCleaning(retriever)
}

// ScheduleLogCleaning schedules the log cleaning task to run at intervals specified in the configuration.
func (lc *LogCleaner) scheduleLogCleaning(retriever SegmentRetriever) {
	go func() {
		for range lc.singleThreadedTicker.C {
			lc.CleanLogs(retriever)
		}
	}()
}
