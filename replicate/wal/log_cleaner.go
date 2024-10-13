package wal

// LogCleaner defines an interface for cleaning logs.
type LogCleaner interface {
	CleanLogs()
	Startup()
	GetSegmentsToBeDeleted() []*Segment
}
