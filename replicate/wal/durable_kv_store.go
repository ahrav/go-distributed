package wal

import (
	"bytes"
	"sync"

	"github.com/ahrav/go-distributed/replicate/common"
)

type DurableKVStore struct {
	kv  map[string]string
	wal *WriteAheadLog
	mu  sync.RWMutex
}

// NewDurableKVStore initializes a new instance of DurableKVStore
func NewDurableKVStore(config *common.Config) *DurableKVStore {
	wal := OpenWAL(config)
	store := &DurableKVStore{
		kv:  make(map[string]string),
		wal: wal,
	}
	store.applyLog() // Apply log at startup to restore state
	return store
}

// Get retrieves the value for a given key
func (s *DurableKVStore) Get(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.kv[key]
}

// Put stores the key-value pair in the key-value store and appends it to the WAL
func (s *DurableKVStore) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.appendLog(key, value)
	s.kv[key] = value
}

// appendLog writes the SetValueCommand to the WAL
func (s *DurableKVStore) appendLog(key, value string) int64 {
	command := &SetValueCommand{Key: key, Value: value}
	b, _ := command.SerializeToBytes()
	return s.wal.WriteEntryBytes(b)
}

// applyLog reads all the entries from the WAL and applies them to the key-value store
func (s *DurableKVStore) applyLog() {
	entries := s.wal.ReadAll()
	s.applyEntries(entries)
}

// applyEntries applies all WAL entries to the key-value store
func (s *DurableKVStore) applyEntries(entries []*Entry) {
	for _, entry := range entries {
		command := deserialize(entry)
		if setValueCmd, ok := command.(*SetValueCommand); ok {
			s.kv[setValueCmd.GetKey()] = setValueCmd.GetValue()
		}
	}
}

// deserialize converts a WAL entry to a Command
func deserialize(entry *Entry) Command {
	cmd, _ := Deserialize(bytes.NewReader(entry.Data))
	return cmd
}

// Close gracefully closes the WAL and clears the key-value store
func (s *DurableKVStore) Close() {
	s.wal.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv = make(map[string]string)
}

// Values returns all the values currently in the key-value store
func (s *DurableKVStore) Values() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]string, 0, len(s.kv))
	for _, value := range s.kv {
		values = append(values, value)
	}
	return values
}
