package common

import (
	"time"
)

// Peer represents a peer in the cluster.
type Peer struct {
	Id int // The unique identifier of the peer.
}

// Config represents the configuration settings for the Write-Ahead Log (WAL) and cluster.
type Config struct {
	walDir              string // The directory where the WAL files are stored.
	maxLogSize          int64  // The maximum size of the log file.
	logMaxDurationMs    int64  // The maximum duration in milliseconds for the log.
	servers             []Peer // The list of peers in the cluster.
	serverId            int    // The unique identifier of the server.
	electionTimeoutMs   int64  // The election timeout in milliseconds.
	heartBeatIntervalMs int64  // The heartbeat interval in milliseconds.
	followerTimeoutMs   int64  // The follower timeout in milliseconds.
	supportLogGroup     bool   // Indicates if log grouping is supported.
	doAsyncRepair       bool   // Indicates if asynchronous repair is enabled.
}

// NewConfig creates a new Config with the specified WAL directory and default values for other fields.
func NewConfig(walDir string) *Config {
	return &Config{
		walDir:              walDir,
		maxLogSize:          int64(^uint64(0) >> 1), // Max value for int64
		logMaxDurationMs:    int64(^uint64(0) >> 1), // Max value for int64
		electionTimeoutMs:   2000,
		heartBeatIntervalMs: 100,
		followerTimeoutMs:   5000,
	}
}

// WithElectionTimeoutMs sets the election timeout in milliseconds and returns the updated Config.
func (c *Config) WithElectionTimeoutMs(electionTimeoutMs int64) *Config {
	c.electionTimeoutMs = electionTimeoutMs
	return c
}

// WithWalDir sets the WAL directory and returns the updated Config.
func (c *Config) WithWalDir(walDir string) *Config {
	c.walDir = walDir
	return c
}

// WithHeartBeatIntervalMs sets the heartbeat interval in milliseconds and returns the updated Config.
func (c *Config) WithHeartBeatIntervalMs(heartBeatIntervalMs int64) *Config {
	c.heartBeatIntervalMs = heartBeatIntervalMs
	return c
}

// WithMaxLogSize sets the maximum log size and returns the updated Config.
func (c *Config) WithMaxLogSize(fileSize int64) *Config {
	c.maxLogSize = fileSize
	return c
}

// NewConfigWithMaxLogSize creates a new Config with the specified WAL directory and maximum log size.
func NewConfigWithMaxLogSize(walDir string, maxLogSize int64) *Config {
	return NewConfigWithServers(walDir, maxLogSize, []Peer{})
}

// NewConfigWithServers creates a new Config with the specified WAL directory, maximum log size, and list of servers.
func NewConfigWithServers(walDir string, maxLogSize int64, servers []Peer) *Config {
	return &Config{
		walDir:           walDir,
		maxLogSize:       maxLogSize,
		servers:          servers,
		serverId:         0,
		logMaxDurationMs: int64(^uint64(0) >> 1), // Max value for int64
	}
}

// NewConfigWithServerId creates a new Config with the specified server ID, WAL directory, and list of servers.
func NewConfigWithServerId(serverId int, walDir string, servers []Peer) *Config {
	return &Config{
		serverId:         serverId,
		walDir:           walDir,
		maxLogSize:       1000,
		servers:          servers,
		logMaxDurationMs: int64(^uint64(0) >> 1), // Max value for int64
	}
}

// GetLogMaxDurationMs returns the maximum log duration in milliseconds.
func (c *Config) GetLogMaxDurationMs() int64 { return c.logMaxDurationMs }

// GetWalDir returns the WAL directory.
func (c *Config) GetWalDir() string { return c.walDir }

// GetMaxLogSize returns the maximum log size.
func (c *Config) GetMaxLogSize() int64 { return c.maxLogSize }

const defaultCleanTaskIntervalMs = 1000

// GetCleanTaskIntervalMs returns the interval in milliseconds for the clean task.
func (c *Config) GetCleanTaskIntervalMs() int64 { return defaultCleanTaskIntervalMs }

// GetServers returns the list of servers.
func (c *Config) GetServers() []Peer { return c.servers }

// GetPeers returns the list of peers excluding the current server.
func (c *Config) GetPeers() []Peer {
	var peers []Peer
	for _, p := range c.servers {
		if p.Id != c.serverId {
			peers = append(peers, p)
		}
	}
	return peers
}

// NumberOfServers returns the number of servers.
func (c *Config) NumberOfServers() int { return len(c.GetServers()) }

// GetNumberOfPeers returns the number of peers excluding the current server.
func (c *Config) GetNumberOfPeers() int { return len(c.GetPeers()) }

// GetServerId returns the server ID.
func (c *Config) GetServerId() int { return c.serverId }

// GetElectionTimeoutMs returns the election timeout in milliseconds.
func (c *Config) GetElectionTimeoutMs() int64 { return c.electionTimeoutMs }

// GetHeartBeatIntervalMs returns the heartbeat interval in milliseconds.
func (c *Config) GetHeartBeatIntervalMs() int { return int(c.heartBeatIntervalMs) }

// GetServer returns the server with the specified leader ID.
func (c *Config) GetServer(leaderId int) *Peer {
	for _, p := range c.servers {
		if p.Id == leaderId {
			return &p
		}
	}
	return nil
}

// GetFollowerWaitTimeoutMs returns the follower timeout in milliseconds.
func (c *Config) GetFollowerWaitTimeoutMs() int64 { return c.followerTimeoutMs }

// WithFollowerTimeoutMs sets the follower timeout in milliseconds and returns the updated Config.
func (c *Config) WithFollowerTimeoutMs(followerTimeoutMs int64) *Config {
	c.followerTimeoutMs = followerTimeoutMs
	return c
}

// WithLogMaxDurationMs sets the maximum log duration in milliseconds and returns the updated Config.
func (c *Config) WithLogMaxDurationMs(maxLogDuration int64) *Config {
	c.logMaxDurationMs = maxLogDuration
	return c
}

// WithGroupLog enables log grouping and returns the updated Config.
func (c *Config) WithGroupLog() *Config {
	c.supportLogGroup = true
	return c
}

// SupportLogGroup returns true if log grouping is supported.
func (c *Config) SupportLogGroup() bool { return c.supportLogGroup }

const defaultTransactionTimeoutMs = 2000

// GetTransactionTimeoutMs returns the transaction timeout in milliseconds.
func (c *Config) GetTransactionTimeoutMs() int64 { return defaultTransactionTimeoutMs }

// GetMaxBatchWaitTime returns the maximum batch wait time in nanoseconds.
func (c *Config) GetMaxBatchWaitTime() int64 { return time.Millisecond.Nanoseconds() }

// SetAsyncReadRepair enables asynchronous read repair.
func (c *Config) SetAsyncReadRepair() { c.doAsyncRepair = true }

// IsAsyncReadRepair returns true if asynchronous read repair is enabled.
func (c *Config) IsAsyncReadRepair() bool { return c.doAsyncRepair }

// SetServerId sets the server ID.
func (c *Config) SetServerId(serverId int) { c.serverId = serverId }
