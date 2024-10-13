package netpkg

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ahrav/go-distributed/replicate/common"
)

// SocketClient is a generic TCP client for sending and receiving messages of type T.
type SocketClient[T any] struct {
	conn          net.Conn
	readTimeoutMs int

	mu      sync.RWMutex
	closed  bool
	closeCh chan struct{}
	waitGrp sync.WaitGroup
}

// NewSocketClient establishes a new TCP connection to the given address.
func NewSocketClient[T any](address *common.InetAddressAndPort, readTimeoutMs int) (*SocketClient[T], error) {
	conn, err := net.Dial("tcp", address.String())
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", address.String(), err)
	}

	client := &SocketClient[T]{
		conn:          conn,
		readTimeoutMs: readTimeoutMs,
		closeCh:       make(chan struct{}),
	}

	return client, nil
}

// SendOneway serializes the message of type T and sends it over the TCP connection.
func (sc *SocketClient[T]) SendOneway(message T) error {
	serializedMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	return sc.sendOnewaySerialized(sc.conn, serializedMessage)
}

// sendOnewaySerialized sends the serialized message bytes over the provided socket.
func (sc *SocketClient[T]) sendOnewaySerialized(socket net.Conn, serializedMessage []byte) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.closed {
		return fmt.Errorf("socket is closed")
	}

	// Prepare the message with a 4-byte length prefix.
	msgLength := uint32(len(serializedMessage))
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, msgLength)

	// Write length prefix.
	_, err := socket.Write(lengthBytes)
	if err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write serialized message.
	_, err = socket.Write(serializedMessage)
	if err != nil {
		return fmt.Errorf("failed to write serialized message: %w", err)
	}

	return nil
}

// Read reads a message from the TCP connection and returns the raw bytes.
func (sc *SocketClient[T]) Read() ([]byte, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.closed {
		return nil, fmt.Errorf("socket is closed")
	}

	// Set read deadline.
	err := sc.conn.SetReadDeadline(time.Now().Add(time.Duration(sc.readTimeoutMs) * time.Millisecond))
	if err != nil {
		return nil, err
	}
	defer sc.conn.SetReadDeadline(time.Time{}) // Clear deadline

	// Read the length prefix.
	lengthBytes := make([]byte, 4)
	_, err = io.ReadFull(sc.conn, lengthBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}
	msgLength := binary.BigEndian.Uint32(lengthBytes)
	if msgLength == 0 {
		return nil, fmt.Errorf("received message with zero length")
	}

	// Read the serialized message.
	serialized := make([]byte, msgLength)
	_, err = io.ReadFull(sc.conn, serialized)
	if err != nil {
		return nil, fmt.Errorf("failed to read serialized message: %w", err)
	}

	return serialized, nil
}

// BlockingSend sends a message of type T and waits for a response, returning a RequestOrResponse.
func (sc *SocketClient[T]) BlockingSend(message T) (*common.RequestOrResponse, error) {
	// Serialize and send the message.
	err := sc.SendOneway(message)
	if err != nil {
		return nil, fmt.Errorf("failed to send oneway message: %w", err)
	}

	// Read the response.
	responseBytes, err := sc.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Deserialize the response into RequestOrResponse.
	var response common.RequestOrResponse
	err = json.Unmarshal(responseBytes, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize response: %w", err)
	}

	return &response, nil
}

// Close gracefully closes the TCP connection.
func (sc *SocketClient[T]) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.closed {
		return nil
	}

	close(sc.closeCh)
	sc.closed = true
	return sc.conn.Close()
}

// IsClosed checks if the TCP connection is closed.
func (sc *SocketClient[T]) IsClosed() bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.closed
}
