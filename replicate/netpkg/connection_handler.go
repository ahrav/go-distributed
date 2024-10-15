package netpkg

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/ahrav/go-distributed/replicate/common"
)

var _ common.ClientConnection = (*ConnectionHandler)(nil)

// ConnectionHandler handles an individual TCP connection.
type ConnectionHandler struct {
	conn            net.Conn
	requestConsumer RequestConsumer
	logger          *log.Logger

	writeChan    chan common.RequestOrResponse
	closeOnce    sync.Once
	closeChan    chan struct{}
	wg           sync.WaitGroup
	shutdownOnce sync.Once
}

// NewConnectionHandler creates a new ConnectionHandler instance.
func NewConnectionHandler(conn net.Conn, consumer RequestConsumer, logger *log.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		conn:            conn,
		requestConsumer: consumer,
		logger:          logger,
		writeChan:       make(chan common.RequestOrResponse, 100), // Buffered channel to prevent blocking
		closeChan:       make(chan struct{}),
	}
}

// Handle manages the connection, starting read and write loops.
func (ch *ConnectionHandler) Handle() {
	ch.wg.Add(2)
	go ch.readLoop()
	go ch.writeLoop()
	ch.wg.Wait()
	ch.Close()
}

// readLoop continuously reads messages from the connection.
func (ch *ConnectionHandler) readLoop() {
	defer ch.wg.Done()

	reader := bufio.NewReader(ch.conn)

	for {
		select {
		case <-ch.closeChan:
			return
		default:
			// Read the length prefix (4 bytes, big-endian).
			lengthBytes := make([]byte, 4)
			_, err := io.ReadFull(reader, lengthBytes)
			if err != nil {
				if err != io.EOF {
					ch.logger.Printf("Error reading length prefix: %v", err)
				}
				ch.Shutdown()
				return
			}

			msgLength := binary.BigEndian.Uint32(lengthBytes)
			if msgLength == 0 {
				ch.logger.Println("Received message with zero length. Ignoring.")
				continue
			}

			// Read the serialized message.
			serialized := make([]byte, msgLength)
			_, err = io.ReadFull(reader, serialized)
			if err != nil {
				ch.logger.Printf("Error reading serialized message: %v", err)
				ch.Shutdown()
				return
			}

			// Deserialize the RequestOrResponse using CBOR.
			var req common.RequestOrResponse
			err = common.Deserialize(serialized, &req)
			if err != nil {
				ch.logger.Printf("Error deserializing RequestOrResponse: %v", err)
				ch.Shutdown()
				return
			}

			// Create a Header instance.
			header := common.Header{
				FromAddress:   req.GetFromAddress(),
				CorrelationId: *req.GetCorrelationID(),
				MessageId:     common.MessageID(*req.GetRequestID()),
			}

			message := common.NewMessageWithClient(req, header, ch)

			// Process the message using RequestConsumer.
			ch.requestConsumer.Accept(*message)
		}
	}
}

// writeLoop continuously listens for outgoing messages and sends them to the connection.
func (ch *ConnectionHandler) writeLoop() {
	defer ch.wg.Done()

	for {
		select {
		case <-ch.closeChan:
			return
		case response, ok := <-ch.writeChan:
			if !ok {
				return
			}

			// Serialize the response using CBOR.
			serialized, err := common.Serialize(&response)
			if err != nil {
				ch.logger.Printf("Failed to serialize response: %v", err)
				continue
			}

			// Prefix with length (4 bytes, big-endian).
			length := uint32(len(serialized))
			lengthBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lengthBytes, length)

			// Write length prefix and serialized data.
			_, err = ch.conn.Write(lengthBytes)
			if err != nil {
				ch.logger.Printf("Failed to write length prefix: %v", err)
				ch.Shutdown()
				return
			}

			_, err = ch.conn.Write(serialized)
			if err != nil {
				ch.logger.Printf("Failed to write serialized response: %v", err)
				ch.Shutdown()
				return
			}
		}
	}
}

// Write sends a RequestOrResponse over the connection.
func (ch *ConnectionHandler) Write(response common.RequestOrResponse) error {
	select {
	case ch.writeChan <- response:
		return nil
	case <-ch.closeChan:
		return fmt.Errorf("connection is closed")
	}
}

// Close gracefully closes the connection and all associated resources.
func (ch *ConnectionHandler) Close() error {
	var err error
	ch.closeOnce.Do(func() {
		close(ch.closeChan)   // Signal goroutines to exit
		close(ch.writeChan)   // Close the write channel
		err = ch.conn.Close() // Close the underlying connection
		ch.logger.Printf("Connection from %s closed.", ch.conn.RemoteAddr().String())
	})
	return err
}

// Shutdown initiates a graceful shutdown of the connection.
func (ch *ConnectionHandler) Shutdown() { ch.Close() }
