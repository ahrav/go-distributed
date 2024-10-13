package netpkg

import (
	"log"
	"net"
	"sync"

	"github.com/ahrav/go-distributed/replicate/common"
)

// SocketListener listens for incoming TCP connections and handles them.
type SocketListener struct {
	listenAddress   *common.InetAddressAndPort
	requestConsumer RequestConsumer
	logger          *log.Logger

	listener net.Listener

	connectionsMu sync.Mutex
	connections   map[*ConnectionHandler]struct{}

	shutdownChan chan struct{}
	wg           sync.WaitGroup
}

// NewSocketListener creates a new SocketListener.
func NewSocketListener(
	requestConsumer RequestConsumer,
	listenAddress *common.InetAddressAndPort,
	logger *log.Logger,
) (*SocketListener, error) {
	addrStr := listenAddress.String()
	listener, err := net.Listen("tcp", addrStr)
	if err != nil {
		return nil, err
	}

	return &SocketListener{
		listenAddress:   listenAddress,
		requestConsumer: requestConsumer,
		logger:          logger,
		listener:        listener,
		connections:     make(map[*ConnectionHandler]struct{}),
		shutdownChan:    make(chan struct{}),
	}, nil
}

// Start begins accepting incoming TCP connections.
func (sl *SocketListener) Start() {
	sl.logger.Printf("SocketListener started on %s", sl.listenAddress.String())

	sl.wg.Add(1)
	go sl.acceptConnections()
}

// acceptConnections handles incoming connections.
func (sl *SocketListener) acceptConnections() {
	defer sl.wg.Done()

	for {
		conn, err := sl.listener.Accept()
		if err != nil {
			select {
			case <-sl.shutdownChan:
				// Shutdown signal received, stop accepting new connections.
				return
			default:
				sl.logger.Printf("Failed to accept connection: %v", err)
				continue
			}
		}

		sl.logger.Printf("Accepted connection from %s", conn.RemoteAddr().String())

		connectionHandler := NewConnectionHandler(conn, sl.requestConsumer, sl.logger)

		sl.addConnection(connectionHandler)

		sl.wg.Add(1)
		go func() {
			defer sl.wg.Done()
			connectionHandler.Handle()
			sl.removeConnection(connectionHandler)
		}()
	}
}

// addConnection adds a new ConnectionHandler to the active connections map.
func (sl *SocketListener) addConnection(conn *ConnectionHandler) {
	sl.connectionsMu.Lock()
	defer sl.connectionsMu.Unlock()
	sl.connections[conn] = struct{}{}
}

// removeConnection removes a ConnectionHandler from the active connections map.
func (sl *SocketListener) removeConnection(conn *ConnectionHandler) {
	sl.connectionsMu.Lock()
	defer sl.connectionsMu.Unlock()
	delete(sl.connections, conn)
}

// Shutdown gracefully shuts down the SocketListener by closing the listener and all active connections.
func (sl *SocketListener) Shutdown() {
	sl.logger.Println("SocketListener shutting down...")

	// Signal shutdown.
	close(sl.shutdownChan)

	// Close the listener to stop accepting new connections.
	err := sl.listener.Close()
	if err != nil {
		sl.logger.Printf("Error closing listener: %v", err)
	}

	// Close all active connections.
	sl.connectionsMu.Lock()
	for conn := range sl.connections {
		conn.Shutdown()
	}
	sl.connectionsMu.Unlock()

	sl.wg.Wait()

	sl.logger.Println("SocketListener shutdown complete.")
}
