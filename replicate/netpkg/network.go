package netpkg

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ahrav/go-distributed/replicate/common"
)

// Network manages sending messages with possible drops and delays to simulate network conditions.
type Network struct {
	logger *log.Logger

	// Constants.
	messageDelay time.Duration

	// Synchronization.
	mu sync.RWMutex

	// Configuration for dropping and delaying messages.
	dropRequestsTo     map[string]struct{}
	noOfMessages       map[string]int
	dropAfter          map[string]int
	delayMessagesAfter map[string]int
	delayMessageTypes  map[string]map[common.MessageId]struct{}

	// Connection pool.
	connectionPool map[string]*SocketClient[common.RequestOrResponse]
}

// NewNetwork creates a new Network instance..
func NewNetwork(logger *log.Logger) *Network {
	return &Network{
		logger:             logger,
		messageDelay:       1000 * time.Millisecond, // MESSAGE_DELAY = 1000ms
		dropRequestsTo:     make(map[string]struct{}),
		noOfMessages:       make(map[string]int),
		dropAfter:          make(map[string]int),
		delayMessagesAfter: make(map[string]int),
		delayMessageTypes:  make(map[string]map[common.MessageId]struct{}),
		connectionPool:     make(map[string]*SocketClient[common.RequestOrResponse]),
	}
}

// addrKey converts InetAddressAndPort to a unique string key.
func (n *Network) addrKey(address common.InetAddressAndPort) string {
	return address.String()
}

// SendOneWay sends a message to the specified address without expecting a response.
// It may drop or delay the message based on the network configuration.
func (n *Network) SendOneWay(address common.InetAddressAndPort, message common.RequestOrResponse) error {
	key := n.addrKey(address)

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if messages to this address should be dropped.
	if _, dropped := n.dropRequestsTo[key]; dropped || n.noOfMessagesReachedLimit(key) {
		n.removeExistingConnection(key)
		return fmt.Errorf("unable to connect to %s", key)
	}

	// Check if messages of this type should be delayed.
	if n.shouldDelayMessagesOfType(key, *message.GetRequestID()) {
		n.sendAfterDelay(key, address, message, n.messageDelay)
		return nil
	}

	// Check if messages to this address should be delayed after a certain number of messages.
	if n.shouldDelayMessagesTo(key) {
		n.sendAfterDelay(key, address, message, n.messageDelay)
		return nil
	}

	// Send the message immediately.
	n.logger.Printf("Sending message ID %d to %s", *message.GetRequestID(), key)
	return n.sendMessage(key, address, message)
}

// noOfMessagesReachedLimit checks if the number of messages sent to the address has reached the drop limit.
func (n *Network) noOfMessagesReachedLimit(key string) bool {
	dropAfter, exists := n.dropAfter[key]
	if !exists {
		return false
	}
	noOfMsgs, exists := n.noOfMessages[key]
	return exists && noOfMsgs >= dropAfter
}

// shouldDelayMessagesOfType checks if messages of the specified MessageId should be delayed for the address.
func (n *Network) shouldDelayMessagesOfType(key string, messageId int) bool {
	ids, exists := n.delayMessageTypes[key]
	if !exists {
		return false
	}
	msgId := common.MessageId(messageId)
	_, exists = ids[msgId]
	return exists
}

// shouldDelayMessagesTo checks if messages to the address should be delayed after a certain number of messages.
func (n *Network) shouldDelayMessagesTo(key string) bool {
	delayAfter, exists := n.delayMessagesAfter[key]
	if !exists {
		return false
	}
	noOfMsgs, exists := n.noOfMessages[key]
	if !exists {
		return false
	}
	return noOfMsgs >= delayAfter
}

// sendAfterDelay schedules the message to be sent after the specified delay.
func (n *Network) sendAfterDelay(
	key string,
	address common.InetAddressAndPort,
	message common.RequestOrResponse,
	delay time.Duration,
) {
	n.logger.Printf("Delaying message ID %d to %s by %v", *message.GetRequestID(), key, delay)
	go func() {
		time.Sleep(delay)
		n.mu.Lock()
		defer n.mu.Unlock()

		// Check again if the connection is not dropped in the meantime.
		if _, dropped := n.dropRequestsTo[key]; dropped || n.noOfMessagesReachedLimit(key) {
			n.removeExistingConnection(key)
			n.logger.Printf("After delay: unable to connect to %s", key)
			return
		}

		// Check if messages of this type should still be delayed.
		if n.shouldDelayMessagesOfType(key, *message.GetRequestID()) || n.shouldDelayMessagesTo(key) {
			// Re-schedule the delay
			n.sendAfterDelay(key, address, message, n.messageDelay)
			return
		}

		// Send the message.
		n.logger.Printf("Sending delayed message ID %d to %s", *message.GetRequestID(), key)
		err := n.sendMessage(key, address, message)
		if err != nil {
			n.logger.Printf("Failed to send delayed message ID %d to %s: %v", *message.GetRequestID(), key, err)
		}
	}()
}

// sendMessage sends the message using a SocketClient, updating the message count.
func (n *Network) sendMessage(key string, address common.InetAddressAndPort, message common.RequestOrResponse) error {
	socketClient, err := n.getOrCreateConnection(key, address)
	if err != nil {
		return err
	}

	err = socketClient.SendOneway(message)
	if err != nil {
		return fmt.Errorf("failed to send oneway message: %w", err)
	}

	n.noOfMessages[key]++
	return nil
}

// sendAndReceive sends a message and waits for a response.
func (n *Network) sendAndReceive(
	address common.InetAddressAndPort,
	message common.RequestOrResponse,
) (*common.RequestOrResponse, error) {
	key := n.addrKey(address)

	n.mu.Lock()
	socketClient, err := n.getOrCreateConnection(key, address)
	if err != nil {
		n.mu.Unlock()
		return nil, err
	}

	err = socketClient.SendOneway(message)
	if err != nil {
		n.mu.Unlock()
		return nil, fmt.Errorf("failed to send oneway message: %w", err)
	}

	n.noOfMessages[key]++
	n.mu.Unlock()

	response, err := socketClient.BlockingSend(message)
	if err != nil {
		return nil, fmt.Errorf("failed to perform blocking send: %w", err)
	}

	return response, nil
}

// getOrCreateConnection retrieves an existing SocketClient or creates a new one.
func (n *Network) getOrCreateConnection(
	key string,
	address common.InetAddressAndPort,
) (*SocketClient[common.RequestOrResponse], error) {
	socketClient, exists := n.connectionPool[key]
	if exists && !socketClient.IsClosed() {
		return socketClient, nil
	}

	socketClient, err := NewSocketClient[common.RequestOrResponse](&address, 5000) // 5000 ms read timeout
	if err != nil {
		return nil, fmt.Errorf("failed to create SocketClient for %s: %w", key, err)
	}

	n.connectionPool[key] = socketClient
	return socketClient, nil
}

// removeExistingConnection closes and removes the SocketClient for the specified key.
func (n *Network) removeExistingConnection(key string) {
	socketClient, exists := n.connectionPool[key]
	if exists {
		err := socketClient.Close()
		if err != nil {
			n.logger.Printf("Failed to close connection to %s: %v", key, err)
		}
		delete(n.connectionPool, key)
	}
}

// DropMessagesTo configures the network to drop all messages to the specified address.
func (n *Network) DropMessagesTo(address common.InetAddressAndPort) {
	key := n.addrKey(address)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.dropRequestsTo[key] = struct{}{}
}

// ReconnectTo removes the drop and delay configurations for the specified address,
// allowing normal message sending.
func (n *Network) ReconnectTo(address common.InetAddressAndPort) {
	key := n.addrKey(address)

	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.dropRequestsTo, key)
	delete(n.dropAfter, key)
	delete(n.delayMessagesAfter, key)
	delete(n.noOfMessages, key)
	delete(n.delayMessageTypes, key)
}

// DropMessagesAfter configures the network to drop messages to the specified address after a certain number.
func (n *Network) DropMessagesAfter(address common.InetAddressAndPort, dropAfterNoOfMessages int) {
	key := n.addrKey(address)

	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.noOfMessages, key)
	n.dropAfter[key] = dropAfterNoOfMessages
}

// AddDelayForMessagesToAfterNMessages configures the network to delay messages to the address after N messages.
func (n *Network) AddDelayForMessagesToAfterNMessages(address common.InetAddressAndPort, noOfMessages int) {
	key := n.addrKey(address)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.delayMessagesAfter[key] = noOfMessages
}

// AddDelayForMessagesOfType configures the network to delay messages of a specific type to the address.
func (n *Network) AddDelayForMessagesOfType(address common.InetAddressAndPort, messageId common.MessageId) {
	key := n.addrKey(address)

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.delayMessageTypes[key]; !exists {
		n.delayMessageTypes[key] = make(map[common.MessageId]struct{})
	}
	n.delayMessageTypes[key][messageId] = struct{}{}
}

// CloseAllConnections closes all active connections in the connection pool.
func (n *Network) CloseAllConnections() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for key, socketClient := range n.connectionPool {
		err := socketClient.Close()
		if err != nil {
			n.logger.Printf("Failed to close connection to %s: %v", key, err)
		}
		delete(n.connectionPool, key)
	}
}
