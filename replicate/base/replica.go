package base

import (
	"errors"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ahrav/go-distributed/replicate/common"
	"github.com/ahrav/go-distributed/replicate/heartbeat"
	"github.com/ahrav/go-distributed/replicate/netpkg"
)

// ReplicaInitializer defines methods that concrete replicas should implement.
type ReplicaInitializer interface {
	OnStart()
	RegisterHandlers()
}

// HeartbeatHandler defines methods related to heartbeat mechanisms.
type HeartbeatHandler interface {
	SendHeartbeats()
	CheckLeader()
}

// Replica represents the base structure for replicas (server) in the system.
// It handles communication, request management, and heartbeat mechanisms.
type Replica struct {
	name                    string
	config                  common.Config
	peerListener            *netpkg.SocketListener
	clientListener          *netpkg.SocketListener
	clientConnectionAddress *common.InetAddressAndPort
	peerConnectionAddress   *common.InetAddressAndPort
	network                 *netpkg.Network
	requestWaitingList      *netpkg.RequestWaitingList[int, common.RequestOrResponse]
	clock                   *common.SystemClock
	peerAddresses           []*common.InetAddressAndPort

	// Single-threaded task executor using a channel and a dedicated goroutine.
	wg        sync.WaitGroup
	taskQueue chan func()

	handlers map[common.MessageID]*MessageHandler

	heartBeatScheduler *heartbeat.Scheduler
	heartbeatChecker   *heartbeat.Scheduler
	heartBeatInterval  time.Duration
	heartbeatTimeout   time.Duration

	logger *log.Logger

	mu                  sync.RWMutex
	heartbeatReceivedNs int64

	initializer      ReplicaInitializer
	heartbeatHandler HeartbeatHandler
}

// MessageHandler encapsulates the handler function and request class for a specific MessageID.
type MessageHandler struct {
	RequestClass any
	Handler      func(message common.Message[any]) any
}

// RequestConsumerImpl implements the RequestConsumer interface.
// It delegates the Accept and Close methods to the Replica's handler methods.
type RequestConsumerImpl struct {
	replica *Replica
	handler func(message common.Message[common.RequestOrResponse])
}

// Accept handles incoming messages by delegating to the Replica's handler.
func (rc *RequestConsumerImpl) Accept(request common.Message[common.RequestOrResponse]) {
	rc.handler(request)
}

// Close handles connection closures.
func (rc *RequestConsumerImpl) Close(connection common.ClientConnection) {
	// Implement any cleanup or logging as needed.
	rc.replica.logger.Printf("Connection closed: %v", connection)
}

// NewReplica initializes and returns a new Replica instance.
// It sets up listeners, schedulers, and starts the necessary services.
func NewReplica(
	name string,
	config common.Config,
	clock *common.SystemClock,
	clientConnAddr *common.InetAddressAndPort,
	peerConnAddr *common.InetAddressAndPort,
	peerAddrs []*common.InetAddressAndPort,
	logger *log.Logger,
	initializer ReplicaInitializer,
	heartbeatHandler HeartbeatHandler,
) (*Replica, error) {
	network := netpkg.NewNetwork(logger)
	requestWaitingList := netpkg.NewRequestWaitingList[int, common.RequestOrResponse](clock, 1000*time.Millisecond, logger)

	replica := &Replica{
		name:                    name,
		config:                  config,
		network:                 network,
		requestWaitingList:      requestWaitingList,
		clock:                   clock,
		peerAddresses:           peerAddrs,
		clientConnectionAddress: clientConnAddr,
		peerConnectionAddress:   peerConnAddr,
		handlers:                make(map[common.MessageID]*MessageHandler),
		logger:                  logger,
		heartBeatInterval:       100 * time.Millisecond,
		heartbeatTimeout:        500 * time.Millisecond, // 5 * heartBeatInterval
		initializer:             initializer,
		heartbeatHandler:        heartbeatHandler,
	}

	// Start the single-threaded task executor.
	replica.wg.Add(1)
	go replica.runTaskQueue()

	// Initialize HeartBeatSchedulers.
	replica.heartBeatScheduler = heartbeat.NewHeartBeatScheduler(
		replica.heartbeatHandler.SendHeartbeats,
		replica.heartBeatInterval.Milliseconds(),
	)
	replica.heartbeatChecker = heartbeat.NewHeartBeatScheduler(
		replica.heartbeatHandler.CheckLeader,
		replica.heartbeatTimeout.Milliseconds(),
	)

	// Initialize the listeners with Replica's methods as handlers.
	// TODO: Not sure if this is the best way to do this...
	peerConsumer := &RequestConsumerImpl{
		replica: replica,
		handler: replica.handlePeerMessage,
	}

	clientConsumer := &RequestConsumerImpl{
		replica: replica,
		handler: replica.handleClientRequest,
	}

	var err error
	replica.peerListener, err = netpkg.NewSocketListener(
		peerConsumer,
		peerConnAddr,
		logger,
	)
	if err != nil {
		return nil, err
	}

	replica.clientListener, err = netpkg.NewSocketListener(
		clientConsumer,
		clientConnAddr,
		logger,
	)
	if err != nil {
		return nil, err
	}

	// Start HeartBeatSchedulers.
	replica.heartBeatScheduler.Start()
	replica.heartbeatChecker.Start()

	// Start listeners.
	replica.peerListener.Start()
	replica.clientListener.Start()

	// Delegate to the initializer to register handlers.
	replica.initializer.RegisterHandlers()

	return replica, nil
}

// runTaskQueue runs the task queue to ensure single-threaded task execution.
func (r *Replica) runTaskQueue() {
	defer r.wg.Done()
	for task := range r.taskQueue {
		task()
	}
}

// enqueueTask adds a task to the task queue for single-threaded execution.
func (r *Replica) enqueueTask(task func()) { r.taskQueue <- task }

// Start begins the replica's operations, including listeners and custom startup logic.
func (r *Replica) Start() {
	r.peerListener.Start()
	r.clientListener.Start()
}

// handlePeerMessage processes incoming messages from peer replicas.
func (r *Replica) handlePeerMessage(message common.Message[common.RequestOrResponse]) {
	handler, exists := r.handlers[message.Header.MessageId]
	if !exists {
		r.logger.Printf("No handler found for MessageID: %v", message.Header.MessageId)
		return
	}

	deserializedRequest := r.deserialize(message.Payload, handler.RequestClass)

	// Enqueue the task for single-threaded execution.
	r.enqueueTask(func() {
		r.markHeartbeatReceived()
		msg := common.Message[any]{
			Payload: deserializedRequest,
			Header:  message.Header,
		}
		handler.Handler(msg)
	})
}

// handleClientRequest processes incoming client requests.
func (r *Replica) handleClientRequest(message common.Message[common.RequestOrResponse]) {
	handler, exists := r.handlers[message.Header.MessageId]
	if !exists {
		r.logger.Printf("No handler found for MessageID: %v", message.Header.MessageId)
		return
	}

	deserializedRequest := r.deserialize(message.Payload, handler.RequestClass)

	// Enqueue the task for single-threaded execution.
	r.enqueueTask(func() {
		// Execute the handler
		response := handler.Handler(common.Message[any]{
			Payload: deserializedRequest,
			Header:  message.Header,
		})

		if respWrapper, ok := response.(common.Response[any]); ok {
			if respWrapper.IsError() {
				r.respondToClient(
					nil,
					errors.New(*respWrapper.GetErrorMessage()),
					message.Header.CorrelationId,
					message.GetClientConnection(),
					*message.Payload.RequestID,
				)
			} else {
				r.respondToClient(
					respWrapper.GetResult(),
					nil,
					message.Header.CorrelationId,
					message.GetClientConnection(),
					*message.Payload.RequestID,
				)
			}
		} else {
			// Handle unexpected response type.
			r.logger.Printf("Unexpected response type: %T", response)
			r.respondToClient(
				response,
				errors.New("unexpected response type"),
				message.GetCorrelationId(),
				message.GetClientConnection(),
				*message.Payload.RequestID,
			)
		}
	})
}

// respondToClient sends a response or error back to the client.
func (r *Replica) respondToClient(
	response any,
	err error,
	correlationID int,
	clientConn common.ClientConnection,
	requestID int,
) {
	var respMsg common.RequestOrResponse
	if err != nil {
		b, _ := serialize(err.Error())
		respMsg = common.RequestOrResponse{
			RequestID:       &requestID,
			MessageBodyJSON: b,
			CorrelationID:   &correlationID,
			IsErr:           true,
		}
	} else {
		b, _ := serialize(response)
		respMsg = common.RequestOrResponse{
			RequestID:       &requestID,
			MessageBodyJSON: b,
			CorrelationID:   &correlationID,
			IsErr:           false,
		}
	}

	err = clientConn.Write(respMsg)
	if err != nil {
		r.logger.Printf("Error writing to client: %v", err)
	}
}

// AddClockSkew adds a clock skew duration to the system clock.
func (r *Replica) AddClockSkew(duration time.Duration) { r.clock.AddClockSkew(duration) }

// SetClock sets a new system clock.
func (r *Replica) SetClock(clock *common.SystemClock) { r.clock = clock }

// handlesMessage registers a handler for one-way message passing communication.
func (r *Replica) handlesMessage(messageID common.MessageID, handler func(common.Message[any]), requestClass any) {
	r.handlers[messageID] = &MessageHandler{
		RequestClass: requestClass,
		Handler: func(msg common.Message[any]) any {
			handler(msg)
			return nil
		},
	}
}

// handlesRequestAsync registers an asynchronous request handler that expects a response.
func (r *Replica) handlesRequestAsync(
	messageID common.MessageID,
	handler func(common.Message[any]) (any, error),
	requestClass any,
) {
	r.handlers[messageID] = &MessageHandler{
		RequestClass: requestClass,
		Handler: func(msg common.Message[any]) any {
			response, err := handler(msg)
			if err != nil {
				r.requestWaitingList.HandleError(msg.GetCorrelationId(), err)
				return nil
			}
			return response
		},
	}
}

// handleResponse processes a received response message and delegates it to the RequestWaitingList.
func (r *Replica) handleResponse(message common.Message[common.RequestOrResponse]) {
	r.requestWaitingList.HandleResponse(message.GetCorrelationId(), message.Payload)
}

// getServerID returns the server ID from the configuration.
func (r *Replica) getServerID() int { return r.config.GetServerId() }

// newCorrelationID generates a new random correlation ID.
func (r *Replica) newCorrelationID() int { return rand.Int() }

// getNoOfReplicas returns the number of peer replicas.
func (r *Replica) getNoOfReplicas() int { return len(r.peerAddresses) }

// getClientConnectionAddress returns the client's connection address.
func (r *Replica) getClientConnectionAddress() *common.InetAddressAndPort {
	return r.clientConnectionAddress
}

// getPeerConnectionAddress returns the peer's connection address.
func (r *Replica) getPeerConnectionAddress() *common.InetAddressAndPort {
	return r.peerConnectionAddress
}

// deserialize converts the message payload into the specified type.
func (r *Replica) deserialize(request common.RequestOrResponse, clazz any) error {
	return common.Deserialize(request.GetMessageBodyJSON(), clazz)
}

// dropMessagesTo drops all messages to the specified replica.
func (r *Replica) dropMessagesTo(n *Replica) {
	r.network.DropMessagesTo(n.getPeerConnectionAddress())
}

// reconnectTo reconnects to the specified replica.
func (r *Replica) reconnectTo(n *Replica) {
	r.network.ReconnectTo(n.getPeerConnectionAddress())
}

// dropAfterNMessagesTo drops messages after a specified number of messages to the replica.
func (r *Replica) dropAfterNMessagesTo(n *Replica, dropAfterNoOfMessages int) {
	r.network.DropMessagesAfter(n.getPeerConnectionAddress(), dropAfterNoOfMessages)
}

// addDelayForMessagesTo adds a delay for messages to the specified replica.
func (r *Replica) addDelayForMessagesTo(rpl *Replica) {
	r.addDelayForMessagesToAfterNMessages(rpl, 0)
}

// addDelayForMessagesToAfterNMessages adds a delay after a certain number of messages to the replica.
func (r *Replica) addDelayForMessagesToAfterNMessages(n *Replica, noOfMessages int) {
	r.network.AddDelayForMessagesToAfterNMessages(n.getPeerConnectionAddress(), noOfMessages)
}

// addDelayForMessagesOfType adds a delay for messages of a specific type to the replica.
func (r *Replica) addDelayForMessagesOfType(n *Replica, messageID common.MessageID) {
	r.network.AddDelayForMessagesOfType(n.getPeerConnectionAddress(), messageID)
}

// quorum calculates and returns the quorum size based on the number of replicas.
func (r *Replica) quorum() int { return (r.getNoOfReplicas() / 2) + 1 }

// serialize converts an object into a byte slice using JSON serialization.
func serialize(e any) ([]byte, error) { return common.Serialize(e) }

// Shutdown gracefully shuts down the replica, including listeners and schedulers.
func (r *Replica) Shutdown() {
	r.peerListener.Shutdown()
	r.clientListener.Shutdown()
	r.heartbeatChecker.Stop()
	r.heartBeatScheduler.Stop()
	r.network.CloseAllConnections()
}

// elapsedTimeSinceLastHeartbeat returns the duration since the last heartbeat was received.
func (r *Replica) elapsedTimeSinceLastHeartbeat() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return time.Duration(r.clock.NanoTime()-r.heartbeatReceivedNs) * time.Nanosecond
}

// resetHeartbeat updates the timestamp of the last received heartbeat.
func (r *Replica) resetHeartbeat(heartbeatReceivedNs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heartbeatReceivedNs = heartbeatReceivedNs
}

// getName returns the name of the replica.
func (r *Replica) getName() string { return r.name }

// markHeartbeatReceived updates the heartbeatReceivedNs to the current time.
func (r *Replica) markHeartbeatReceived() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heartbeatReceivedNs = r.clock.NanoTime()
}
