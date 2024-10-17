package quorum

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ahrav/go-distributed/replicate/base"
	"github.com/ahrav/go-distributed/replicate/common"
	"github.com/ahrav/go-distributed/replicate/quorum/messages"
	"github.com/ahrav/go-distributed/replicate/wal"
)

// KVStore is a quorum-based key-value store that ensures consistency across multiple replicas.
// It embeds base.Replica to leverage replication mechanisms and implements the ReplicaInitializer
// and HeartbeatHandler interfaces to handle initialization and heartbeat functionalities respectively.
type KVStore struct {
	*base.Replica

	logger          *log.Logger
	firstGeneration int
	config          *common.Config
	generation      int
	replicas        []*common.InetAddressAndPort
	clientState     *ClientState

	mutex         sync.Mutex
	systemStorage *wal.DurableKVStore
	durableStore  *wal.DurableKVStore
}

// NewQuorumKVStore initializes and returns a new KVStore instance.
// It sets up the replication mechanisms, durable storage, and message handlers required
// for quorum-based operations.
// Parameters:
// - name: the identifier for the KVStore instance.
// - config: configuration settings for the KVStore.
// - clock: system clock for timestamp management.
// - clientConnectionAddress: address for client connections.
// - peerConnectionAddress: address for peer replica connections.
// - replicas: list of addresses of all replica nodes.
// Returns:
// - A pointer to the initialized KVStore.
// - An error if initialization fails.
func NewQuorumKVStore(
	name string,
	config *common.Config,
	clock *common.SystemClock,
	clientConnectionAddress *common.InetAddressAndPort,
	peerConnectionAddress *common.InetAddressAndPort,
	replicas []*common.InetAddressAndPort,
) (*KVStore, error) {
	logger := log.New(os.Stdout, "KVStore: ", log.LstdFlags)

	q := &KVStore{
		logger:          logger,
		firstGeneration: 1,
		config:          config,
		replicas:        replicas,
		clientState:     NewClientState(clock),
	}

	// Assign KVStore as both the initializer and heartbeat handler
	// to integrate with the base Replica's lifecycle.
	initializer := q
	heartbeatHandler := q

	// Initialize the embedded base.Replica with the provided parameters.
	replica, err := base.NewReplica(
		name,
		*config,
		clock,
		clientConnectionAddress,
		peerConnectionAddress,
		replicas,
		logger,
		initializer,
		heartbeatHandler,
	)
	if err != nil {
		return nil, err
	}

	// Embed the initialized Replica into KVStore to utilize replication functionalities.
	q.Replica = replica

	// Create a new configuration with a separate WAL directory for system storage.
	configWithSystemWalDir, err := q.makeNewConfigWithSystemWalDir(config)
	if err != nil {
		return nil, err
	}

	// Initialize systemStorage for internal KV operations.
	q.systemStorage = wal.NewDurableKVStore(configWithSystemWalDir)

	// Initialize durableStore for client-facing KV operations.
	q.durableStore = wal.NewDurableKVStore(config)

	// Increment and retrieve the current generation for versioning.
	q.generation, err = q.incrementAndGetGeneration()
	if err != nil {
		return nil, err
	}

	// Register message handlers for processing incoming messages.
	q.RegisterHandlers()

	return q, nil
}

// OnStart is invoked when the Replica starts.
// It implements the ReplicaInitializer interface to perform any startup routines.
func (q *KVStore) OnStart() {
	q.logger.Println("KVStore started.")
	// Additional startup logic can be added here if needed
}

// RegisterHandlers sets up all necessary message handlers for the KVStore.
// It implements the ReplicaInitializer interface to ensure message processing is correctly
// routed to the appropriate handler functions.
func (q *KVStore) RegisterHandlers() {
	// Register handlers for messages received from other replicas (one-way messages).
	q.HandlesMessage(common.VersionedSetValueRequest, q.handleSetValueRequest, new(messages.VersionedSetValueRequest))
	q.HandlesMessage(common.SetValueResponse, q.handleSetValueResponse, new(messages.SetValueResponse))
	q.HandlesMessage(common.VersionedGetValueRequest, q.handleGetValueRequest, new(messages.GetValueRequest))
	q.HandlesMessage(common.GetValueResponse, q.handleGetValueResponse, new(messages.GetValueResponse))

	// Register handlers for client-initiated requests that expect responses.
	q.HandlesRequestAsync(common.SetValueRequest, q.handleClientSetValueRequest, new(messages.SetValueRequest))
	q.HandlesRequestAsync(common.GetValueRequest, q.handleClientGetValueRequest, new(messages.GetValueRequest))
}

// SendHeartbeats sends periodic heartbeat messages to all replica nodes.
// It implements the HeartbeatHandler interface to maintain liveness information.
// The heartbeat mechanism is crucial for leader election and detecting failed replicas.
func (q *KVStore) SendHeartbeats() {
	q.logger.Println("Sending heartbeats to replicas.")
	// Example heartbeat message creation and sending can be implemented here.
	// heartbeatMsg := &heartbeat.Heartbeat{
	// 	Timestamp: q.clientState.GetTimestamp(),
	// }
	// q.sendOnewayMessageToReplicas(heartbeatMsg)
}

// CheckLeader verifies if the current node holds the leadership role.
// It implements the HeartbeatHandler interface to participate in leader election.
// Leader status determines if the node can perform certain privileged operations.
func (q *KVStore) CheckLeader() {
	q.logger.Println("Checking leader status.")
	// Implement leader election logic based on received heartbeats.
	// This is a placeholder for the actual implementation.
}

// handleSetValueRequest processes incoming VersionedSetValueRequest messages from peer replicas.
// It ensures that only newer versions of a key-value pair are stored to maintain consistency.
func (q *KVStore) handleSetValueRequest(message common.Message[any]) {
	setValueReq := message.MessagePayload().(*messages.VersionedSetValueRequest)

	storedValue, err := q.Get(setValueReq.Key)
	if err != nil {
		q.logger.Printf("Error retrieving key %s: %v", setValueReq.Key, err)
		// Should we return an error?
	}

	if storedValue.Timestamp < setValueReq.Version {
		q.logger.Printf("Updating key %s with newer value: %s", setValueReq.Key, setValueReq.Value)
		newStoredValue := &StoredValue{
			Key:        setValueReq.Key,
			Value:      setValueReq.Value,
			Timestamp:  setValueReq.Version,
			Generation: 1,
		}
		if err := q.Put(setValueReq.Key, newStoredValue); err != nil {
			q.logger.Printf("Error setting key %s: %v", setValueReq.Key, err)
			// Should we return an error?
		}
	} else {
		q.logger.Printf("Ignoring set for key %s: existing timestamp %d >= request version %d",
			setValueReq.Key, storedValue.Timestamp, setValueReq.Version)
	}

	// Send a one-way response back to the sender indicating the operation was processed.
	response := messages.NewSetValueResponse("Success")
	q.SendOneway(message.Header.FromAddress, response, message.Header.CorrelationId)
}

// handleSetValueResponse processes SetValueResponse messages from peer replicas.
// It delegates the response to the RequestWaitingList to handle asynchronous operations.
func (q *KVStore) handleSetValueResponse(message common.Message[any]) {
	setResp, ok := message.Payload.(*messages.SetValueResponse)
	if !ok {
		q.logger.Println("Received invalid SetValueResponse message.")
		return
	}
	// Delegate the response to the RequestWaitingList
	q.HandleResponse(message.Header.CorrelationId, setResp)
}

// handleGetValueResponse processes GetValueResponse messages from peer replicas.
// It delegates the response to the RequestWaitingList to handle asynchronous operations.
func (q *KVStore) handleGetValueResponse(message common.Message[any]) {
	getResp, ok := message.Payload.(*messages.GetValueResponse)
	if !ok {
		q.logger.Println("Received invalid GetValueResponse message.")
		return
	}
	// Delegate the response to the RequestWaitingList
	q.HandleResponse(message.Header.CorrelationId, getResp)
}

// handleClientSetValueRequest handles client-initiated SetValue requests asynchronously.
// It propagates the request to all replicas and waits for a quorum of acknowledgments.
func (q *KVStore) handleClientSetValueRequest(message common.Message[any]) (any, error) {
	clientReq, ok := message.Payload.(*messages.SetValueRequest)
	if !ok {
		return nil, errors.New("invalid SetValueRequest payload")
	}

	// Create a VersionedSetValueRequest with the current timestamp to ensure versioning.
	requestToReplicas := &messages.VersionedSetValueRequest{
		Key:           clientReq.Key,
		Value:         clientReq.Value,
		ClientID:      clientReq.ClientID,
		RequestNumber: clientReq.RequestNumber,
		Version:       q.clientState.GetTimestamp(),
	}

	// Initialize a quorum callback to track responses from replicas.
	quorumCallback := common.NewAsyncQuorumCallback[any](q.Quorum())

	// Send the VersionedSetValueRequest to all replicas.
	q.SendMessageToReplicas(quorumCallback, common.VersionedSetValueRequest, requestToReplicas)

	// Wait for a quorum of responses or timeout.
	select {
	case <-quorumCallback.GetQuorumFuture():
		return messages.NewSetValueResponse("Success"), nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("quorum set value request timed out")
	}
}

// handleClientGetValueRequest handles client-initiated GetValue requests asynchronously.
// It queries all replicas and performs read repair based on the responses received from a quorum.
func (q *KVStore) handleClientGetValueRequest(message common.Message[any]) (any, error) {
	clientReq, ok := message.Payload.(*messages.GetValueRequest)
	if !ok {
		return nil, errors.New("invalid GetValueRequest payload")
	}

	q.logger.Printf("Handling get request for key: %s in %s", clientReq.Key, q.GetName())

	requestToReplicas := &messages.GetValueRequest{Key: clientReq.Key}

	// Initialize a quorum callback to collect responses from replicas.
	quorumCallback := common.NewAsyncQuorumCallback[messages.GetValueResponse](q.Quorum())

	// Adapter to handle type conversion for the quorum callback.
	// There is probably a better way to do this, but i'm not sure what it is....
	adapter := &AnyRequestCallback[messages.GetValueResponse]{Callback: quorumCallback}

	// Send the GetValueRequest to all replicas.
	q.SendMessageToReplicas(adapter, common.VersionedGetValueRequest, requestToReplicas)

	// Wait for a quorum of responses or timeout and perform read repair if necessary.
	select {
	case quorumResult := <-quorumCallback.GetQuorumFuture():
		if quorumResult.Err != nil {
			return nil, quorumResult.Err
		}

		// Perform read repair to ensure consistency across replicas.
		readRepairer := NewReadRepairer(q.Replica, quorumResult.Responses, q.config.IsAsyncReadRepair(), q.logger)
		finalResponse := readRepairer.ReadRepair()
		return finalResponse, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("quorum get value request timed out")
	}
}

// AnyRequestCallback adapts a RequestCallback[T] to a RequestCallback[any].
// This allows handling generic response types in a type-safe manner.
type AnyRequestCallback[T any] struct {
	Callback common.RequestCallback[T]
}

// OnResponse handles the response by type asserting it to the expected type T.
// If the type assertion fails, it logs the error and invokes the OnError callback.
func (arc *AnyRequestCallback[T]) OnResponse(response any, fromNode *common.InetAddressAndPort) {
	tResponse, ok := response.(T)
	if !ok {
		// Handle type assertion failure, e.g., log and invoke OnError
		err := fmt.Errorf("type assertion failed: expected %T, got %T", arc.Callback, response)
		arc.Callback.OnError(err)
		return
	}
	arc.Callback.OnResponse(tResponse, fromNode)
}

// OnError forwards the error to the underlying RequestCallback[T].
// This ensures that error handling is consistent across different callback types.
func (arc *AnyRequestCallback[T]) OnError(err error) {
	arc.Callback.OnError(err)
}

// handleGetValueRequest processes incoming GetValueRequest messages from peer replicas.
// It retrieves the requested value and responds back to the requester.
func (q *KVStore) handleGetValueRequest(message common.Message[any]) {
	getValueReq, ok := message.MessagePayload().(*messages.GetValueRequest)
	if !ok {
		q.logger.Println("Received invalid GetValueRequest message.")
		return
	}

	storedValue, err := q.Get(getValueReq.Key)
	if err != nil {
		q.logger.Printf("Error retrieving key %s: %v", getValueReq.Key, err)
		// Respond with an empty StoredValue on error to indicate absence.
		storedValue = &Empty
	}

	q.logger.Printf("Retrieving value for key %s: %+v from %s", getValueReq.Key, storedValue, q.GetName())

	response := &messages.GetValueResponse{Value: *storedValue}
	q.SendOneway(message.Header.FromAddress, response, message.Header.CorrelationId)
}

// makeNewConfigWithSystemWalDir creates a new Config instance with a dedicated WAL directory for system storage.
// This separation ensures that system-related WAL logs do not interfere with client-facing storage.
func (q *KVStore) makeNewConfigWithSystemWalDir(config *common.Config) (*common.Config, error) {
	systemWalDir, err := q.makeSystemWalDir(config)
	if err != nil {
		return nil, err
	}
	return common.NewConfig(systemWalDir), nil
}

// makeSystemWalDir constructs the file path for the system WAL directory and ensures the directory exists.
// This directory is used for storing internal WAL logs required for system operations.
func (q *KVStore) makeSystemWalDir(config *common.Config) (string, error) {
	systemWalDir := filepath.Join(config.GetWalDir(), "_System")
	if err := os.MkdirAll(systemWalDir, os.ModePerm); err != nil {
		return "", err
	}
	return systemWalDir, nil
}

// incrementAndGetGeneration increments the generation counter in the system storage and returns the new value.
// The generation counter is used for versioning to track changes over time.
func (q *KVStore) incrementAndGetGeneration() (int, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	s := q.systemStorage.Get("generation")

	currentGeneration := q.firstGeneration
	if s != "" {
		gen, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		currentGeneration = gen + 1
	}

	q.systemStorage.Put("generation", strconv.Itoa(currentGeneration))

	return currentGeneration, nil
}

// Put stores a key-value pair in the durable store.
// The value is serialized to JSON before being persisted.
// This ensures that data is durable and can survive restarts or failures.
func (q *KVStore) Put(key string, storedValue *StoredValue) error {
	valueBytes, err := json.Marshal(storedValue)
	if err != nil {
		return err
	}
	q.durableStore.Put(key, string(valueBytes))
	return nil
}

// Get retrieves a StoredValue by key from the durable store.
// If the key does not exist, it returns an empty StoredValue.
// The value is deserialized from JSON format.
func (q *KVStore) Get(key string) (*StoredValue, error) {
	storedValueStr := q.durableStore.Get(key)

	if storedValueStr == "" {
		return &Empty, nil
	}

	var storedValue StoredValue
	if err := json.Unmarshal([]byte(storedValueStr), &storedValue); err != nil {
		return nil, err
	}

	return &storedValue, nil
}

// GetValue retrieves the value associated with the given key.
// It fetches the StoredValue and returns its Value field.
func (q *KVStore) GetValue(key string) (string, error) {
	storedValue, err := q.Get(key)
	if err != nil {
		return "", err
	}
	return storedValue.Value, nil
}

// DoAsyncReadRepair enables asynchronous read repair to maintain consistency across replicas.
// Read repair helps to fix any inconsistencies detected during read operations by updating out-of-date replicas.
func (q *KVStore) DoAsyncReadRepair() {
	q.config.SetAsyncReadRepair()
}

// SetClock updates the system clock used by the KVStore for timestamp management.
// This allows the KVStore to synchronize its operations with an external clock source if needed.
func (q *KVStore) SetClock(clock *common.SystemClock) { q.clientState.clock = clock }
