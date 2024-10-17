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

// KVStore represents a quorum-based key-value store.
// It embeds the base Replica and implements the ReplicaInitializer and HeartbeatHandler interfaces.
type KVStore struct {
	*base.Replica

	logger          *log.Logger
	firstGeneration int
	config          *common.Config
	generation      int
	replicas        []*common.InetAddressAndPort
	clientState     *ClientState

	systemStorage *wal.DurableKVStore
	durableStore  *wal.DurableKVStore

	mutex sync.Mutex
}

// NewQuorumKVStore creates a new instance of KVStore.
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

	// Create the initializer and heartbeat handler.
	initializer := q
	heartbeatHandler := q

	// Initialize the base Replica.
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

	// Embed the Replica into KVStore.
	q.Replica = replica

	configWithSystemWalDir, err := q.makeNewConfigWithSystemWalDir(config)
	if err != nil {
		return nil, err
	}

	q.systemStorage = wal.NewDurableKVStore(configWithSystemWalDir)

	q.durableStore = wal.NewDurableKVStore(config)

	// Initialize generation.
	q.generation, err = q.incrementAndGetGeneration()
	if err != nil {
		return nil, err
	}

	// Register message handlers.
	q.RegisterHandlers()

	return q, nil
}

// OnStart is called when the Replica starts.
// Implements the ReplicaInitializer interface.
func (q *KVStore) OnStart() {
	q.logger.Println("KVStore started.")
	// Additional startup logic can be added here if needed
}

// RegisterHandlers registers all necessary message handlers.
// Implements the ReplicaInitializer interface.
func (q *KVStore) RegisterHandlers() {
	// Messages handled by replicas (one-way messages)
	q.HandlesMessage(common.VersionedSetValueRequest, q.handleSetValueRequest, &messages.VersionedSetValueRequest{})
	q.HandlesMessage(common.SetValueResponse, q.handleSetValueResponse, &messages.SetValueResponse{})
	q.HandlesMessage(common.VersionedGetValueRequest, q.handleGetValueRequest, &messages.GetValueRequest{})
	q.HandlesMessage(common.GetValueResponse, q.handleGetValueResponse, &messages.GetValueResponse{})

	// Client requests (expecting responses)
	q.HandlesRequestAsync(common.SetValueRequest, q.handleClientSetValueRequest, &messages.SetValueRequest{})
	q.HandlesRequestAsync(common.GetValueRequest, q.handleClientGetValueRequest, &messages.GetValueRequest{})
}

// SendHeartbeats sends heartbeat messages to all replicas.
// Implements the HeartbeatHandler interface.
func (q *KVStore) SendHeartbeats() {
	q.logger.Println("Sending heartbeats to replicas.")
	// heartbeatMsg := &heartbeat.Heartbeat{
	// 	Timestamp: q.clientState.GetTimestamp(),
	// }
	//
	// q.sendOnewayMessageToReplicas(heartbeatMsg)
}

// CheckLeader checks if the current node is the leader.
// Implements the HeartbeatHandler interface.
func (q *KVStore) CheckLeader() {
	q.logger.Println("Checking leader status.")
	// Leader election logic can be implemented here based on heartbeats
	// For now, it's a placeholder
}

// handleSetValueRequest processes VersionedSetValueRequest messages from peers.
func (q *KVStore) handleSetValueRequest(message common.Message[any]) {
	setValueReq := message.MessagePayload().(*messages.VersionedSetValueRequest)

	storedValue, err := q.Get(setValueReq.Key)
	if err != nil {
		q.logger.Printf("Error retrieving key %s: %v", setValueReq.Key, err)
		// Optionally, you can decide to respond with an error message
	}

	if storedValue.Timestamp < setValueReq.Version {
		q.logger.Printf("Setting newer value for key %s: %s", setValueReq.Key, setValueReq.Value)
		newStoredValue := &StoredValue{
			Key:        setValueReq.Key,
			Value:      setValueReq.Value,
			Timestamp:  setValueReq.Version,
			Generation: 1,
		}
		if err := q.Put(setValueReq.Key, newStoredValue); err != nil {
			q.logger.Printf("Error setting key %s: %v", setValueReq.Key, err)
			// Optionally, handle the error as needed
		}
	} else {
		q.logger.Printf("Not setting key %s: existing timestamp %d >= request version %d",
			setValueReq.Key, storedValue.Timestamp, setValueReq.Version)
	}

	// Send a one-way response back to the sender.
	response := messages.NewSetValueResponse("Success")
	q.SendOneway(message.Header.FromAddress, response, message.Header.CorrelationId)
}

// handleSetValueResponse processes SetValueResponse messages from peers.
func (q *KVStore) handleSetValueResponse(message common.Message[any]) {
	setResp, ok := message.Payload.(*messages.SetValueResponse)
	if !ok {
		q.logger.Println("Invalid SetValueResponse message.")
		return
	}
	// Delegate the response to the RequestWaitingList
	q.HandleResponse(message.Header.CorrelationId, setResp)
}

// handleGetValueResponse processes GetValueResponse messages from peers.
func (q *KVStore) handleGetValueResponse(message common.Message[any]) {
	getResp, ok := message.Payload.(*messages.GetValueResponse)
	if !ok {
		q.logger.Println("Invalid GetValueResponse message.")
		return
	}
	// Delegate the response to the RequestWaitingList
	q.HandleResponse(message.Header.CorrelationId, getResp)
}

// handleClientSetValueRequest handles client SetValue requests asynchronously.
func (q *KVStore) handleClientSetValueRequest(message common.Message[any]) (any, error) {
	clientReq, ok := message.Payload.(*messages.SetValueRequest)
	if !ok {
		return nil, errors.New("invalid SetValueRequest payload")
	}

	// Create VersionedSetValueRequest with timestamp
	requestToReplicas := &messages.VersionedSetValueRequest{
		Key:           clientReq.Key,
		Value:         clientReq.Value,
		ClientID:      clientReq.ClientID,
		RequestNumber: clientReq.RequestNumber,
		Version:       q.clientState.GetTimestamp(),
	}

	// Initialize quorum callback.
	quorumCallback := common.NewAsyncQuorumCallback[any](q.Quorum())

	// Send message to replicas.
	q.SendMessageToReplicas(quorumCallback, common.VersionedSetValueRequest, requestToReplicas)

	// Wait for quorum or timeout.
	select {
	case <-quorumCallback.GetQuorumFuture():
		return messages.NewSetValueResponse("Success"), nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("quorum set value request timed out")
	}
}

// handleClientGetValueRequest handles client GetValue requests asynchronously.
func (q *KVStore) handleClientGetValueRequest(message common.Message[any]) (any, error) {
	clientReq, ok := message.Payload.(*messages.GetValueRequest)
	if !ok {
		return nil, errors.New("invalid GetValueRequest payload")
	}

	q.logger.Printf("Handling get request for key: %s in %s", clientReq.Key, q.GetName())

	requestToReplicas := &messages.GetValueRequest{Key: clientReq.Key}

	// Initialize quorum callback.
	quorumCallback := common.NewAsyncQuorumCallback[messages.GetValueResponse](q.Quorum())
	// Temporary hack.
	adapter := &AnyRequestCallback[messages.GetValueResponse]{Callback: quorumCallback}

	// Send message to replicas.
	q.SendMessageToReplicas(adapter, common.VersionedGetValueRequest, requestToReplicas)

	// Wait for quorum or timeout and perform read repair.
	select {
	case quorumResult := <-quorumCallback.GetQuorumFuture():
		if quorumResult.Err != nil {
			return nil, quorumResult.Err
		}

		readRepairer := NewReadRepairer(q.Replica, quorumResult.Responses, q.config.IsAsyncReadRepair(), q.logger)
		finalResponse := readRepairer.ReadRepair()
		return finalResponse, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("quorum get value request timed out")
	}
}

// AnyRequestCallback is an adapter that converts RequestCallback[T] to RequestCallback[any]
type AnyRequestCallback[T any] struct {
	Callback common.RequestCallback[T]
}

// OnResponse implements RequestCallback[any]. It type asserts the response to T.
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

// OnError implements RequestCallback[any]. It directly forwards the error.
func (arc *AnyRequestCallback[T]) OnError(err error) {
	arc.Callback.OnError(err)
}

// handleGetValueRequest processes GetValueRequest messages from peers.
func (q *KVStore) handleGetValueRequest(message common.Message[any]) {
	getValueReq, ok := message.MessagePayload().(*messages.GetValueRequest)
	if !ok {
		q.logger.Println("Invalid VersionedGetValueRequest message.")
		return
	}

	storedValue, err := q.Get(getValueReq.Key)
	if err != nil {
		q.logger.Printf("Error retrieving key %s: %v", getValueReq.Key, err)
		// Respond with empty StoredValue on error
		storedValue = &Empty
	}

	q.logger.Printf("Getting value for key %s: %+v from %s", getValueReq.Key, storedValue, q.GetName())

	response := &messages.GetValueResponse{Value: *storedValue}
	q.SendOneway(message.Header.FromAddress, response, message.Header.CorrelationId)
}

// makeNewConfigWithSystemWalDir creates a new Config with the system WAL directory.
func (q *KVStore) makeNewConfigWithSystemWalDir(config *common.Config) (*common.Config, error) {
	systemWalDir, err := q.makeSystemWalDir(config)
	if err != nil {
		return nil, err
	}
	return common.NewConfig(systemWalDir), nil
}

// makeSystemWalDir constructs the system WAL directory path and creates the directory.
func (q *KVStore) makeSystemWalDir(config *common.Config) (string, error) {
	systemWalDir := filepath.Join(config.GetWalDir(), "_System")
	if err := os.MkdirAll(systemWalDir, os.ModePerm); err != nil {
		return "", err
	}
	return systemWalDir, nil
}

// incrementAndGetGeneration increments the generation counter and returns the new value.
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
func (q *KVStore) Put(key string, storedValue *StoredValue) error {
	valueBytes, err := json.Marshal(storedValue)
	if err != nil {
		return err
	}
	q.durableStore.Put(key, string(valueBytes))
	return nil
}

// Get retrieves a StoredValue by key from the durable store.
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

// GetValue retrieves the value for a given key.
func (q *KVStore) GetValue(key string) (string, error) {
	storedValue, err := q.Get(key)
	if err != nil {
		return "", err
	}
	return storedValue.Value, nil
}

// DoAsyncReadRepair enables asynchronous read repair.
func (q *KVStore) DoAsyncReadRepair() {
	q.config.SetAsyncReadRepair()
}

// SetClock updates the system clock.
func (q *KVStore) SetClock(clock *common.SystemClock) { q.clientState.clock = clock }
