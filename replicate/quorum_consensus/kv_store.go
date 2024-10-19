package quorumconsensus

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ahrav/go-distributed/replicate/base"
	"github.com/ahrav/go-distributed/replicate/common"
	"github.com/ahrav/go-distributed/replicate/quorum_consensus/messages"
	"github.com/ahrav/go-distributed/replicate/wal"
)

// KVStore implements a quorum-based consensus algorithm for KV storage.
type KVStore struct {
	*base.Replica

	logger        *log.Logger
	config        *common.Config
	replicas      []*common.InetAddressAndPort
	durableStore  *wal.DurableKVStore
	mutex         sync.Mutex
	generation    int
	systemStorage *wal.DurableKVStore
}

// NewKVStore initializes a new KVStore instance.
// Parameters:
// - name: The name of the KVStore instance.
// - config: Configuration for the KVStore.
// - clock: System clock instance.
// - clientConnectionAddress: Address for client connections.
// - peerConnectionAddress: Address for peer connections.
// - replicas: List of replica addresses.
// Returns: A new instance of KVStore or an error if initialization fails.
func NewKVStore(
	name string,
	config *common.Config,
	clock *common.SystemClock,
	clientConnectionAddress *common.InetAddressAndPort,
	peerConnectionAddress *common.InetAddressAndPort,
	replicas []*common.InetAddressAndPort,
) (*KVStore, error) {
	logger := log.New(os.Stdout, "KVStore: ", log.LstdFlags)

	qc := &KVStore{
		logger:   logger,
		config:   config,
		replicas: replicas,
	}

	// Assign KVStore as both the initializer and heartbeat handler
	// to integrate with the base Replica's lifecycle.
	initializer := qc
	heartbeatHandler := qc

	// Initialize the embedded Replica.
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
	qc.Replica = replica

	// Initialize durableStore.
	qc.durableStore = wal.NewDurableKVStore(config)

	// TODO: Initialize systemStorage.
	// Initialize systemStorage
	// qc.systemStorage = wal.NewDurableKVStore(systemConfig)

	// Register message handlers.
	qc.RegisterHandlers()

	return qc, nil
}

// OnStart is invoked when the Replica starts.
// It implements the ReplicaInitializer interface to perform any startup routines.
func (s *KVStore) OnStart() {
	s.logger.Println("KVStore started.")
	// Additional startup logic can be added here if needed
}

// RegisterHandlers sets up all necessary message handlers for the KVStore.
func (s *KVStore) RegisterHandlers() {
	// Register handlers for messages received from other replicas (one-way messages).
	s.HandlesMessage(common.GetVersion, s.handleGetVersionRequest, new(messages.GetVersionRequest))
	s.HandlesMessage(common.GetVersionResponse, s.handleGetVersionResponse, new(messages.GetVersionResponse))

	// These handlers are responsible for persisting data to durable storage.
	s.HandlesMessage(common.VersionedSetValueRequest, s.handlePeerSetValueRequest, new(messages.VersionedSetValueRequest))
	s.HandlesMessage(common.SetValueResponse, s.handleSetValueResponse, new(messages.SetValueResponse))

	s.HandlesMessage(common.VersionedGetValueRequest, s.handleGetValueRequest, new(messages.GetValueRequest))
	s.HandlesMessage(common.GetValueResponse, s.handleGetValueResponse, new(messages.GetValueResponse))

	// Register handlers for client-initiated requests that expect responses.
	s.HandlesRequestAsync(common.SetValueRequest, s.handleClientSetValueRequest, new(messages.SetValueRequest))
	s.HandlesRequestAsync(common.GetValueRequest, s.handleClientGetValueRequest, new(messages.GetValueRequest))
}

// SendHeartbeats sends periodic heartbeat messages to all replica nodes.
// It implements the HeartbeatHandler interface to maintain liveness information.
// The heartbeat mechanism is crucial for leader election and detecting failed replicas.
func (s *KVStore) SendHeartbeats() {
	s.logger.Println("Sending heartbeats to replicas.")
	// Example heartbeat message creation and sending can be implemented here.
	// heartbeatMsg := &heartbeat.Heartbeat{
	// 	Timestamp: q.clientState.GetTimestamp(),
	// }
	// q.sendOnewayMessageToReplicas(heartbeatMsg)
}

// CheckLeader verifies if the current node holds the leadership role.
// It implements the HeartbeatHandler interface to participate in leader election.
// Leader status determines if the node can perform certain privileged operations.
func (s *KVStore) CheckLeader() {
	s.logger.Println("Checking leader status.")
	// Implement leader election logic based on received heartbeats.
}

// Put stores a key-value pair in the durable store.
// Parameters:
// - key: The key to store.
// - storedValue: The value to store.
// Returns: An error if storing fails.
func (s *KVStore) Put(key string, storedValue *StoredValue) error {
	valueBytes, err := json.Marshal(storedValue)
	if err != nil {
		return err
	}
	s.durableStore.Put(key, string(valueBytes))
	return nil
}

// Get retrieves a StoredValue by key from the durable store.
// Parameters:
// - key: The key to retrieve.
// Returns: The stored value or an error if retrieval fails.
func (s *KVStore) Get(key string) (*StoredValue, error) {
	storedValueStr := s.durableStore.Get(key)
	if storedValueStr == "" {
		return &EmptyStoredValue, nil
	}

	var storedValue StoredValue
	if err := json.Unmarshal([]byte(storedValueStr), &storedValue); err != nil {
		return nil, err
	}

	return &storedValue, nil
}

// GetVersion retrieves the version of a given key.
// Parameters:
// - key: The key to retrieve the version for.
// Returns: The version or an error if retrieval fails.
func (s *KVStore) GetVersion(key string) (common.MonotonicID, error) {
	storedValue, err := s.Get(key)
	if err != nil {
		return common.MonotonicID{}, err
	}
	if storedValue == nil {
		return EmptyStoredValue.Version, nil
	}
	return storedValue.Version, nil
}

// handleGetVersionRequest processes incoming GetVersionRequest messages.
// Parameters:
// - message: The incoming message containing the GetVersionRequest.
func (s *KVStore) handleGetVersionRequest(message common.Message[any]) {
	getVersionReq, ok := message.Payload.(*messages.GetVersionRequest)
	if !ok {
		s.logger.Println("Invalid GetVersionRequest payload")
		return
	}

	storedValue, err := s.Get(getVersionReq.Key)
	var version common.MonotonicID
	if err != nil || storedValue == nil {
		version = common.EmptyMonotonicId()
	} else {
		version = storedValue.Version
	}

	response := &messages.GetVersionResponse{Version: version}
	s.SendOneway(message.GetFromAddress(), response, message.GetCorrelationId())
}

// handleGetVersionResponse processes GetVersionResponse messages.
// Parameters:
// - message: The incoming message containing the GetVersionResponse.
func (s *KVStore) handleGetVersionResponse(message common.Message[any]) {
	// Implementation depends on how responses are handled.
	// Typically, delegate to a callback or future.
	s.HandleResponse(message.GetCorrelationId(), message.Payload)
}

// handlePeerSetValueRequest processes incoming VersionedSetValueRequest messages from peers.
// Parameters:
// - message: The incoming message containing the VersionedSetValueRequest.
func (s *KVStore) handlePeerSetValueRequest(message common.Message[any]) {
	setValueReq, ok := message.Payload.(*messages.VersionedSetValueRequest)
	if !ok {
		s.logger.Println("Invalid VersionedSetValueRequest payload")
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	storedValue, err := s.Get(setValueReq.Key)
	if err != nil {
		s.logger.Printf("Error retrieving key %s: %v", setValueReq.Key, err)
		// Optionally handle the error
	}

	// Compare versions.
	if setValueReq.Version.IsAfter(storedValue.Version) {
		newStoredValue := &StoredValue{
			Key:     setValueReq.Key,
			Value:   setValueReq.Value,
			Version: setValueReq.Version,
		}
		if err := s.Put(setValueReq.Key, newStoredValue); err != nil {
			s.logger.Printf("Error setting key %s: %v", setValueReq.Key, err)
		} else {
			s.logger.Printf("Updated key %s with newer version.", setValueReq.Key)
		}
	} else {
		s.logger.Printf("Ignored set for key %s: existing version >= request version", setValueReq.Key)
	}

	// Send a one-way response back to the sender.
	response := &messages.SetValueResponse{Result: "Success"}
	s.SendOneway(message.GetFromAddress(), response, message.GetCorrelationId())
}

// handleSetValueResponse processes SetValueResponse messages.
// Parameters:
// - message: The incoming message containing the SetValueResponse.
func (s *KVStore) handleSetValueResponse(message common.Message[any]) {
	setResp, ok := message.Payload.(*messages.SetValueResponse)
	if !ok {
		s.logger.Println("Invalid SetValueResponse payload")
		return
	}
	// Delegate the response to the RequestWaitingList or appropriate handler
	s.HandleResponse(message.GetCorrelationId(), setResp)
}

// handleGetValueRequest processes incoming GetValueRequest messages.
// Parameters:
// - message: The incoming message containing the GetValueRequest.
func (s *KVStore) handleGetValueRequest(message common.Message[any]) {
	getValueReq, ok := message.Payload.(*messages.GetValueRequest)
	if !ok {
		s.logger.Println("Invalid GetValueRequest payload")
		return
	}

	storedValue, err := s.Get(getValueReq.Key)
	if err != nil {
		s.logger.Printf("Error retrieving key %s: %v", getValueReq.Key, err)
		// Respond with an empty StoredValue on error.
		storedValue = &EmptyStoredValue
	}

	response := &messages.GetValueResponse{Value: *storedValue}
	s.SendOneway(message.GetFromAddress(), response, message.GetCorrelationId())
}

// handleGetValueResponse processes GetValueResponse messages.
// Parameters:
// - message: The incoming message containing the GetValueResponse.
func (s *KVStore) handleGetValueResponse(message common.Message[any]) {
	getResp, ok := message.Payload.(*messages.GetValueResponse)
	if !ok {
		s.logger.Println("Invalid GetValueResponse payload")
		return
	}
	// Delegate the response to the RequestWaitingList or appropriate handler.
	s.HandleResponse(message.GetCorrelationId(), getResp)
}

// handleClientSetValueRequest handles client-initiated SetValue requests asynchronously.
// Parameters:
// - message: The incoming message containing the SetValueRequest.
// Returns: A response or an error if the request fails.
func (s *KVStore) handleClientSetValueRequest(message common.Message[any]) (any, error) {
	clientSetValueReq, ok := message.Payload.(*messages.SetValueRequest)
	if !ok {
		return nil, errors.New("invalid SetValueRequest payload")
	}

	// Create a GetVersionRequest to retrieve current version.
	getVersionReq := &messages.GetVersionRequest{Key: clientSetValueReq.Key}
	versionCallback := common.NewAsyncQuorumCallback[any](s.Quorum())

	// Send GetVersionRequest to all replicas.
	s.SendMessageToReplicas(versionCallback, common.GetVersion, getVersionReq)

	// Wait for quorum of GetVersionResponses.
	versions := <-versionCallback.GetQuorumFuture()

	// Determine the next version.
	existingVersions := extractVersions(versions.Responses)
	nextVersion := s.getNextId(existingVersions)

	// Create VersionedSetValueRequest with the new version.
	versionedSetValueReq := &messages.VersionedSetValueRequest{
		Key:     clientSetValueReq.Key,
		Value:   clientSetValueReq.Value,
		Version: nextVersion,
	}

	// Initialize quorum callback for SetValueResponses.
	setValueCallback := common.NewAsyncQuorumCallback[any](s.Quorum())

	// Send VersionedSetValueRequest to all replicas.
	s.SendMessageToReplicas(setValueCallback, common.VersionedSetValueRequest, versionedSetValueReq)

	// Wait for quorum of SetValueResponses.
	setResponses := <-setValueCallback.GetQuorumFuture()

	// For simplicity, return the first successful response.
	for _, resp := range setResponses.Responses {
		if setResp, ok := resp.(*messages.SetValueResponse); ok {
			return setResp, nil
		}
	}

	return nil, errors.New("no successful SetValueResponse received")
}

// handleClientGetValueRequest handles client-initiated GetValue requests asynchronously.
// Parameters:
// - message: The incoming message containing the GetValueRequest.
// Returns: A response or an error if the request fails.
func (s *KVStore) handleClientGetValueRequest(message common.Message[any]) (any, error) {
	clientGetValueReq, ok := message.Payload.(*messages.GetValueRequest)
	if !ok {
		return nil, errors.New("invalid GetValueRequest payload")
	}

	s.logger.Printf("Handling get request for key: %s", clientGetValueReq.Key)

	getValueReq := &messages.GetValueRequest{Key: clientGetValueReq.Key}
	getValueCallback := common.NewAsyncQuorumCallback[messages.GetValueResponse](s.Quorum())

	// Adapter to handle type conversion for the quorum callback.
	// There is probably a better way to do this, but i'm not sure what it is....
	adapter := &AnyRequestCallback[messages.GetValueResponse]{Callback: getValueCallback}

	// Send GetValueRequest to all replicas
	s.SendMessageToReplicas(adapter, common.VersionedGetValueRequest, getValueReq)

	// Wait for a quorum of responses or timeout and perform read repair if necessary.
	select {
	case quorumResult := <-getValueCallback.GetQuorumFuture():
		if quorumResult.Err != nil {
			return nil, quorumResult.Err
		}

		// Perform read repair to ensure consistency across replicas.
		readRepairer := NewReadRepairer(s.Replica, quorumResult.Responses, s.logger)
		finalResponse := readRepairer.ReadRepair()
		return finalResponse, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("quorum get value request timed out")
	}
}

// Helper function to extract versions from GetVersionResponses
// Parameters:
// - responses: Map of responses from replicas.
// Returns: A slice of MonotonicID representing the versions.
func extractVersions(responses map[*common.InetAddressAndPort]any) []common.MonotonicID {
	var versions []common.MonotonicID
	for _, resp := range responses {
		if getVersionResp, ok := resp.(*messages.GetVersionResponse); ok {
			versions = append(versions, getVersionResp.Version)
		}
	}
	return versions
}

// getNextId generates the next MonotonicId based on existing versions.
// Parameters:
// - ids: Slice of existing MonotonicID.
// Returns: The next MonotonicID.
func (s *KVStore) getNextId(ids []common.MonotonicID) common.MonotonicID {
	max := s.getMax(ids)
	if max.IsEmpty() {
		return common.NewMonotonicId(1, int32(s.GetServerID()))
	}
	// Manually increment the RequestID
	newRequestID := max.RequestID + 1
	return common.NewMonotonicId(newRequestID, int32(s.GetServerID()))
}

// getMax finds the maximum MonotonicId from a list.
// Parameters:
// - ids: Slice of MonotonicID.
// Returns: The maximum MonotonicID.
func (s *KVStore) getMax(ids []common.MonotonicID) common.MonotonicID {
	mx := common.EmptyMonotonicId()
	for _, id := range ids {
		if id.IsAfter(mx) {
			mx = id
		}
	}
	return mx
}
