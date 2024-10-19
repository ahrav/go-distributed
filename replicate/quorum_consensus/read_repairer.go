package quorumconsensus

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/ahrav/go-distributed/replicate/base"
	"github.com/ahrav/go-distributed/replicate/common"
	"github.com/ahrav/go-distributed/replicate/quorum_consensus/messages"
)

// ReadRepairer handles read repair operations in the quorum system.
// It identifies replicas with stale data and repairs them by sending updated values.
type ReadRepairer struct {
	logger        *log.Logger                                              // Logger for logging events.
	replica       *base.Replica                                            // The replica instance performing the read repair.
	nodesToValues map[*common.InetAddressAndPort]messages.GetValueResponse // Map of nodes to their GetValueResponse.
	requestId     int32                                                    // Unique request ID for the read repair operation.
}

// NewReadRepairer initializes a new ReadRepairer.
// Parameters:
// - replica: The replica instance performing the read repair.
// - nodesToValues: Map of nodes to their GetValueResponse.
// - logger: Logger for logging events.
// Returns: A new instance of ReadRepairer.
func NewReadRepairer(
	replica *base.Replica,
	nodesToValues map[*common.InetAddressAndPort]messages.GetValueResponse,
	logger *log.Logger,
) *ReadRepairer {
	return &ReadRepairer{
		logger:        logger,
		replica:       replica,
		nodesToValues: nodesToValues,
		requestId:     0,
	}
}

// ReadRepair initiates the read repair process.
// It returns a channel that will receive the final StoredValue once repair is complete.
// Returns: A channel that will receive the final StoredValue.
func (rr *ReadRepairer) ReadRepair() <-chan StoredValue {
	resultChan := make(chan StoredValue, 1)

	go func() {
		defer close(resultChan)

		latestStoredValue := rr.getLatestStoredValue()
		repairedValue := rr.readRepair(latestStoredValue)

		resultChan <- repairedValue
	}()

	return resultChan
}

// readRepair performs the read repair logic and returns the repaired StoredValue.
func (rr *ReadRepairer) readRepair(latestStoredValue StoredValue) StoredValue {
	nodesHavingStaleValues := rr.getNodesHavingStaleValues(latestStoredValue.Version)
	if len(nodesHavingStaleValues) == 0 {
		return latestStoredValue
	}

	writeRequest := rr.createSetValueRequest(latestStoredValue.Key, latestStoredValue.Value, latestStoredValue.Version)
	requestCallback := common.NewAsyncQuorumCallback[messages.GetValueResponse](len(nodesHavingStaleValues))

	// Adapter to handle type conversion for the quorum callback.
	// There is probably a better way to do this, but i'm not sure what it is....
	adapter := &AnyRequestCallback[messages.GetValueResponse]{Callback: requestCallback}

	for _, node := range nodesHavingStaleValues {
		rr.logger.Printf("Sending read repair request to %v: %s", node, latestStoredValue.Value)
		rr.replica.SendMessageToReplica(adapter, node, writeRequest)
	}

	// Wait for quorum or all responses.
	_ = <-requestCallback.GetQuorumFuture()

	// For simplicity, we're returning the latestStoredValue.
	return latestStoredValue
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

// createSetValueRequest constructs a RequestOrResponse message to set a value with versioning.
// Parameters:
// - key: The key to set.
// - value: The value to set.
// - timestamp: The timestamp for versioning.
// Returns: A RequestOrResponse message to set a value with versioning.
func (rr *ReadRepairer) createSetValueRequest(key, value string, timestamp common.MonotonicID) common.RequestOrResponse {
	setValueRequest := messages.VersionedSetValueRequest{
		Key:     key,
		Value:   value,
		Version: timestamp,
	}

	serializedRequest, err := json.Marshal(setValueRequest)
	if err != nil {
		rr.logger.Printf("Error serializing SetValueRequest: %v", err)
		// Handle error appropriately. For now, returning an empty RequestOrResponse
		return common.RequestOrResponse{}
	}

	reqID := atomic.AddInt32(&rr.requestId, 1)
	rID := int(reqID)

	return common.RequestOrResponse{
		// MessageID:       common.VersionedSetValueRequest,
		MessageBodyJSON: serializedRequest,
		RequestID:       &rID,
		FromAddress:     rr.replica.GetPeerConnectionAddress(),
	}
}

// getNodesHavingStaleValues identifies nodes with stale values based on the latest version.
// Parameters:
// - latestVersion: The latest version to compare against.
// Returns: A slice of nodes with stale values.
func (rr *ReadRepairer) getNodesHavingStaleValues(latestVersion common.MonotonicID) []*common.InetAddressAndPort {
	var staleNodes []*common.InetAddressAndPort
	for node, response := range rr.nodesToValues {
		if latestVersion.IsAfter(response.Value.Version) {
			staleNodes = append(staleNodes, node)
		}
	}
	return staleNodes
}

// getLatestStoredValue retrieves the most recent StoredValue from all nodes.
// If no values are present, it returns an empty StoredValue.
// Returns: The most recent StoredValue.
func (rr *ReadRepairer) getLatestStoredValue() StoredValue {
	var latest = EmptyStoredValue
	for _, response := range rr.nodesToValues {
		if response.Value.Version.IsAfter(latest.Version) {
			latest = response.Value
		}
	}
	return latest
}
