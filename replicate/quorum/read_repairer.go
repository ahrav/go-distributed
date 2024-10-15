package quorum

import (
	"log"
	"math/rand/v2"

	"github.com/ahrav/go-distributed/replicate/base"
	"github.com/ahrav/go-distributed/replicate/common"
	"github.com/ahrav/go-distributed/replicate/quorum/messages"
	"github.com/ahrav/go-distributed/replicate/vsr"
)

// ReadRepairer handles read repair operations in the quorum system.
// It identifies replicas with stale data and repairs them by sending updated values.
type ReadRepairer struct {
	logger        *log.Logger                                              // Logger for logging events.
	replica       *base.Replica                                            // The replica instance performing the read repair.
	nodesToValues map[*common.InetAddressAndPort]messages.GetValueResponse // Map of nodes to their GetValueResponse.
	isAsyncRepair bool                                                     // Flag indicating if the repair is asynchronous.
	requestId     int                                                      // Unique request ID for the read repair operation.
}

// NewReadRepairer initializes a new ReadRepairer.
// Parameters:
// - replica: The replica instance performing the read repair.
// - nodesToValues: Map of nodes to their GetValueResponse.
// - isAsyncRepair: Flag indicating if the repair is asynchronous.
// - logger: Logger for logging events.
// Returns: A new instance of ReadRepairer.
func NewReadRepairer(
	replica *base.Replica,
	nodesToValues map[*common.InetAddressAndPort]messages.GetValueResponse,
	isAsyncRepair bool,
	logger *log.Logger,
) *ReadRepairer {
	return &ReadRepairer{
		logger:        logger,
		replica:       replica,
		nodesToValues: nodesToValues,
		isAsyncRepair: isAsyncRepair,
		requestId:     rand.Int(),
	}
}

// ReadRepair initiates the read repair process.
// It returns a channel that will receive the final GetValueResponse once repair is complete.
// Returns: A channel that will receive the final GetValueResponse.
func (rr *ReadRepairer) ReadRepair() <-chan common.CompletionResult[messages.GetValueResponse] {
	resultChan := make(chan common.CompletionResult[messages.GetValueResponse], 1)

	go func() {
		defer close(resultChan)

		latestStoredValue := rr.getLatestStoredValue()
		repairedValue, err := rr.readRepair(latestStoredValue)

		if err != nil {
			resultChan <- common.CompletionResult[messages.GetValueResponse]{Err: err}
			return
		}

		getValueResponse := messages.GetValueResponse{
			Value: repairedValue,
		}
		resultChan <- common.CompletionResult[messages.GetValueResponse]{Value: getValueResponse}
	}()

	return resultChan
}

// readRepair performs the read repair logic and returns the repaired StoredValue or an error.
// Parameters:
// - latestStoredValue: The most recent StoredValue.
// Returns: The repaired StoredValue or an error.
func (rr *ReadRepairer) readRepair(latestStoredValue StoredValue) (StoredValue, error) {
	nodesHavingStaleValues := rr.getNodesHavingStaleValues(latestStoredValue.Timestamp)
	if len(nodesHavingStaleValues) == 0 {
		return latestStoredValue, nil
	}

	var responseFutures []<-chan common.CompletionResult[common.RequestOrResponse]
	writeRequest := rr.createSetValueRequest(latestStoredValue.Key, latestStoredValue.Value, latestStoredValue.Timestamp)

	for _, node := range nodesHavingStaleValues {
		completionCallback := vsr.NewCompletionCallback[common.RequestOrResponse]()
		rr.logger.Printf("Sending read repair request to %v: %s", node, latestStoredValue.Value)
		responseFutures = append(responseFutures, completionCallback.GetFuture())
		rr.replica.SendMessageToReplica(completionCallback, node, writeRequest)
	}

	if rr.isAsyncRepair {
		// Asynchronous repair: Do not wait for repair responses.
		return latestStoredValue, nil
	}

	// Synchronous repair: Wait for all repair operations to complete.
	allResults := <-common.Sequence(responseFutures)

	// Optionally, handle individual results or aggregate errors here.
	for _, res := range allResults {
		if res.Err != nil {
			rr.logger.Printf("Read repair failed: %v", res.Err)
			// Decide whether to continue or return an error.
			// For simplicity, we'll continue.
		} else {
			rr.logger.Printf("Read repair succeeded: %v", res.Value)
		}
	}

	return latestStoredValue, nil
}

// createSetValueRequest constructs a RequestOrResponse message to set a value with versioning.
// Parameters:
// - key: The key to set.
// - value: The value to set.
// - timestamp: The timestamp for versioning.
// Returns: A RequestOrResponse message to set a value with versioning.
func (rr *ReadRepairer) createSetValueRequest(key, value string, timestamp int64) common.RequestOrResponse {
	setValueRequest := messages.VersionedSetValueRequest{
		Key:           key,
		Value:         value,
		ClientID:      -1,
		RequestNumber: -1,
		Version:       timestamp,
	}
	serializedRequest, _ := common.Serialize(setValueRequest)

	requestID := rr.requestId + 1
	versionID := common.VersionedSetValueRequest.GetId()

	requestOrResponse := common.RequestOrResponse{
		RequestID:       &versionID,
		MessageBodyJSON: serializedRequest,
		CorrelationID:   &requestID,
		FromAddress:     rr.replica.GetPeerConnectionAddress(),
	}

	rr.requestId++

	return requestOrResponse
}

// getNodesHavingStaleValues identifies nodes with stale values based on the latest timestamp.
// Parameters:
// - latestTimestamp: The latest timestamp to compare against.
// Returns: A slice of nodes with stale values.
func (rr *ReadRepairer) getNodesHavingStaleValues(latestTimestamp int64) []*common.InetAddressAndPort {
	var staleNodes []*common.InetAddressAndPort
	for node, response := range rr.nodesToValues {
		if latestTimestamp > response.Value.Timestamp {
			staleNodes = append(staleNodes, node)
		}
	}
	return staleNodes
}

// getLatestStoredValue retrieves the most recent StoredValue from all nodes.
// If no values are present, it returns an empty StoredValue.
// Returns: The most recent StoredValue.
func (rr *ReadRepairer) getLatestStoredValue() StoredValue {
	var latest = Empty
	for _, response := range rr.nodesToValues {
		if response.Value.Timestamp > latest.Timestamp {
			latest = response.Value
		}
	}
	return latest
}
