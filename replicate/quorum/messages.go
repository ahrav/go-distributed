package quorum

import (
	"github.com/ahrav/go-distributed/replicate/common"
)

// GetValueRequest represents a request to get the value associated with a key.
type GetValueRequest struct {
	common.MessagePayload
	Key string `json:"key"`
}

// NewGetValueRequest creates a new GetValueRequest with the specified key.
func NewGetValueRequest(key string) *GetValueRequest {
	return &GetValueRequest{
		MessagePayload: common.NewMessagePayload(common.GetValueRequest),
		Key:            key,
	}
}

// GetValueResponse represents the response containing the requested value.
type GetValueResponse struct {
	common.MessagePayload
	Value StoredValue `json:"value"`
}

// NewGetValueResponse creates a new GetValueResponse with the specified value.
func NewGetValueResponse(value StoredValue) *GetValueResponse {
	return &GetValueResponse{
		MessagePayload: common.NewMessagePayload(common.GetValueResponse),
		Value:          value,
	}
}

// SetValueRequest represents a request to set a value for a key.
type SetValueRequest struct {
	common.MessagePayload
	ClientID      int64  `json:"clientId"`
	RequestNumber int    `json:"requestNumber"`
	Key           string `json:"key"`
	Value         string `json:"value"`
	Timestamp     int64  `json:"timestamp"`
}

// NewSetValueRequest creates a new SetValueRequest with all fields.
func NewSetValueRequest(key, value string, clientID int64, requestNumber int, timestamp int64) *SetValueRequest {
	return &SetValueRequest{
		MessagePayload: common.NewMessagePayload(common.SetValueRequest),
		ClientID:       clientID,
		RequestNumber:  requestNumber,
		Key:            key,
		Value:          value,
		Timestamp:      timestamp,
	}
}

// NewSetValueRequestSimple creates a new SetValueRequest with default clientId, requestNumber, and timestamp.
func NewSetValueRequestSimple(key, value string) *SetValueRequest {
	return &SetValueRequest{
		MessagePayload: common.NewMessagePayload(common.SetValueRequest),
		ClientID:       -1,
		RequestNumber:  -1,
		Key:            key,
		Value:          value,
		Timestamp:      -1,
	}
}

// SetValueResponse represents the response to a SetValueRequest.
type SetValueResponse struct {
	common.MessagePayload
	Result string `json:"result"`
}

// NewSetValueResponse creates a new SetValueResponse with the specified result.
func NewSetValueResponse(result string) *SetValueResponse {
	return &SetValueResponse{
		MessagePayload: common.NewMessagePayload(common.SetValueResponse),
		Result:         result,
	}
}

// VersionedSetValueRequest represents a request to set a value with versioning.
type VersionedSetValueRequest struct {
	common.MessagePayload
	ClientID      int64  `json:"clientId"`
	RequestNumber int    `json:"requestNumber"`
	Key           string `json:"key"`
	Value         string `json:"value"`
	Version       int64  `json:"version"`
}

// NewVersionedSetValueRequest creates a new VersionedSetValueRequest with all fields.
func NewVersionedSetValueRequest(
	key string,
	value string,
	clientID int64,
	requestNumber int,
	version int64,
) *VersionedSetValueRequest {
	return &VersionedSetValueRequest{
		MessagePayload: common.NewMessagePayload(common.VersionedSetValueRequest),
		ClientID:       clientID,
		RequestNumber:  requestNumber,
		Key:            key,
		Value:          value,
		Version:        version,
	}
}
