package messages

import "github.com/ahrav/go-distributed/replicate/common"

// GetValueRequest represents a request to get the value associated with a key.
type GetValueRequest struct {
	common.MessagePayload
	Key string `json:"key"`
}

// NewGetValueRequest creates a new GetValueRequest instance.
func NewGetValueRequest(key string) *GetValueRequest {
	return &GetValueRequest{
		MessagePayload: common.NewMessagePayload(common.GetValueRequest),
		Key:            key,
	}
}

// GetValueResponse represents a response containing the stored value.
type GetValueResponse struct {
	common.MessagePayload
	Value StoredValue `json:"value"`
}

// NewGetValueResponse creates a new GetValueResponse instance.
func NewGetValueResponse(value StoredValue) *GetValueResponse {
	return &GetValueResponse{
		MessagePayload: common.NewMessagePayload(common.GetValueResponse),
		Value:          value,
	}
}

// GetVersionRequest represents a request to get the version associated with a key.
type GetVersionRequest struct {
	common.MessagePayload
	Key string `json:"key"`
}

// NewGetVersionRequest creates a new GetVersionRequest instance.
func NewGetVersionRequest(key string) *GetVersionRequest {
	return &GetVersionRequest{
		MessagePayload: common.NewMessagePayload(common.GetVersion),
		Key:            key,
	}
}

// GetVersionResponse represents a response containing the version (MonotonicId).
type GetVersionResponse struct {
	common.MessagePayload
	Version common.MonotonicID `json:"version"`
}

// NewGetVersionResponse creates a new GetVersionResponse instance.
func NewGetVersionResponse(version common.MonotonicID) *GetVersionResponse {
	return &GetVersionResponse{
		MessagePayload: common.NewMessagePayload(common.GetVersionResponse),
		Version:        version,
	}
}

// SetValueRequest represents a request to set a value for a key.
type SetValueRequest struct {
	common.MessagePayload
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NewSetValueRequest creates a new SetValueRequest instance.
func NewSetValueRequest(key, value string) *SetValueRequest {
	return &SetValueRequest{
		MessagePayload: common.NewMessagePayload(common.SetValueRequest),
		Key:            key,
		Value:          value,
	}
}

// SetValueResponse represents a response indicating the result of a set operation.
type SetValueResponse struct {
	common.MessagePayload
	Result string `json:"result"`
}

// NewSetValueResponse creates a new SetValueResponse instance.
func NewSetValueResponse(result string) *SetValueResponse {
	return &SetValueResponse{
		MessagePayload: common.NewMessagePayload(common.SetValueResponse),
		Result:         result,
	}
}

// VersionedSetValueRequest represents a request to set a value for a key with a specific version.
type VersionedSetValueRequest struct {
	common.MessagePayload
	Key     string             `json:"key"`
	Value   string             `json:"value"`
	Version common.MonotonicID `json:"version"`
}

// NewVersionedSetValueRequest creates a new VersionedSetValueRequest instance.
func NewVersionedSetValueRequest(key, value string, version common.MonotonicID) *VersionedSetValueRequest {
	return &VersionedSetValueRequest{
		MessagePayload: common.NewMessagePayload(common.VersionedSetValueRequest),
		Key:            key,
		Value:          value,
		Version:        version,
	}
}
