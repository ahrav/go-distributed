package common

// Response encapsulates the result of a network operation, which can be either a success or an error.
type Response[T any] struct {
	Result       T
	ErrorMessage *string
}

// IsSuccess returns true if the response is successful.
func (r *Response[T]) IsSuccess() bool {
	return r.ErrorMessage == nil
}

// IsError returns true if the response contains an error.
func (r *Response[T]) IsError() bool {
	return r.ErrorMessage != nil
}

// GetResult returns the result of the response.
func (r *Response[T]) GetResult() T {
	return r.Result
}

// GetErrorMessage returns the error message of the response, if any.
func (r *Response[T]) GetErrorMessage() *string {
	return r.ErrorMessage
}

// NewErrorResponse creates a new Response instance representing an error.
func NewErrorResponse[T any](message string) Response[T] {
	return Response[T]{ErrorMessage: &message}
}

// NewSuccessResponse creates a new Response instance representing a successful result.
func NewSuccessResponse[T any](result T) Response[T] {
	return Response[T]{Result: result}
}
