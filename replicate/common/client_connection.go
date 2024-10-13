package common

// ClientConnection represents a client connection with methods to write responses and close the connection.
type ClientConnection interface {
	Write(response RequestOrResponse) error
	Close() error
}
