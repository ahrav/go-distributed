package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// InetAddressAndPort represents an IP address and port.
type InetAddressAndPort struct {
	Address net.IP `json:"address"`
	Port    int    `json:"port"`
}

// Equals checks if two InetAddressAndPort instances are equal.
func (i *InetAddressAndPort) Equals(other *InetAddressAndPort) bool {
	if i == other {
		return true
	}
	if other == nil {
		return false
	}
	return i.Address.Equal(other.Address) && i.Port == other.Port
}

// String returns the string representation in the format "[ip,port]".
func (i *InetAddressAndPort) String() string {
	return fmt.Sprintf("[%s,%d]", i.Address.String(), i.Port)
}

// MarshalJSON implements the json.Marshaler interface.
// It serializes the InetAddressAndPort to a JSON string in the format "[ip,port]".
func (i *InetAddressAndPort) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// It deserializes a JSON string in the format "[ip,port]" into an InetAddressAndPort.
func (i *InetAddressAndPort) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := Parse(s)
	if err != nil {
		return err
	}
	i.Address = parsed.Address
	i.Port = parsed.Port
	return nil
}

// Parse parses a string in the format "[ip,port]" into an InetAddressAndPort.
func Parse(key string) (*InetAddressAndPort, error) {
	if len(key) < 5 || key[0] != '[' || key[len(key)-1] != ']' {
		return nil, fmt.Errorf("invalid format: %s", key)
	}
	content := key[1 : len(key)-1]
	parts := strings.Split(content, ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid format: %s", key)
	}
	hostIP := strings.TrimSpace(parts[0])
	portStr := strings.TrimSpace(parts[1])
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %s", portStr)
	}
	return Create(hostIP, port)
}

// Create creates a new InetAddressAndPort from a host IP string and port.
// It returns an error if the host IP is invalid.
func Create(hostIP string, port int) (*InetAddressAndPort, error) {
	ip := net.ParseIP(hostIP)
	if ip == nil {
		// Attempt to resolve the host name
		resolvedIPs, err := net.LookupIP(hostIP)
		if err != nil || len(resolvedIPs) == 0 {
			return nil, fmt.Errorf("unknown host: %s", hostIP)
		}
		ip = resolvedIPs[0]
	}
	return &InetAddressAndPort{
		Address: ip,
		Port:    port,
	}, nil
}

// MustCreate creates a new InetAddressAndPort from a host IP string and port.
// It panics if the host IP is invalid.
func MustCreate(hostIP string, port int) *InetAddressAndPort {
	addr, err := Create(hostIP, port)
	if err != nil {
		panic(err)
	}
	return addr
}

// NewInetAddressAndPort creates a new InetAddressAndPort with the given address and port.
func NewInetAddressAndPort(address net.IP, port int) *InetAddressAndPort {
	return &InetAddressAndPort{Address: address, Port: port}
}

// MustParse parses a string in the format "[ip,port]" into an InetAddressAndPort.
// It panics if the format is invalid.
func MustParse(key string) *InetAddressAndPort {
	addr, err := Parse(key)
	if err != nil {
		panic(err)
	}
	return addr
}

// GetAddress returns the IP address as a net.IP.
func (i *InetAddressAndPort) GetAddress() net.IP { return i.Address }

// GetPort returns the port number.
func (i *InetAddressAndPort) GetPort() int { return i.Port }

// CompareTo compares two InetAddressAndPort instances.
// Returns -1 if i < other, 0 if i == other, 1 if i > other
func (i *InetAddressAndPort) CompareTo(other *InetAddressAndPort) int {
	cmp := strings.Compare(i.Address.String(), other.Address.String())
	if cmp == 0 {
		if i.Port < other.Port {
			return -1
		} else if i.Port > other.Port {
			return 1
		}
		return 0
	}
	return cmp
}

// EqualsBytes is a helper method to compare byte slices of IP addresses.
func (i *InetAddressAndPort) EqualsBytes(other *InetAddressAndPort) bool {
	if i == other {
		return true
	}
	if other == nil {
		return false
	}
	return bytes.Equal(i.Address, other.Address) && i.Port == other.Port
}
