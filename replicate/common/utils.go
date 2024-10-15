package common

import (
	"crypto"
	"fmt"
	"math/big"
	"sync"
)

// CompletionResult encapsulates the result of an asynchronous operation.
// It holds either a successful value or an error.
type CompletionResult[T any] struct {
	Value T
	Err   error
}

// Hash computes the MD5 hash of the input data and returns it as a big.Int.
// This function is analogous to the Java method: public static BigInteger hash(byte[] bytes)
func Hash(data []byte) *big.Int {
	hashBytes, err := HashWithType("MD5", data)
	if err != nil {
		// TODO: Handle error gracefully instead of panicking.
		panic(fmt.Sprintf("HashWithType failed: %v", err))
	}
	hashInt := new(big.Int).SetBytes(hashBytes)
	return hashInt.Abs(hashInt)
}

// HashWithType computes the hash of the input data using the specified hash algorithm.
// Supported hash types include "MD5", "SHA1", "SHA256", etc.
// This function is analogous to the Java method: public static byte[] hash(String type, byte[] data)
func HashWithType(hashType string, data []byte) ([]byte, error) {
	var hasher crypto.Hash
	switch hashType {
	case "MD5":
		hasher = crypto.MD5
	case "SHA1":
		hasher = crypto.SHA1
	case "SHA256":
		hasher = crypto.SHA256
	// Add more hash types as needed.
	default:
		return nil, fmt.Errorf("unsupported hash type: %s", hashType)
	}

	if !hasher.Available() {
		return nil, fmt.Errorf("hash type %s is not available", hashType)
	}

	h := hasher.New()
	if _, err := h.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data to hasher: %v", err)
	}
	return h.Sum(nil), nil
}

// Sequence aggregates multiple asynchronous results into a single channel.
// It waits for all input channels to emit their results and then sends a slice of CompletionResult[T].
func Sequence[T any](futures []<-chan CompletionResult[T]) <-chan []CompletionResult[T] {
	var wg sync.WaitGroup
	results := make([]CompletionResult[T], len(futures))

	for i, ch := range futures {
		wg.Add(1)
		go func(i int, ch <-chan CompletionResult[T]) {
			defer wg.Done()
			if result, ok := <-ch; ok {
				results[i] = result
			} else {
				// Channel closed without sending a result; assign zero value with an error.
				results[i] = CompletionResult[T]{Value: *new(T), Err: fmt.Errorf("channel closed without result")}
			}
		}(i, ch)
	}

	done := make(chan []CompletionResult[T], 1)
	go func() {
		wg.Wait()
		done <- results
		close(done)
	}()
	return done
}
