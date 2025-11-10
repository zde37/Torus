package pkg

import "errors"

var (
	// ErrKeyNotFound is returned when a key doesn't exist
	ErrKeyNotFound = errors.New("key not found")

	// ErrContextCanceled is returned when the context is canceled
	ErrContextCanceled = errors.New("context canceled")

	// ErrStorageUnavailable is returned when storage is closed
	ErrStorageUnavailable = errors.New("storage unavailable")
)
