package franzgoconsumer

import "errors"

var (
	ErrClientNotInitialized  = errors.New("consumer client is not initialized")
	ErrHandlerNotInitialized = errors.New("message handler is not initialized")
)
