package kafka

import "errors"

var (
	ErrClientNotInitialized  = errors.New("kafka client is not initialized")
	ErrHandlerNotInitialized = errors.New("message handler is not initialized")
	ErrProducerNotReady      = errors.New("producer is not initialized")
	ErrInvalidOptions        = errors.New("invalid client options")
)
