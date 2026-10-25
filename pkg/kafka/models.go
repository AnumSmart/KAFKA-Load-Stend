package kafka

import "time"

// Message — универсальное сообщение Kafka.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
}
