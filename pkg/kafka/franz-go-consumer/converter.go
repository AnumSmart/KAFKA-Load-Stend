package franzgoconsumer

import (
	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ToKafkaMessage(record *kgo.Record) *kafka.Message {
	headers := make(map[string][]byte, len(record.Headers))
	for _, h := range record.Headers {
		headers[h.Key] = h.Value
	}

	return &kafka.Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Timestamp: record.Timestamp,
		Key:       record.Key,
		Value:     record.Value,
		Headers:   headers,
	}
}
