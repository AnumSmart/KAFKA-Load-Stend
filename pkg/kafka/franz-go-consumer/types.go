package franzgoconsumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaClient — зависимость консьюмера от Kafka-клиента.
type KafkaClient interface {
	PollFetches(ctx context.Context) kgo.Fetches
	CommitUncommittedOffsets(ctx context.Context) error
	Close()
}
