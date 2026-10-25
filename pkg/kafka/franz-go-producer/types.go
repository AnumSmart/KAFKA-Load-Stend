package franzgoproducer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerClient — зависимость продюсера от Kafka-клиента.
type ProducerClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	Close()
}
