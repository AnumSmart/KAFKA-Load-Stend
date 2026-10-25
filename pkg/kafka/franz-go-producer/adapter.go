package franzgoproducer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaClientAdapter оборачивает *kgo.Client.
type KafkaClientAdapter struct {
	client *kgo.Client
}

func NewKafkaClientAdapter(client *kgo.Client) *KafkaClientAdapter {
	return &KafkaClientAdapter{client: client}
}

func (a *KafkaClientAdapter) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	a.client.Produce(ctx, r, promise)
}

func (a *KafkaClientAdapter) Flush(ctx context.Context) error {
	return a.client.Flush(ctx)
}

func (a *KafkaClientAdapter) Close() {
	a.client.Close()
}
