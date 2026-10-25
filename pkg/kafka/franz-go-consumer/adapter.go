package franzgoconsumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaClientAdapter struct {
	client *kgo.Client
}

func NewKafkaClientAdapter(client *kgo.Client) *KafkaClientAdapter {
	return &KafkaClientAdapter{client: client}
}

func (a *KafkaClientAdapter) PollFetches(ctx context.Context) kgo.Fetches {
	return a.client.PollFetches(ctx)
}

func (a *KafkaClientAdapter) CommitUncommittedOffsets(ctx context.Context) error {
	return a.client.CommitUncommittedOffsets(ctx)
}

func (a *KafkaClientAdapter) Close() {
	a.client.Close()
}
