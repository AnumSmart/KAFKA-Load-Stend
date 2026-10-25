package franzgoproducer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type mockClient struct {
	produceFn func(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	flushErr  error
}

func (m *mockClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	if m.produceFn != nil {
		m.produceFn(ctx, r, promise)
		return
	}
	promise(r, nil)
}

func (m *mockClient) Flush(context.Context) error { return m.flushErr }

func (m *mockClient) Close() {}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunCountsAcked(t *testing.T) {
	client := &mockClient{}
	opts := &ProducerOptions{
		Topic:           "load-test",
		MessageCount:    10,
		MessageTemplate: "привет - я сообщение #%d",
		ExpectedTotal:   10,
	}

	p, err := NewBaseProducer(client, opts, silentLogger())
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	produced, acked, failed := p.GetStats()
	if produced != 10 || acked != 10 || failed != 0 {
		t.Fatalf("stats produced=%d acked=%d failed=%d", produced, acked, failed)
	}
}

func TestRunCountsFailed(t *testing.T) {
	client := &mockClient{
		produceFn: func(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
			promise(r, errors.New("broker error"))
		},
	}
	opts := &ProducerOptions{MessageCount: 5, ExpectedTotal: 5, Topic: "load-test"}

	p, err := NewBaseProducer(client, opts, silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, acked, failed := p.GetStats()
	if acked != 0 || failed != 5 {
		t.Fatalf("acked=%d failed=%d", acked, failed)
	}
}

func TestNewBaseProducerRequiresClient(t *testing.T) {
	if _, err := NewBaseProducer(nil, nil, silentLogger()); err == nil {
		t.Fatal("expected error")
	}
}
