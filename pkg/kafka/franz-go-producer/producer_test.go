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

// TestDefaultOptionsMessageCountIs100k фиксирует целевой объём (PR #2).
// Регресс: раньше DefaultOptions.MessageCount был 10_000_000.
func TestDefaultOptionsMessageCountIs100k(t *testing.T) {
	opts := DefaultOptions()
	if opts.MessageCount != 100_000 {
		t.Fatalf("default MessageCount = %d, want 100000", opts.MessageCount)
	}
}

func TestRunCountsAcked(t *testing.T) {
	client := &mockClient{}
	opts := &ProducerOptions{
		Topic:           "load-test",
		MessageCount:    10,
		MessageTemplate: "привет - я сообщение #%d",
		MetricsHold:     0,
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
	opts := &ProducerOptions{MessageCount: 5, Topic: "load-test", MetricsHold: 0}

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

func TestRunUsesFixedMessageBytes(t *testing.T) {
	var got int
	client := &mockClient{
		produceFn: func(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
			got = len(r.Value)
			promise(r, nil)
		},
	}
	opts := &ProducerOptions{
		Topic:        "load-test",
		MessageCount: 2,
		MessageBytes: 102400,
		MetricsHold:  0,
	}

	p, err := NewBaseProducer(client, opts, silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got != 102400 {
		t.Fatalf("value length = %d, want 102400", got)
	}
}

func TestValidateRejectsNegativeMessageBytes(t *testing.T) {
	opts := &ProducerOptions{Topic: "t", MessageCount: 1, MessageBytes: -1}
	if err := opts.Validate(); err == nil {
		t.Fatal("expected error for message_bytes < 0")
	}
}

func TestValidateRejectsZeroMessageCount(t *testing.T) {
	opts := &ProducerOptions{Topic: "t", MessageCount: 0}
	if err := opts.Validate(); err == nil {
		t.Fatal("expected error for message_count=0")
	}
}

func TestCloseDelegatesToClient(t *testing.T) {
	closed := false
	client := &mockClient{
		produceFn: func(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
			promise(r, nil)
		},
	}
	// Перехватываем Close через обёртку.
	p, err := NewBaseProducer(&closeTrackingClient{mockClient: client, onClose: func() { closed = true }}, &ProducerOptions{
		Topic:        "load-test",
		MessageCount: 1,
		MetricsHold:  0,
	}, silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !closed {
		t.Fatal("expected client.Close to be called")
	}
}

type closeTrackingClient struct {
	*mockClient
	onClose func()
}

func (c *closeTrackingClient) Close() {
	if c.onClose != nil {
		c.onClose()
	}
	c.mockClient.Close()
}
