package franzgoconsumer

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

type stubHandler struct {
	count int
}

func (h *stubHandler) HandleMessage(context.Context, *kafka.Message) error {
	h.count++
	return nil
}

func (h *stubHandler) OnBatchProcessed(int) {}

func TestNewBaseConsumerRequiresDeps(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := NewBaseConsumer(nil, &stubHandler{}, DefaultOptions(), logger); err == nil {
		t.Fatal("expected client error")
	}
	if _, err := NewBaseConsumer(&KafkaClientAdapter{}, nil, DefaultOptions(), logger); err == nil {
		t.Fatal("expected handler error")
	}
}

func TestToKafkaMessageAndHandler(t *testing.T) {
	h := &stubHandler{}
	msg := ToKafkaMessage(&kgo.Record{
		Topic: "load-test",
		Value: []byte("привет - я сообщение #1"),
	})
	if err := h.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if h.count != 1 {
		t.Fatalf("count = %d", h.count)
	}
}
