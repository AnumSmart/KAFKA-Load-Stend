package consumer

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"pkg/kafka"
)

func TestCountHandlerIncrements(t *testing.T) {
	h := NewCountHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), "test-group")
	if err := h.HandleMessage(context.Background(), &kafka.Message{Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if h.Processed() != 1 {
		t.Fatalf("processed = %d", h.Processed())
	}
	h.OnBatchProcessed(1)
}
