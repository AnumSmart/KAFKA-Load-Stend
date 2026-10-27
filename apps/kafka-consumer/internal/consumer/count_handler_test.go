package consumer

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"pkg/kafka"
)

func TestCountHandlerIncrementsOnlyAfterBatch(t *testing.T) {
	h := NewCountHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), "test-group")
	if err := h.HandleMessage(context.Background(), &kafka.Message{Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if h.Processed() != 0 {
		t.Fatalf("processed after HandleMessage = %d, want 0", h.Processed())
	}
	h.OnBatchProcessed(3)
	if h.Processed() != 3 {
		t.Fatalf("processed after OnBatchProcessed = %d, want 3", h.Processed())
	}
}
