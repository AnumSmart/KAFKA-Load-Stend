package consumer

import (
	"context"
	"log/slog"
	"sync/atomic"

	"pkg/kafka"
)

// CountHandler считает обработанные сообщения без побочных эффектов.
type CountHandler struct {
	processed atomic.Int64
	logger    *slog.Logger
	debug     bool
}

func NewCountHandler(logger *slog.Logger) *CountHandler {
	return &CountHandler{logger: logger}
}

func (h *CountHandler) HandleMessage(_ context.Context, msg *kafka.Message) error {
	h.processed.Add(1)
	if h.debug {
		h.logger.Debug("message processed",
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
	}
	return nil
}

func (h *CountHandler) OnBatchProcessed(batchSize int) {
	h.logger.Info("batch processed",
		"batch_size", batchSize,
		"total", h.processed.Load(),
	)
}

func (h *CountHandler) Processed() int64 {
	return h.processed.Load()
}
