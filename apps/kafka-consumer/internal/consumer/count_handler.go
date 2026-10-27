package consumer

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"pkg/kafka"
)

// CountHandler считает обработанные сообщения без побочных эффектов
// и публикует метрики в Prometheus.
//
// Метрики:
//   - consumer_messages_processed_total{group="..."} — counter,
//     инкрементируется в HandleMessage.
//   - consumer_batches_total{group="..."} — counter,
//     инкрементируется в OnBatchProcessed.
//   - consumer_batch_size{group="..."} — histogram,
//     наблюдает размер каждого батча.
//
// Все метрики регистрируются в дефолтном реестре с ConstLabels group,
// чтобы Prometheus мог различать несколько групп на одном хосте.
type CountHandler struct {
	processed atomic.Int64
	logger    *slog.Logger
	debug     bool

	processedCnt prometheus.Counter
	batchesCnt   prometheus.Counter
	batchSize    prometheus.Histogram
}

func NewCountHandler(logger *slog.Logger, groupID string) *CountHandler {
	labels := prometheus.Labels{"group": groupID}
	return &CountHandler{
		logger: logger,
		processedCnt: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "consumer_messages_processed_total",
			Help:        "Число успешно обработанных сообщений",
			ConstLabels: labels,
		}),
		batchesCnt: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "consumer_batches_total",
			Help:        "Число успешно обработанных батчей",
			ConstLabels: labels,
		}),
		batchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "consumer_batch_size",
			Help:        "Размер обработанного батча",
			Buckets:     prometheus.ExponentialBuckets(100, 2, 8), // 100..12800
			ConstLabels: labels,
		}),
	}
}

func (h *CountHandler) HandleMessage(_ context.Context, msg *kafka.Message) error {
	h.processed.Add(1)
	h.processedCnt.Inc()
	if h.debug {
		h.logger.Debug("message processed",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
	}
	return nil
}

func (h *CountHandler) OnBatchProcessed(batchSize int) {
	h.batchesCnt.Inc()
	h.batchSize.Observe(float64(batchSize))
	h.logger.Info("batch processed",
		"batch_size", batchSize, "total", h.processed.Load())
}

func (h *CountHandler) Processed() int64 {
	return h.processed.Load()
}
