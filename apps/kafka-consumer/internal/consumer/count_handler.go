package consumer

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"pkg/kafka"
)

// CountHandler считает закоммиченные сообщения без побочных эффектов
// и публикует метрики в Prometheus.
//
// Метрики (все обновляются в OnBatchProcessed — только после успешного
// CommitRecords в BaseConsumer):
//   - consumer_messages_processed_total{group="..."} — counter,
//     +batchSize после коммита;
//   - consumer_batches_total{group="..."} — counter;
//   - consumer_batch_size{group="..."} — histogram размера батча.
//
// HandleMessage намеренно пустой по счётчикам: иначе при ошибке коммита
// метрики опережали бы реальный прогресс группы.
//
// Все метрики регистрируются в дефолтном реестре с ConstLabels group,
// чтобы Prometheus мог различать несколько групп на одном хосте.
type CountHandler struct {
	processed atomic.Int64
	logger    *slog.Logger

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
			Help:        "Число успешно закоммиченных сообщений",
			ConstLabels: labels,
		}),
		batchesCnt: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "consumer_batches_total",
			Help:        "Число успешно закоммиченных батчей",
			ConstLabels: labels,
		}),
		batchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "consumer_batch_size",
			Help:        "Размер закоммиченного батча",
			Buckets:     prometheus.ExponentialBuckets(100, 2, 8), // 100..12800
			ConstLabels: labels,
		}),
	}
}

func (h *CountHandler) HandleMessage(_ context.Context, _ *kafka.Message) error {
	return nil
}

func (h *CountHandler) OnBatchProcessed(batchSize int) {
	if batchSize > 0 {
		h.processed.Add(int64(batchSize))
		h.processedCnt.Add(float64(batchSize))
	}
	h.batchesCnt.Inc()
	h.batchSize.Observe(float64(batchSize))
	h.logger.Info("batch committed",
		"batch_size", batchSize, "total", h.processed.Load())
}

func (h *CountHandler) Processed() int64 {
	return h.processed.Load()
}
