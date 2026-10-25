package franzgoconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

var _ kafka.Consumer = (*BaseConsumer)(nil)

// BaseConsumer — lean-консьюмер без DLQ.
type BaseConsumer struct {
	recordPool         sync.Pool
	statsPrintInterval time.Duration
	lastStatsTime      time.Time
	messagesProcessed  atomic.Int64
	currentBatch       atomic.Int64
	client             KafkaClient
	handler            kafka.MessageHandler
	logger             *slog.Logger
	options            *ConsumerOptions
	isShuttingDown     atomic.Bool
}

func NewBaseConsumer(client KafkaClient, handler kafka.MessageHandler, opts *ConsumerOptions, logger *slog.Logger) (*BaseConsumer, error) {
	if client == nil {
		return nil, ErrClientNotInitialized
	}
	if handler == nil {
		return nil, ErrHandlerNotInitialized
	}
	if opts == nil {
		opts = DefaultOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer options: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}

	bc := &BaseConsumer{
		client:             client,
		handler:            handler,
		options:            opts,
		logger:             logger,
		statsPrintInterval: opts.StatsPrintInterval,
		lastStatsTime:      time.Now(),
	}
	bc.recordPool = sync.Pool{
		New: func() any {
			capacity := bc.options.MaxBatchSize
			if capacity == 0 {
				capacity = 1000
			}
			s := make([]*kgo.Record, 0, capacity)
			return &s
		},
	}
	return bc, nil
}

func (bc *BaseConsumer) Start(ctx context.Context) error {
	bc.logger.Info("starting consumer")

	for {
		if bc.isShuttingDown.Load() {
			return bc.waitForCompletion()
		}

		select {
		case <-ctx.Done():
			bc.logger.Info("shutdown signal received")
			bc.isShuttingDown.Store(true)
			return bc.waitForCompletion()
		default:
			bc.pollAndProcessMessages(ctx)
			if bc.isShuttingDown.Load() {
				return bc.waitForCompletion()
			}
			bc.printStatsIfNeeded()
		}
	}
}

func (bc *BaseConsumer) waitForCompletion() error {
	timeout := bc.options.ShutdownTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if bc.currentBatch.Load() == 0 {
			bc.logger.Info("all pending messages processed")
			return nil
		}
		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				bc.logger.Warn("shutdown timeout, forcing stop",
					"pending", bc.currentBatch.Load(),
				)
				return nil
			}
		}
	}
}

func (bc *BaseConsumer) pollAndProcessMessages(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fetches := bc.client.PollFetches(pollCtx)
	if fetches.Empty() {
		return
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, fetchErr := range errs {
			if fetchErr.Err == context.DeadlineExceeded || fetchErr.Err == context.Canceled {
				continue
			}
			bc.logger.Error("fetch error",
				"error", fetchErr.Err,
				"topic", fetchErr.Topic,
				"partition", fetchErr.Partition,
			)
		}
	}

	bc.processFetches(ctx, fetches)
}

func (bc *BaseConsumer) processFetches(ctx context.Context, fetches kgo.Fetches) {
	iter := fetches.RecordIter()
	recordsPtr := bc.recordPool.Get().(*[]*kgo.Record)
	records := (*recordsPtr)[:0]
	defer func() {
		*recordsPtr = records
		bc.recordPool.Put(recordsPtr)
	}()

	maxBatch := int(bc.options.MaxBatchSize)
	for !iter.Done() && len(records) < maxBatch {
		records = append(records, iter.Next())
	}
	if len(records) == 0 {
		return
	}

	bc.currentBatch.Store(int64(len(records)))
	defer bc.currentBatch.Store(0)

	successCount := 0
	for i, record := range records {
		if bc.isShuttingDown.Load() {
			bc.logger.Warn("stopping batch due to shutdown",
				"processed", successCount,
				"remaining", len(records)-i,
			)
			return
		}

		msg := ToKafkaMessage(record)
		if err := bc.processMessageSafely(ctx, msg); err != nil {
			bc.logger.Error("failed to process message",
				"error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
			continue
		}

		successCount++
		bc.messagesProcessed.Add(1)
	}

	if !bc.isShuttingDown.Load() {
		bc.commitOffsets(ctx, len(records))
	}

	if successCount > 0 {
		bc.handler.OnBatchProcessed(successCount)
	}
}

func (bc *BaseConsumer) processMessageSafely(ctx context.Context, msg *kafka.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in handler: %v", r)
		}
	}()
	return bc.handler.HandleMessage(ctx, msg)
}

func (bc *BaseConsumer) commitOffsets(ctx context.Context, batchSize int) {
	if bc.options.CommitInterval > 0 || batchSize == 0 {
		return
	}
	if err := bc.client.CommitUncommittedOffsets(ctx); err != nil {
		bc.logger.Error("failed to commit offsets", "error", err)
	}
}

func (bc *BaseConsumer) Shutdown() {
	bc.logger.Info("shutting down consumer")
	bc.isShuttingDown.Store(true)
	_ = bc.waitForCompletion()
	bc.client.Close()
	bc.logger.Info("consumer shutdown completed", "processed", bc.messagesProcessed.Load())
}

func (bc *BaseConsumer) GetStats() (processed int64) {
	return bc.messagesProcessed.Load()
}

func (bc *BaseConsumer) printStatsIfNeeded() {
	if time.Since(bc.lastStatsTime) < bc.statsPrintInterval {
		return
	}
	bc.logger.Info("consumer statistics",
		"processed", bc.messagesProcessed.Load(),
		"pending", bc.currentBatch.Load(),
	)
	bc.lastStatsTime = time.Now()
}
