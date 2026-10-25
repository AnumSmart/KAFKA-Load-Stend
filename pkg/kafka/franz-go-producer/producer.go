package franzgoproducer

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

var _ kafka.Producer = (*BaseProducer)(nil)

// BaseProducer — асинхронный нагрузочный продюсер.
type BaseProducer struct {
	client  ProducerClient
	options *ProducerOptions
	logger  *slog.Logger

	produced atomic.Int64
	acked    atomic.Int64
	failed   atomic.Int64
}

func NewBaseProducer(client ProducerClient, opts *ProducerOptions, logger *slog.Logger) (*BaseProducer, error) {
	if client == nil {
		return nil, ErrClientNotInitialized
	}
	if opts == nil {
		opts = DefaultOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &BaseProducer{
		client:  client,
		options: opts,
		logger:  logger,
	}, nil
}

func (p *BaseProducer) Run(ctx context.Context) error {
	start := time.Now()
	p.logger.Info("starting produce run",
		"topic", p.options.Topic,
		"message_count", p.options.MessageCount,
	)

	for n := int64(1); n <= p.options.MessageCount; n++ {
		if err := ctx.Err(); err != nil {
			p.logger.Warn("produce loop interrupted", "error", err, "produced", p.produced.Load())
			break
		}

		record := &kgo.Record{
			Topic: p.options.Topic,
			Value: []byte(fmt.Sprintf(p.options.MessageTemplate, n)),
		}

		p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
			if err != nil {
				p.failed.Add(1)
				errorsTotal.Inc()
				return
			}
			p.acked.Add(1)
			ackedTotal.Inc()
		})

		p.produced.Add(1)
		producedTotal.Inc()

		if n%1_000_000 == 0 {
			p.logger.Info("produce progress",
				"produced", n,
				"acked", p.acked.Load(),
				"failed", p.failed.Load(),
			)
		}
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := p.client.Flush(flushCtx); err != nil {
		p.logger.Error("flush failed", "error", err)
		return fmt.Errorf("flush: %w", err)
	}

	elapsed := time.Since(start)
	produced, acked, failed := p.GetStats()
	avg := float64(0)
	if elapsed.Seconds() > 0 {
		avg = float64(acked) / elapsed.Seconds()
	}

	p.logger.Info("produce run finished",
		"produced", produced,
		"acked", acked,
		"failed", failed,
		"duration", elapsed.String(),
		"avg_msg_sec", int64(avg),
	)

	if acked != p.options.ExpectedTotal {
		p.logger.Warn("acked count differs from expected",
			"acked", acked,
			"expected", p.options.ExpectedTotal,
			"failed", failed,
		)
	}

	return nil
}

func (p *BaseProducer) Close() error {
	return nil
}

func (p *BaseProducer) GetStats() (produced, acked, failed int64) {
	return p.produced.Load(), p.acked.Load(), p.failed.Load()
}
