package deps

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"kafka-producer/internal/config"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Container struct {
	config        *config.ProducerServiceConfig
	logger        *slog.Logger
	kafkaClient   *kgo.Client
	producer      kafka.Producer
	metricsServer *http.Server
	closers       []func() error
	closeOnce     sync.Once
	closeErr      error
}

func NewContainer(ctx context.Context, cfg *config.ProducerServiceConfig) (*Container, error) {
	c := &Container{
		config:  cfg,
		closers: make([]func() error, 0),
	}

	if err := c.initLogger(); err != nil {
		return nil, fmt.Errorf("init logger: %w", err)
	}
	if err := c.initKafkaClient(); err != nil {
		c.Close()
		return nil, fmt.Errorf("init kafka client: %w", err)
	}
	if err := c.initProducer(); err != nil {
		c.Close()
		return nil, fmt.Errorf("init producer: %w", err)
	}
	if err := c.initMetricsServer(); err != nil {
		c.Close()
		return nil, fmt.Errorf("init metrics server: %w", err)
	}

	c.logger.Info("DI container initialized")
	return c, nil
}

func (c *Container) addCloser(closer func() error) {
	c.closers = append(c.closers, closer)
}

func (c *Container) Close() error {
	c.closeOnce.Do(func() {
		var errs []error
		for i := len(c.closers) - 1; i >= 0; i-- {
			if err := c.closers[i](); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			c.closeErr = fmt.Errorf("close errors: %v", errs)
		}
	})
	return c.closeErr
}

func (c *Container) GetProducer() kafka.Producer {
	return c.producer
}

func (c *Container) GetLogger() *slog.Logger {
	return c.logger
}
