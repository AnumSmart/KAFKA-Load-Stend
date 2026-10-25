package deps

import (
	"context"
	"fmt"
	"kafka-consumer/internal/config"
	"log/slog"
	"sync"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Container struct {
	config      *config.ConsumerServiceConfig
	logger      *slog.Logger
	kafkaClient *kgo.Client
	consumer    kafka.Consumer
	closers     []func() error
	closeOnce   sync.Once
	closeErr    error
}

func NewContainer(_ context.Context, cfg *config.ConsumerServiceConfig) (*Container, error) {
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
	if err := c.initConsumer(); err != nil {
		c.Close()
		return nil, fmt.Errorf("init consumer: %w", err)
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

func (c *Container) GetConsumer() kafka.Consumer {
	return c.consumer
}

func (c *Container) GetLogger() *slog.Logger {
	return c.logger
}
