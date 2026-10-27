package deps

import (
	"fmt"
	"net/http"
	"time"

	franzgoproducer "pkg/kafka/franz-go-producer"
	"pkg/logger"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (c *Container) initLogger() error {
	logger.InitLogger(c.config.LoggerConfig)
	c.logger = logger.GetLogger()
	if c.logger == nil {
		return fmt.Errorf("failed to initialize logger")
	}
	c.logger.Info("logger initialized",
		"level", c.config.LoggerConfig.Level,
		"format", c.config.LoggerConfig.Format,
	)
	return nil
}

func (c *Container) initKafkaClient() error {
	opts, err := c.config.KafkaClientConfig.ToKgoProducerOptions()
	if err != nil {
		return err
	}

	kpromMetrics := franzgoproducer.NewKpromMetrics()
	opts = append(opts, kgo.WithHooks(kpromMetrics))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}

	c.kafkaClient = client
	// Close клиента — только через producer.Close (см. initProducer),
	// чтобы не было double-close одного *kgo.Client.

	c.logger.Info("kafka client initialized",
		"brokers", c.config.KafkaClientConfig.Brokers,
		"topic", c.config.KafkaClientConfig.Topic,
	)
	return nil
}

func (c *Container) initProducer() error {
	cfg := c.config.KafkaClientConfig
	opts := &franzgoproducer.ProducerOptions{
		Topic:           cfg.Topic,
		MessageCount:    cfg.Producer.MessageCount,
		MessageTemplate: cfg.Producer.MessageTemplate,
		MetricsHold:     cfg.Producer.MetricsHold.Duration(),
	}

	producer, err := franzgoproducer.NewBaseProducer(
		franzgoproducer.NewKafkaClientAdapter(c.kafkaClient),
		opts,
		c.logger,
	)
	if err != nil {
		return err
	}

	c.producer = producer
	c.addCloser(func() error {
		if err := c.producer.Close(); err != nil {
			return err
		}
		c.logger.Info("kafka client closed")
		return nil
	})
	return nil
}

func (c *Container) initMetricsServer() error {
	addr := c.config.KafkaClientConfig.Producer.MetricsAddr
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	c.metricsServer = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		c.logger.Info("metrics server listening", "addr", addr)
		if err := c.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			c.logger.Error("metrics server error", "error", err)
		}
	}()

	c.addCloser(func() error {
		return c.metricsServer.Close()
	})
	return nil
}
