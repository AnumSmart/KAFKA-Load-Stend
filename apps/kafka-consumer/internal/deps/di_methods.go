package deps

import (
	"fmt"
	"kafka-consumer/internal/consumer"

	franzgoconsumer "pkg/kafka/franz-go-consumer"
	"pkg/logger"

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
		"group_id", c.config.KafkaClientConfig.Consumer.GroupID,
	)
	return nil
}

func (c *Container) initKafkaClient() error {
	opts, err := c.config.KafkaClientConfig.ToKgoConsumerOptions()
	if err != nil {
		return err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}

	c.kafkaClient = client
	c.addCloser(func() error {
		c.kafkaClient.Close()
		c.logger.Info("kafka client closed")
		return nil
	})

	c.logger.Info("kafka client initialized",
		"brokers", c.config.KafkaClientConfig.Brokers,
		"topic", c.config.KafkaClientConfig.Topic,
		"group", c.config.KafkaClientConfig.Consumer.GroupID,
		"start_offset", c.config.KafkaClientConfig.Consumer.StartOffset,
	)
	return nil
}

func (c *Container) initConsumer() error {
	handler := consumer.NewCountHandler(c.logger)
	opts := franzgoconsumer.DefaultOptions()
	opts.MaxBatchSize = c.config.KafkaClientConfig.Consumer.MaxBatchSize
	opts.CommitInterval = c.config.KafkaClientConfig.Consumer.CommitInterval.Duration()
	if c.config.KafkaClientConfig.Consumer.DisableAutoCommit {
		opts.CommitInterval = 0
	}

	base, err := franzgoconsumer.NewBaseConsumer(
		franzgoconsumer.NewKafkaClientAdapter(c.kafkaClient),
		handler,
		opts,
		c.logger,
	)
	if err != nil {
		return err
	}

	c.consumer = base
	c.addCloser(func() error {
		c.consumer.Shutdown()
		return nil
	})
	return nil
}
