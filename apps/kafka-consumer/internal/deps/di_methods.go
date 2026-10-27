package deps

import (
	"fmt"
	"kafka-consumer/internal/consumer"
	"time"

	franzgoconsumer "pkg/kafka/franz-go-consumer"
	"pkg/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

// initLogger инициализирует глобальный логгер из конфига. Побочный эффект:
// logger.GetLogger() становится доступен остальным частям приложения.
// Если по какой-то причине логгер не создался, возвращаем ошибку — без
// логов приложение работать не должно.
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

// initKafkaClient создаёт *kgo.Client по конфигу и регистрирует closer для
// его закрытия. Клиент один на приложение (на одну consumer group).
//
// Auto-commit внутри клиента выключен на уровне ToKgoConsumerOptions —
// логируем это явно, чтобы при разборе логов не возникало сомнений.
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
		"max_batch_size", c.config.KafkaClientConfig.Consumer.MaxBatchSize,
		"auto_commit", false,
		"session_timeout", c.config.KafkaClientConfig.Consumer.SessionTimeout.Duration().String(),
		"rebalance_timeout", c.config.KafkaClientConfig.Consumer.RebalanceTimeout.Duration().String(),
	)
	return nil
}

// initConsumer создаёт BaseConsumer с CountHandler и регистрирует closer,
// который вызовет Shutdown. Shutdown не закрывает *kgo.Client — только
// останавливает цикл и ждёт currentBatch==0. Порядок закрытия
// (Container.Close — обратный порядку регистрации): Shutdown ДО client.Close.
func (c *Container) initConsumer() error {
	handler := consumer.NewCountHandler(c.logger, c.config.KafkaClientConfig.Consumer.GroupID)
	opts := franzgoconsumer.DefaultOptions()
	opts.MaxBatchSize = c.config.KafkaClientConfig.Consumer.MaxBatchSize
	// Согласовано с GracefulShutdownTimeout в cmd (30s).
	opts.ShutdownTimeout = 30 * time.Second

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

	c.logger.Info("consumer initialized",
		"max_batch_size", opts.MaxBatchSize,
		"shutdown_timeout", opts.ShutdownTimeout.String(),
	)
	return nil
}
