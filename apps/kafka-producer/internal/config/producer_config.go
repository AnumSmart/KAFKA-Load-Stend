package config

import (
	"fmt"
	"os"

	"pkg/configs"
	"pkg/logger"

	"github.com/joho/godotenv"
)

type ProducerServiceConfig struct {
	KafkaClientConfig *configs.KafkaClientConfig
	LoggerConfig      *logger.LoggerConfig
}

func LoadConfig() (*ProducerServiceConfig, error) {
	_ = godotenv.Load(".env")

	kafkaPath := os.Getenv("PRODUCER_CONFIG_ADDRESS_STRING")
	if kafkaPath == "" {
		kafkaPath = "configs-yml/producer.yml"
	}

	kafkaCfg, err := configs.LoadKafkaClientConfig(kafkaPath)
	if err != nil {
		return nil, fmt.Errorf("load kafka client config: %w", err)
	}

	loggerPath := os.Getenv("LOGGER_CONFIG_ADDRESS_STRING")
	if loggerPath == "" {
		loggerPath = "configs-yml/logger.yml"
	}

	loggerCfg, err := configs.LoadYAMLConfig(loggerPath, func() *logger.LoggerConfig {
		cfg := logger.DefaultLoggerConfig()
		cfg.Service = "kafka-producer"
		return cfg
	})
	if err != nil {
		return nil, fmt.Errorf("load logger config: %w", err)
	}

	return &ProducerServiceConfig{
		KafkaClientConfig: kafkaCfg,
		LoggerConfig:      loggerCfg,
	}, nil
}
