package main

import (
	"context"
	"kafka-consumer/internal/config"
	"log"
	"os"
)

func main() {
	log.Println("Starting Kafka load consumer")

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Printf("failed to load config: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	container, err := NewDIContainer(ctx, cfg)
	if err != nil {
		log.Printf("failed to create DI container: %v", err)
		os.Exit(1)
	}

	logger := container.GetLogger()
	defer gracefulShutdownResources(container, logger)

	startMetricsServer(ctx, cfg.KafkaClientConfig.Consumer.MetricsAddr, logger)

	loadConsumer := container.GetConsumer()
	if loadConsumer == nil {
		logger.Error("consumer is nil")
		os.Exit(1)
	}

	consumerErrors := startConsumer(ctx, loadConsumer, logger)
	waitForShutdown(cancel, loadConsumer, consumerErrors, logger)

	logger.Info("consumer service stopped")
}
