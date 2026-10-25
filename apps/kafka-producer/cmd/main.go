package main

import (
	"context"
	"kafka-producer/internal/config"
	"log"
	"os"
)

func main() {
	log.Println("Starting Kafka load producer")

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

	producer := container.GetProducer()
	if producer == nil {
		logger.Error("producer is nil")
		os.Exit(1)
	}

	runErrors := startProducer(ctx, producer, logger)
	waitForShutdown(cancel, runErrors, logger)

	logger.Info("producer service stopped")
}
