package main

import (
	"context"
	"log/slog"

	"pkg/kafka"
)

func startProducer(ctx context.Context, producer kafka.Producer, logger *slog.Logger) <-chan error {
	errorChan := make(chan error, 1)

	go func() {
		logger.Info("starting produce run")
		if err := producer.Run(ctx); err != nil {
			logger.Error("producer error", "error", err)
			errorChan <- err
		}
		close(errorChan)
		logger.Info("produce run finished")
	}()

	return errorChan
}
