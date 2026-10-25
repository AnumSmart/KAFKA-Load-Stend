package main

import (
	"context"
	"log/slog"

	"pkg/kafka"
)

func startConsumer(ctx context.Context, loadConsumer kafka.Consumer, logger *slog.Logger) <-chan error {
	errorChan := make(chan error, 1)

	go func() {
		logger.Info("starting consumer loop")
		if err := loadConsumer.Start(ctx); err != nil {
			logger.Error("consumer error", "error", err)
			errorChan <- err
		}
		close(errorChan)
		logger.Info("consumer finished")
	}()

	return errorChan
}
