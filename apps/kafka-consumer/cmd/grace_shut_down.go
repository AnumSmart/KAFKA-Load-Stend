package main

import (
	"context"
	"kafka-consumer/internal/deps"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pkg/kafka"
)

const GracefulShutdownTimeout = 30 * time.Second

func waitForShutdown(cancel context.CancelFunc, loadConsumer kafka.Consumer, consumerErrors <-chan error, logger *slog.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)

	select {
	case sig := <-sigChan:
		logger.Info("received signal", "signal", sig.String())
		if sig == syscall.SIGQUIT {
			logger.Warn("SIGQUIT received, forcing shutdown")
			cancel()
			return
		}
		cancel()
	case err := <-consumerErrors:
		if err != nil {
			logger.Error("consumer error, initiating shutdown", "error", err)
		}
	}

	performGracefulShutdown(loadConsumer, logger)
}

func performGracefulShutdown(loadConsumer kafka.Consumer, logger *slog.Logger) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), GracefulShutdownTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		loadConsumer.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("graceful shutdown completed")
	case <-shutdownCtx.Done():
		logger.Warn("graceful shutdown timeout exceeded")
	}
}

func gracefulShutdownResources(container *deps.Container, logger *slog.Logger) {
	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		if err := container.Close(); err != nil {
			logger.Warn("container close error", "error", err)
		}
		close(done)
	}()

	select {
	case <-done:
		logger.Info("resources closed")
	case <-closeCtx.Done():
		logger.Warn("resource cleanup timeout exceeded")
	}
}
