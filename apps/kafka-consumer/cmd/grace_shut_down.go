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

// waitForShutdown ждёт сигнал или ошибку Start, затем:
//  1. отменяет ctx (если ещё не отменён),
//  2. дожидается выхода Start через consumerErrors,
//  3. вызывает Shutdown (ждёт обнуления currentBatch, клиент не закрывает).
//
// Закрытие *kgo.Client делает container.Close() в defer main — после Shutdown.
func waitForShutdown(cancel context.CancelFunc, loadConsumer kafka.Consumer, consumerErrors <-chan error, logger *slog.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)

	select {
	case sig := <-sigChan:
		logger.Info("received signal", "signal", sig.String())
		if sig == syscall.SIGQUIT {
			logger.Warn("SIGQUIT received, forcing shutdown")
			cancel()
			drainConsumerErrors(consumerErrors, 2*time.Second, logger)
			return
		}
		cancel()
	case err := <-consumerErrors:
		if err != nil {
			logger.Error("consumer error, initiating shutdown", "error", err)
		} else {
			logger.Info("consumer loop exited")
		}
		performGracefulShutdown(loadConsumer, logger)
		return
	}

	// После cancel() ждём, пока Start выйдет из PollFetches/processRecords.
	// Только потом Shutdown — иначе client.Close в DI гоняется с CommitRecords.
	drainConsumerErrors(consumerErrors, GracefulShutdownTimeout, logger)
	performGracefulShutdown(loadConsumer, logger)
}

func drainConsumerErrors(consumerErrors <-chan error, timeout time.Duration, logger *slog.Logger) {
	select {
	case err, ok := <-consumerErrors:
		if ok && err != nil {
			logger.Error("consumer finished with error", "error", err)
		} else {
			logger.Info("consumer loop stopped")
		}
	case <-time.After(timeout):
		logger.Warn("timeout waiting for consumer loop to stop",
			"timeout", timeout.String(),
		)
	}
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
