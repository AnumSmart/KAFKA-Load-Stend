package main

import (
	"context"
	"kafka-producer/internal/deps"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const GracefulShutdownTimeout = 30 * time.Second

func waitForShutdown(cancel context.CancelFunc, runErrors <-chan error, logger *slog.Logger) {
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
		logger.Info("cancelling produce run")
		cancel()
		select {
		case err, ok := <-runErrors:
			if ok && err != nil {
				logger.Error("producer finished with error", "error", err)
			}
		case <-time.After(GracefulShutdownTimeout):
			logger.Warn("timeout waiting for produce run to stop")
		}
	case err, ok := <-runErrors:
		if ok && err != nil {
			logger.Error("producer finished with error", "error", err)
		} else {
			logger.Info("producer finished successfully")
		}
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
