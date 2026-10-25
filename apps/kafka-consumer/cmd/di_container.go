package main

import (
	"context"
	"kafka-consumer/internal/config"
	"kafka-consumer/internal/deps"
)

func NewDIContainer(ctx context.Context, cfg *config.ConsumerServiceConfig) (*deps.Container, error) {
	return deps.NewContainer(ctx, cfg)
}
