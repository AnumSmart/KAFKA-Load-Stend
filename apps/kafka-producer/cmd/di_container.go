package main

import (
	"context"
	"kafka-producer/internal/config"
	"kafka-producer/internal/deps"
)

func NewDIContainer(ctx context.Context, cfg *config.ProducerServiceConfig) (*deps.Container, error) {
	return deps.NewContainer(ctx, cfg)
}
