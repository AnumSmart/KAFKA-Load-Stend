package kafka

import "context"

// MessageHandler — обработка сообщений в бизнес-слое.
type MessageHandler interface {
	HandleMessage(ctx context.Context, msg *Message) error
	OnBatchProcessed(batchSize int)
}

// Producer — нагрузочный продюсер.
type Producer interface {
	Run(ctx context.Context) error
	Close() error
	GetStats() (produced, acked, failed int64)
}

// Consumer — консьюмер стенда.
type Consumer interface {
	Start(ctx context.Context) error
	Shutdown()
	GetStats() (processed int64)
}
