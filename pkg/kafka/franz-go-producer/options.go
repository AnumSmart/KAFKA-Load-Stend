package franzgoproducer

import (
	"fmt"
	"time"
)

// ProducerOptions — настройки прогона, не связанные с kgo.Client.
//
// Eдинственный источник истины — MessageCount.
type ProducerOptions struct {
	Topic           string
	MessageCount    int64
	MessageTemplate string
	// MessageBytes — длина value в байтах. 0 — значение берётся из
	// MessageTemplate. Больше нуля — фиксированный payload этого размера.
	MessageBytes int
	// MetricsHold — сколько держать процесс после Flush, чтобы Prometheus
	// успел снять метрики (scrape_interval обычно 5s). 0 — не ждать.
	MetricsHold time.Duration
}

func DefaultOptions() *ProducerOptions {
	return &ProducerOptions{
		Topic:           "load-test",
		MessageCount:    100_000,
		MessageTemplate: "привет - я сообщение #%d",
		MetricsHold:     20 * time.Second,
	}
}

// Validate проверяет обязательные поля. MessageCount должен быть > 0
// (как в KafkaClientConfig.Validate) — тихое подставление 1 скрывало бы
// ошибки конфига.
func (o *ProducerOptions) Validate() error {
	if o.Topic == "" {
		o.Topic = "load-test"
	}
	if o.MessageCount <= 0 {
		return fmt.Errorf("message_count must be > 0")
	}
	if o.MessageTemplate == "" {
		o.MessageTemplate = "привет - я сообщение #%d"
	}
	if o.MessageBytes < 0 {
		return fmt.Errorf("message_bytes must be >= 0")
	}
	if o.MetricsHold < 0 {
		o.MetricsHold = 20 * time.Second
	}
	return nil
}
