// Package franzgoconsumer содержит реализацию kafka.Consumer на основе
// franz-go (github.com/twmb/franz-go). Пакет не занимается бизнес-логикой —
// только обвязкой над kgo.Client и предоставлением узкого интерфейса
// KafkaClient, чтобы consumer можно было тестировать без реального брокера.
package franzgoconsumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaClientAdapter — тонкая обёртка над *kgo.Client, реализующая интерфейс
// KafkaClient. Нужна исключительно для подмены клиента в тестах: в
// production всегда используется настоящий franz-go client.
//
// Логирование намеренно отсутствует: адаптер не принимает решений и не знает
// контекста вызова — все сообщения об ошибках генерирует вызывающий код
// (BaseConsumer), у которого есть logger с привязкой к consumer-group.
type KafkaClientAdapter struct {
	client *kgo.Client
}

// NewKafkaClientAdapter создаёт адаптер над готовым kgo.Client.
// nil-клиент допустим только в тестах, где методы не вызываются.
func NewKafkaClientAdapter(client *kgo.Client) *KafkaClientAdapter {
	return &KafkaClientAdapter{client: client}
}

// PollFetches блокируется до появления записей, истечения ctx или ошибки.
// Возвращает kgo.Fetches — набор записей и ошибок по партициям.
func (a *KafkaClientAdapter) PollFetches(ctx context.Context) kgo.Fetches {
	return a.client.PollFetches(ctx)
}

// CommitRecords коммитит только переданные записи. franz-go дедуплицирует
// по (topic, partition) и для каждой партиции коммитит max(offset)+1.
// Поэтому BaseConsumer передаёт только непрерывный префикс успешных записей
// без «дыр» — иначе failed offset будет перепрыгнут и сообщение потеряется.
func (a *KafkaClientAdapter) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return a.client.CommitRecords(ctx, records...)
}

// Close освобождает ресурсы kgo.Client. Идемпотентен.
func (a *KafkaClientAdapter) Close() {
	a.client.Close()
}
