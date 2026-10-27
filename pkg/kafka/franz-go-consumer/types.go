package franzgoconsumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaClient — зависимость консьюмера от Kafka-клиента. Интерфейс намеренно
// узкий: BaseConsumer не должен уметь создавать топики, читать метаданные
// или управлять группами — только получать записи и коммитить оффсеты.
//
// Реализация — KafkaClientAdapter над *kgo.Client. В тестах подменяется
// mock-реализацией, что позволяет проверять логику батчевой обработки и
// коммитов без реального брокера.
//
// CommitUncommittedOffsets намеренно отсутствует: он коммитит все оффсеты
// последнего фетча, включая необработанные, и ломает at-least-once.
type KafkaClient interface {
	// PollFetches блокируется до появления записей или истечения ctx.
	PollFetches(ctx context.Context) kgo.Fetches

	// CommitRecords коммитит переданные записи. У franz-go для каждой
	// партиции берётся максимальный offset среди аргументов и коммитится
	// offset+1 — поэтому вызывающий обязан передавать только непрерывный
	// префикс успешно обработанных записей без «дыр».
	CommitRecords(ctx context.Context, records ...*kgo.Record) error

	// Close освобождает ресурсы клиента. Идемпотентен.
	Close()
}
