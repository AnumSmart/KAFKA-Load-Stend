package franzgoconsumer

import (
	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ToKafkaMessage конвертирует kgo.Record в доменную kafka.Message.
//
// Особенности:
//   - Headers не аллоцируются, если их нет: на нагрузочном стенде 100k × 3
//     группы = 300k пустых map, которых можно избежать.
//   - Дубликаты ключей заголовков теряются (Kafka допускает несколько
//     заголовков с одинаковым ключом, map — нет). Для нагрузочного стенда
//     это не имеет значения, но при развитии в сторону реальной обработки
//     стоит заменить map на []Header.
//   - Timestamp — это create time записи (kgo.Record.Timestamp), а не
//     commit time. Для count handler неважно, но фиксирую явно.
func ToKafkaMessage(record *kgo.Record) *kafka.Message {
	var headers map[string][]byte
	if len(record.Headers) > 0 {
		headers = make(map[string][]byte, len(record.Headers))
		for _, h := range record.Headers {
			headers[h.Key] = h.Value
		}
	}

	return &kafka.Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Timestamp: record.Timestamp,
		Key:       record.Key,
		Value:     record.Value,
		Headers:   headers,
	}
}
