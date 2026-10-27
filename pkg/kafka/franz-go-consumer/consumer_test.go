package franzgoconsumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// silentLogger — логгер, который никуда не пишет. Нужен, чтобы шум от
// Debug/Info/Warn в тестах не засорял вывод `go test`.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stubHandler реализует kafka.MessageHandler.
// failAt = 0 — не падать никогда;
// failAt = N — вернуть ошибку на N-м вызове (1-based). Счётчик вызовов
// увеличивается ДО проверки failAt, поэтому N-й вызов гарантированно падает.
type stubHandler struct {
	count              int
	failAt             int
	onBatchProcessedN  int
	lastBatchSize      int
}

func (h *stubHandler) HandleMessage(_ context.Context, _ *kafka.Message) error {
	h.count++
	if h.failAt > 0 && h.count == h.failAt {
		return errors.New("forced handler error")
	}
	return nil
}

func (h *stubHandler) OnBatchProcessed(n int) {
	h.onBatchProcessedN++
	h.lastBatchSize = n
}

// mockKafkaClient — управляемая подмена KafkaClient для тестов.
// Запоминает, какие записи были закоммичены через CommitRecords,
// и позволяет задать ошибку коммита.
type mockKafkaClient struct {
	fetches   kgo.Fetches
	committed []*kgo.Record
	commitErr error
	closed    bool
}

func (m *mockKafkaClient) PollFetches(context.Context) kgo.Fetches { return m.fetches }

func (m *mockKafkaClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	m.committed = append(m.committed, records...)
	return m.commitErr
}

func (m *mockKafkaClient) Close() { m.closed = true }

func maxCommittedOffset(records []*kgo.Record, partition int32) int64 {
	max := int64(-1)
	for _, r := range records {
		if r.Partition == partition && r.Offset > max {
			max = r.Offset
		}
	}
	return max
}

// Проверяем, что конструктор отклоняет nil-зависимости. Это защита от
// ошибок DI: без неё BaseConsumer упал бы с nil pointer в первом PollFetches.
func TestNewBaseConsumerRequiresDeps(t *testing.T) {
	logger := silentLogger()
	if _, err := NewBaseConsumer(nil, &stubHandler{}, DefaultOptions(), logger); err == nil {
		t.Fatal("expected client error")
	}
	if _, err := NewBaseConsumer(&KafkaClientAdapter{}, nil, DefaultOptions(), logger); err == nil {
		t.Fatal("expected handler error")
	}
}

// Проверяем, что конвертер корректно переносит поля из kgo.Record
// и что handler вызывается ровно один раз.
func TestToKafkaMessageAndHandler(t *testing.T) {
	h := &stubHandler{}
	msg := ToKafkaMessage(&kgo.Record{
		Topic: "load-test",
		Value: []byte("привет - я сообщение #1"),
	})
	if err := h.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if h.count != 1 {
		t.Fatalf("count = %d", h.count)
	}
}

// Регрессия: CommitRecords у franz-go коммитит max(offset)+1 по партиции.
// При ошибке handler'а нельзя коммитить записи «после» failed offset —
// иначе сообщение будет потеряно. Блокировка партиции действует на весь фетч
// (все чанки MaxBatchSize), а не только на текущий чанк.
//
// Сценарий: фетч 2500 записей одной партиции, MaxBatchSize=1000.
// Handler падает на 1500-й записи (offset 1499). Ожидаем:
//   - обработаны и закоммичены только offset 0..1498 (непрерывный префикс);
//   - max committed offset = 1498 (next offset на брокере был бы 1499);
//   - записи 1500..2499 не обрабатывались (партиция заблокирована до конца фетча).
func TestProcessRecordsCommitsContiguousPrefixOnly(t *testing.T) {
	records := make([]*kgo.Record, 2500)
	for i := range records {
		records[i] = &kgo.Record{
			Topic:     "load-test",
			Partition: 0,
			Offset:    int64(i),
			Value:     []byte("x"),
		}
	}

	client := &mockKafkaClient{}
	handler := &stubHandler{failAt: 1500}
	opts := DefaultOptions()
	opts.MaxBatchSize = 1000

	bc, err := NewBaseConsumer(client, handler, opts, silentLogger())
	if err != nil {
		t.Fatal(err)
	}

	bc.processRecords(context.Background(), records)

	if got := bc.messagesProcessed.Load(); got != 1499 {
		t.Fatalf("messagesProcessed = %d, want 1499", got)
	}
	if got := len(client.committed); got != 1499 {
		t.Fatalf("committed = %d, want 1499", got)
	}
	if max := maxCommittedOffset(client.committed, 0); max != 1498 {
		t.Fatalf("max committed offset = %d, want 1498", max)
	}
	for _, r := range client.committed {
		if r.Partition == 0 && r.Offset >= 1499 {
			t.Fatalf("offset %d must NOT be committed", r.Offset)
		}
	}
	if handler.onBatchProcessedN == 0 {
		t.Fatal("expected OnBatchProcessed after successful commits")
	}
}

// При ошибке CommitRecords OnBatchProcessed не должен вызываться —
// метрики батча отражают только зафиксированный прогресс.
func TestProcessBatchSkipsOnBatchProcessedWhenCommitFails(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "load-test", Partition: 0, Offset: 0, Value: []byte("a")},
		{Topic: "load-test", Partition: 0, Offset: 1, Value: []byte("b")},
	}
	client := &mockKafkaClient{commitErr: errors.New("commit failed")}
	handler := &stubHandler{}
	bc, err := NewBaseConsumer(client, handler, DefaultOptions(), silentLogger())
	if err != nil {
		t.Fatal(err)
	}

	bc.processBatch(context.Background(), records, nil)

	if bc.messagesProcessed.Load() != 0 {
		t.Fatalf("messagesProcessed = %d, want 0 after failed commit", bc.messagesProcessed.Load())
	}
	if handler.onBatchProcessedN != 0 {
		t.Fatalf("OnBatchProcessed called %d times, want 0", handler.onBatchProcessedN)
	}
}

// Проверяем, что при установленном isShuttingDown consumer не обрабатывает
// ни одной записи и ничего не коммитит. Это защищает от гонки «Shutdown
// выставлен, но процесс ещё успевает обработать чанк».
func TestProcessRecordsRespectsShutdown(t *testing.T) {
	records := make([]*kgo.Record, 2500)
	for i := range records {
		records[i] = &kgo.Record{
			Topic:     "load-test",
			Partition: 0,
			Offset:    int64(i),
			Value:     []byte("x"),
		}
	}

	client := &mockKafkaClient{}
	handler := &stubHandler{}
	opts := DefaultOptions()
	opts.MaxBatchSize = 1000

	bc, err := NewBaseConsumer(client, handler, opts, silentLogger())
	if err != nil {
		t.Fatal(err)
	}

	bc.isShuttingDown.Store(true)
	bc.processRecords(context.Background(), records)

	if got := bc.messagesProcessed.Load(); got != 0 {
		t.Fatalf("messagesProcessed = %d, want 0", got)
	}
	if got := len(client.committed); got != 0 {
		t.Fatalf("committed = %d, want 0", got)
	}
}

// Shutdown не закрывает клиент — lifetime принадлежит DI.
func TestShutdownDoesNotCloseClient(t *testing.T) {
	client := &mockKafkaClient{}
	bc, err := NewBaseConsumer(client, &stubHandler{}, DefaultOptions(), silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	bc.Shutdown()
	if client.closed {
		t.Fatal("Shutdown must not close Kafka client")
	}
}

// Пустой слайс не должен приводить к коммитам или панике.
func TestProcessRecordsEmpty(t *testing.T) {
	client := &mockKafkaClient{}
	bc, err := NewBaseConsumer(client, &stubHandler{}, DefaultOptions(), silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	bc.processRecords(context.Background(), nil)
	if len(client.committed) != 0 {
		t.Fatal("expected no commits")
	}
}
