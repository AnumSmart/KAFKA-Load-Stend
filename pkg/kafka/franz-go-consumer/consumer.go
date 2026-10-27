package franzgoconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Проверка на этапе компиляции, что BaseConsumer реализует kafka.Consumer.
// Если интерфейс в pkg/kafka изменится, ошибка будет видна сразу при сборке,
// а не в DI-контейнере в рантайме.
var _ kafka.Consumer = (*BaseConsumer)(nil)

// partKey идентифицирует партицию внутри батча. Нужен, чтобы при ошибке
// handler'а не коммитить «дыру»: franz-go CommitRecords для партиции берёт
// максимальный offset среди переданных записей и коммитит offset+1.
type partKey struct {
	topic     string
	partition int32
}

// BaseConsumer — батчевый консьюмер без DLQ (dead-letter queue).
//
// Гарантии доставки: at-least-once. Каждый фетч разбивается на чанки размером
// не более MaxBatchSize; каждый чанк обрабатывается отдельно, и в Kafka
// коммитятся только непрерывные префиксы успешно обработанных offset'ов по
// каждой партиции (CommitRecords). После первой ошибки handler'а на партиции
// оставшиеся записи этой партиции в чанке не обрабатываются и не коммитятся —
// при рестарте группы они будут получены повторно.
//
// Auto-commit на kgo.Client должен быть выключен (см. ToKgoConsumerOptions
// в pkg/configs). Иначе franz-go сам закоммитит все оффсеты, полученные из
// PollFetches, включая необработанные — это тихая потеря данных.
//
// Поля:
//   - recordPool — переиспользуемый буфер под слайс записей; сокращает
//     аллокации при частых фетчах.
//   - statsPrintInterval/lastStatsTime — троттлинг Info-логов статистики,
//     чтобы не засорять вывод при коротких прогонах (100k сообщений — секунды).
//   - messagesProcessed — счётчик успешно закоммиченных сообщений (для GetStats);
//     увеличивается только после успешного CommitRecords.
//   - currentBatch — сколько записей из текущего фетча ещё не обработано;
//     используется waitForCompletion, чтобы не закрывать клиент, пока идёт
//     обработка. Значение 0 = «нет работы в полёте».
//   - isShuttingDown — флаг кооперативной остановки: Start проверяет его между
//     итерациями, а processRecords — между чанками.
//   - shutdownOnce — идемпотентность Shutdown (клиент закрывает DI, не Shutdown).
type BaseConsumer struct {
	recordPool         sync.Pool
	statsPrintInterval time.Duration
	lastStatsTime      time.Time
	messagesProcessed  atomic.Int64
	currentBatch       atomic.Int64
	client             KafkaClient
	handler            kafka.MessageHandler
	logger             *slog.Logger
	options            *ConsumerOptions
	isShuttingDown     atomic.Bool
	shutdownOnce       sync.Once
}

// NewBaseConsumer создаёт консьюмер. Все зависимости обязательны, кроме
// options (при nil берутся DefaultOptions) и logger (при nil — slog.Default).
//
// Валидация options на этом шаге гарантирует, что остальной код BaseConsumer
// может не проверять нулевые значения MaxBatchSize/ShutdownTimeout.
func NewBaseConsumer(client KafkaClient, handler kafka.MessageHandler, opts *ConsumerOptions, logger *slog.Logger) (*BaseConsumer, error) {
	if client == nil {
		return nil, ErrClientNotInitialized
	}
	if handler == nil {
		return nil, ErrHandlerNotInitialized
	}
	if opts == nil {
		opts = DefaultOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer options: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}

	bc := &BaseConsumer{
		client:             client,
		handler:            handler,
		options:            opts,
		logger:             logger,
		statsPrintInterval: opts.StatsPrintInterval,
		lastStatsTime:      time.Now(),
	}

	// sync.Pool для слайса записей: capacity фиксирована по MaxBatchSize,
	// чтобы при последующих Get() не было реаллокаций на типичном чанке.
	// Внутри processFetches слайс всё равно режется [:0] — это безопасно.
	bc.recordPool = sync.Pool{
		New: func() any {
			capacity := bc.options.MaxBatchSize
			if capacity <= 0 {
				capacity = 1000
			}
			s := make([]*kgo.Record, 0, capacity)
			return &s
		},
	}

	bc.logger.Info("consumer initialized",
		"max_batch_size", opts.MaxBatchSize,
		"stats_print_interval", opts.StatsPrintInterval.String(),
		"shutdown_timeout", opts.ShutdownTimeout.String(),
	)

	return bc, nil
}

// Start — блокирующий цикл поллинга. Возвращает nil после graceful shutdown
// (по ctx.Done() или после вызова Shutdown из другой горутины). Ошибки
// обработки отдельных сообщений не прерывают цикл: они логируются, а
// соответствующая партиция в чанке не продвигается (at-least-once).
func (bc *BaseConsumer) Start(ctx context.Context) error {
	bc.logger.Info("starting consumer loop",
		"max_batch_size", bc.options.MaxBatchSize,
	)

	for {
		if bc.isShuttingDown.Load() {
			return bc.waitForCompletion()
		}

		select {
		case <-ctx.Done():
			bc.logger.Info("shutdown signal received via context",
				"processed", bc.messagesProcessed.Load(),
			)
			bc.isShuttingDown.Store(true)
			return bc.waitForCompletion()
		default:
			bc.pollAndProcessMessages(ctx)
			if bc.isShuttingDown.Load() {
				return bc.waitForCompletion()
			}
			bc.printStatsIfNeeded()
		}
	}
}

// waitForCompletion ждёт, пока currentBatch обнулится (обработка завершена)
// либо истечёт ShutdownTimeout. При таймауте возвращает ошибку, чтобы
// вызывающий мог отличить чистое завершение от принудительного обрыва.
func (bc *BaseConsumer) waitForCompletion() error {
	timeout := bc.options.ShutdownTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	start := time.Now()
	for {
		if bc.currentBatch.Load() == 0 {
			bc.logger.Info("all pending messages processed",
				"processed", bc.messagesProcessed.Load(),
				"wait_duration", time.Since(start).String(),
			)
			return nil
		}
		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				pending := bc.currentBatch.Load()
				bc.logger.Warn("shutdown timeout, forcing stop",
					"pending", pending,
					"processed", bc.messagesProcessed.Load(),
				)
				return fmt.Errorf("shutdown timeout with %d pending records", pending)
			}
		}
	}
}

// pollAndProcessMessages — одна итерация: получить фетч и обработать его.
// Внутренний контекст с таймаутом ограничивает время блокировки на PollFetches,
// чтобы Start мог реагировать на ctx.Done() без задержек на пустом топике.
func (bc *BaseConsumer) pollAndProcessMessages(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	// 5 секунд — компромисс: пустой топик не блокирует Start навсегда,
	// но и не создаёт busy-loop при отсутствии сообщений.
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fetches := bc.client.PollFetches(pollCtx)
	if fetches.Empty() {
		return
	}

	// Ошибки уровня фетча (ошибки соединения, таймауты брокера) —
	// не фатальны: логируем и продолжаем, следующая итерация попробует снова.
	// context.DeadlineExceeded/Canceled при штатном завершении — шум, пропускаем.
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, fetchErr := range errs {
			if fetchErr.Err == context.DeadlineExceeded || fetchErr.Err == context.Canceled {
				continue
			}
			bc.logger.Error("fetch error",
				"error", fetchErr.Err,
				"topic", fetchErr.Topic,
				"partition", fetchErr.Partition,
			)
		}
	}

	bc.processFetches(ctx, fetches)
}

// processFetches выгружает записи из kgo.Fetches в переиспользуемый слайс и
// передаёт его в processRecords. Слайс берётся из recordPool и возвращается
// обратно — при частых фетчах это экономит аллокации.
func (bc *BaseConsumer) processFetches(ctx context.Context, fetches kgo.Fetches) {
	recordsPtr := bc.recordPool.Get().(*[]*kgo.Record)
	records := (*recordsPtr)[:0]
	defer func() {
		// Обнуляем элементы и возвращаем слайс с len=0, чтобы пул не
		// удерживал ссылки на *kgo.Record (и связанные буферы) до GC.
		for i := range records {
			records[i] = nil
		}
		*recordsPtr = records[:0]
		bc.recordPool.Put(recordsPtr)
	}()

	iter := fetches.RecordIter()
	for !iter.Done() {
		records = append(records, iter.Next())
	}

	bc.processRecords(ctx, records)
}

// processRecords обрабатывает весь набор записей чанками по MaxBatchSize.
//
// Вынесено в отдельный метод (а не сделано внутри processFetches), чтобы
// юнит-тесты могли проверять логику батчевой обработки и коммитов без
// конструирования kgo.Fetches — достаточно передать слайс *kgo.Record.
//
// currentBatch выставляется в размер всего набора в начале и обновляется
// после каждого чанка, чтобы waitForCompletion видел реальный прогресс.
// В конце (в том числе при раннем return по shutdown) сбрасывается в 0:
// остаток необработанных записей не должен блокировать закрытие клиента.
func (bc *BaseConsumer) processRecords(ctx context.Context, records []*kgo.Record) {
	total := len(records)
	if total == 0 {
		return
	}

	maxBatch := int(bc.options.MaxBatchSize)
	if maxBatch <= 0 {
		maxBatch = 1000
	}

	batches := (total + maxBatch - 1) / maxBatch
	bc.logger.Debug("processing fetch",
		"total_records", total,
		"max_batch_size", maxBatch,
		"batches", batches,
	)

	bc.currentBatch.Store(int64(total))
	defer bc.currentBatch.Store(0)

	// blocked живёт на весь фетч: ошибка в чанке N не должна позволить
	// чанку N+1 закоммитить более поздние offset'ы той же партиции.
	blocked := make(map[partKey]struct{})

	for i := 0; i < total; i += maxBatch {
		if bc.isShuttingDown.Load() {
			// Часть записей фетча не будет обработана в этом прогоне.
			// Она не закоммичена — при рестарте группы придёт повторно.
			// Это ожидаемое поведение at-least-once.
			bc.logger.Warn("shutdown before next batch, remaining records not processed",
				"processed_offset", i,
				"remaining", total-i,
				"total", total,
			)
			return
		}

		end := min(i+maxBatch, total)
		bc.processBatch(ctx, records[i:end], blocked)
		bc.currentBatch.Store(int64(total - end))
	}
}

// processBatch обрабатывает один чанк, коммитит непрерывный префикс успешных
// записей по каждой партиции и уведомляет handler о размере закоммиченного батча.
//
// Ключевые инварианты:
//   - После ошибки handler'а на (topic, partition) оставшиеся записи этой
//     партиции в чанке не обрабатываются и не попадают в CommitRecords.
//     Иначе franz-go закоммитил бы max(offset)+1 и «перепрыгнул» failed offset.
//   - Если successfully processed пуст, коммит не выполняется и
//     OnBatchProcessed не вызывается.
//   - OnBatchProcessed и messagesProcessed обновляются только после
//     успешного CommitRecords (метрики = закоммиченный прогресс).
//   - Коммит идёт с context.WithoutCancel + timeout 30s, чтобы отмена
//     родительского ctx при shutdown не срывала фиксацию уже обработанного.
func (bc *BaseConsumer) processBatch(ctx context.Context, records []*kgo.Record, blocked map[partKey]struct{}) {
	if len(records) == 0 {
		return
	}
	if blocked == nil {
		blocked = make(map[partKey]struct{})
	}

	processed := make([]*kgo.Record, 0, len(records))

	for i, record := range records {
		if bc.isShuttingDown.Load() {
			bc.logger.Warn("stopping batch due to shutdown",
				"processed", len(processed),
				"remaining", len(records)-i,
			)
			break
		}

		key := partKey{topic: record.Topic, partition: record.Partition}
		if _, skip := blocked[key]; skip {
			continue
		}

		msg := ToKafkaMessage(record)
		if err := bc.processMessageSafely(ctx, msg); err != nil {
			bc.logger.Error("failed to process message",
				"error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
			// Блокируем партицию до конца фетча: последующие чанки не должны
			// коммитить более поздние offset'ы — иначе CommitRecords
			// зафиксировал бы max offset и потерял бы failed запись.
			blocked[key] = struct{}{}
			continue
		}

		processed = append(processed, record)
	}

	if len(processed) == 0 {
		bc.logger.Warn("batch produced no successfully processed records",
			"batch_size", len(records),
			"blocked_partitions", len(blocked),
		)
		return
	}

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	if err := bc.client.CommitRecords(commitCtx, processed...); err != nil {
		bc.logger.Error("failed to commit offsets",
			"error", err,
			"count", len(processed),
			"batch_size", len(records),
		)
		return
	}

	bc.messagesProcessed.Add(int64(len(processed)))
	bc.logger.Debug("batch committed",
		"count", len(processed),
		"batch_size", len(records),
	)
	bc.handler.OnBatchProcessed(len(processed))
}

// processMessageSafely вызывает handler с защитой от паник. Паника в handler'е
// не должна убивать весь consumer-loop: логируем как ошибку, запись не
// коммитится, работа продолжается.
func (bc *BaseConsumer) processMessageSafely(ctx context.Context, msg *kafka.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in handler: %v", r)
		}
	}()
	return bc.handler.HandleMessage(ctx, msg)
}

// Shutdown останавливает цикл обработки и ждёт завершения текущего батча.
// Идемпотентен. Клиент НЕ закрывается здесь — lifetime *kgo.Client принадлежит
// DI-контейнеру (client.Close после Shutdown). Вызывающий должен сначала
// отменить ctx у Start и дождаться выхода Start, затем вызвать Shutdown.
func (bc *BaseConsumer) Shutdown() {
	bc.shutdownOnce.Do(func() {
		bc.logger.Info("shutting down consumer")
		bc.isShuttingDown.Store(true)
		if err := bc.waitForCompletion(); err != nil {
			bc.logger.Warn("shutdown wait incomplete", "error", err)
		}
		bc.logger.Info("consumer shutdown completed",
			"processed", bc.messagesProcessed.Load(),
		)
	})
}

// GetStats возвращает число успешно закоммиченных сообщений с момента старта.
func (bc *BaseConsumer) GetStats() (processed int64) {
	return bc.messagesProcessed.Load()
}

// printStatsIfNeeded печатает статистику не чаще statsPrintInterval.
// Нужно, чтобы при долгом прогоне был виден прогресс, но при коротком
// (например, 100k сообщений за пару секунд) не засорять лог.
func (bc *BaseConsumer) printStatsIfNeeded() {
	if time.Since(bc.lastStatsTime) < bc.statsPrintInterval {
		return
	}
	bc.logger.Info("consumer statistics",
		"processed", bc.messagesProcessed.Load(),
		"pending", bc.currentBatch.Load(),
	)
	bc.lastStatsTime = time.Now()
}
