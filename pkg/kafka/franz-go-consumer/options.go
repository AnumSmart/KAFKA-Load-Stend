package franzgoconsumer

import "time"

// ConsumerOptions — настройки BaseConsumer, не связанные с kgo.Client.
// Параметры самого клиента (брокеры, group.id, auto-commit и т.п.) задаются
// через pkg/configs.KafkaClientConfig.ToKgoConsumerOptions.
type ConsumerOptions struct {
	// StatsPrintInterval — как часто печатать Info-лог со статистикой.
	// При коротких прогонах (100k сообщений) лог может не напечататься ни разу;
	// финальная статистика всегда выводится в waitForCompletion/Shutdown.
	StatsPrintInterval time.Duration

	// ShutdownTimeout — максимальное время ожидания завершения текущего
	// батча при shutdown. Согласован с GracefulShutdownTimeout приложения (30s).
	// Если батч не уложился, waitForCompletion возвращает ошибку (необработанные
	// записи не коммитятся и придут повторно).
	ShutdownTimeout time.Duration

	// MaxBatchSize — размер чанка, на который разбивается фетч перед вызовом
	// handler.HandleMessage. Фетч целиком НЕ обрезается: все записи из
	// PollFetches будут обработаны чанками по MaxBatchSize.
	MaxBatchSize int64
}

// DefaultOptions возвращает опции по умолчанию, рассчитанные на нагрузочный
// стенд с 100k сообщений и 6 партициями.
func DefaultOptions() *ConsumerOptions {
	return &ConsumerOptions{
		StatsPrintInterval: 10 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		MaxBatchSize:       1000,
	}
}

// Validate нормализует нулевые/отрицательные значения к дефолтным. Не
// возвращает ошибок: для нагрузочного стенда лучше подставить безопасный
// дефолт, чем ронять приложение на старте из-за пропущенного поля в YAML.
func (o *ConsumerOptions) Validate() error {
	if o.StatsPrintInterval <= 0 {
		o.StatsPrintInterval = 10 * time.Second
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 30 * time.Second
	}
	if o.MaxBatchSize <= 0 {
		o.MaxBatchSize = 1000
	}
	return nil
}
