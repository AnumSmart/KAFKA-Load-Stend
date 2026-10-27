package configs

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMessageTemplate       = "привет - я сообщение #%d"
	defaultMetricsAddr           = ":8081"
	defaultConsumerMetricsAddr   = ":8082"
	defaultTopic                 = "load-test"
)

// KafkaClientConfig — общий конфиг Kafka-клиента для продюсера и консьюмера.
// Один и тот же тип используется в обоих приложениях: продюсер читает только
// блок Producer, консьюмер — только блок Consumer. Поля Topic/Brokers общие.
type KafkaClientConfig struct {
	Brokers  []string       `yaml:"brokers"`
	Topic    string         `yaml:"topic"`
	Producer ProducerConfig `yaml:"producer"`
	Consumer ConsumerConfig `yaml:"consumer"`
}

// ProducerConfig — параметры нагрузочного продюсера.
//
// DisableIdempotency и RequiredAcks по умолчанию настроены на максимальный
// throughput в ущерб надёжности. Это осознанное решение для нагрузочного
// стенда: НЕ является production-семантикой.
type ProducerConfig struct {
	Linger             Duration `yaml:"linger"`
	BatchMaxBytes      int32    `yaml:"batch_max_bytes"`
	Compression        string   `yaml:"compression"`
	MaxBufferedRecords int      `yaml:"max_buffered_records"`
	RequiredAcks       string   `yaml:"required_acks"`
	DisableIdempotency bool     `yaml:"disable_idempotency"`
	MessageCount       int64    `yaml:"message_count"`
	MessageTemplate    string   `yaml:"message_template"`
	MetricsAddr        string   `yaml:"metrics_addr"`
	// MetricsHold — пауза после Flush, чтобы Prometheus успел снять метрики.
	MetricsHold Duration `yaml:"metrics_hold"`
}

// ConsumerConfig — параметры консьюмера.
//
// Auto-commit всегда выключен (см. ToKgoConsumerOptions). Коммит выполняется
// вручную в BaseConsumer через CommitRecords: это единственный способ
// гарантировать, что необработанные записи не будут закоммичены при фетче,
// размер которого превышает MaxBatchSize. Поля DisableAutoCommit и
// CommitInterval, которые были здесь раньше, удалены намеренно — иначе
// пользователь мог бы выставить disable_auto_commit: false в YAML и получить
// тихую потерю данных.
type ConsumerConfig struct {
	GroupID          string   `yaml:"group_id"`
	StartOffset      string   `yaml:"start_offset"`
	MaxBatchSize     int64    `yaml:"max_batch_size"`
	SessionTimeout   Duration `yaml:"session_timeout"`
	RebalanceTimeout Duration `yaml:"rebalance_timeout"`
	MetricsAddr      string   `yaml:"metrics_addr"`
}

// DefaultKafkaClientConfig возвращает конфиг по умолчанию, соответствующий
// docker-compose стенду: 3 брокера, топик load-test, 100 000 сообщений
// (целевой объём прогона, см. docs/experiments.md).
// Переопределяется через YAML и env-переменные.
func DefaultKafkaClientConfig() *KafkaClientConfig {
	return &KafkaClientConfig{
		Brokers: []string{"localhost:9092", "localhost:9094", "localhost:9096"},
		Topic:   defaultTopic,
		Producer: ProducerConfig{
			Linger:             Duration(50 * time.Millisecond),
			BatchMaxBytes:      1 << 20,
			Compression:        "snappy",
			MaxBufferedRecords: 1_000_000,
			RequiredAcks:       "leader",
			DisableIdempotency: true,
			MessageCount:       100_000,
			MessageTemplate:    defaultMessageTemplate,
			MetricsAddr:        defaultMetricsAddr,
			MetricsHold:        Duration(20 * time.Second),
		},
		Consumer: ConsumerConfig{
			GroupID:          "load-test-group",
			// earliest: при новой группе не теряем сообщения, если продюсер
			// стартовал до завершения join (в отличие от latest + sleep).
			StartOffset:      "earliest",
			MaxBatchSize:     1000,
			SessionTimeout:   Duration(30 * time.Second),
			RebalanceTimeout: Duration(60 * time.Second),
			MetricsAddr:      defaultConsumerMetricsAddr,
		},
	}
}

// LoadKafkaClientConfig — точка входа для приложений. Порядок применения
// источников: сначала YAML (или дефолт, если файла нет), затем env-переменные,
// затем валидация. Env перебивает YAML — это удобно для экспериментов
// (MESSAGE_COUNT=10000 make producer) без правки конфигов.
//
// Ошибки парсинга env-переменных НЕ глотаются: Apply*EnvOverrides возвращают
// error, а LoadKafkaClientConfig прокидывает его наружу. Это защищает
// эксперимент от тихого применения дефолтов при опечатке (например,
// PRODUCER_LINGER=100msx или PRODUCER_DISABLE_IDEMPOTENCY=maybe): приложение
// упадёт с понятным сообщением, а не отправит другое число сообщений.
func LoadKafkaClientConfig(configPath string) (*KafkaClientConfig, error) {
	cfg, err := LoadYAMLConfig(configPath, DefaultKafkaClientConfig)
	if err != nil {
		return nil, err
	}
	if err := ApplyCommonEnvOverrides(cfg); err != nil {
		return nil, err
	}
	if err := ApplyProducerEnvOverrides(cfg); err != nil {
		return nil, err
	}
	if err := ApplyConsumerEnvOverrides(cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ApplyCommonEnvOverrides применяет env-переменные, общие для продюсера
// и консьюмера: KAFKA_BROKERS, KAFKA_TOPIC.
//
// Возвращает error ради симметрии с остальными Apply*-функциями. Текущая
// реализация никогда не падает: и список брокеров, и топик — свободные
// строки, которые валидируются позже в Validate.
func ApplyCommonEnvOverrides(cfg *KafkaClientConfig) error {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		var brokers []string
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				brokers = append(brokers, part)
			}
		}
		if len(brokers) > 0 {
			cfg.Brokers = brokers
		}
	}
	if v := os.Getenv("KAFKA_TOPIC"); v != "" {
		cfg.Topic = v
	}
	return nil
}

// ApplyProducerEnvOverrides применяет env-переменные продюсера.
//
// Возвращает error при ошибке парсинга хотя бы одной переменной — это
// критично для экспериментов: раньше опечатка (PRODUCER_LINGER=100msx,
// PRODUCER_DISABLE_IDEMPOTENCY=TRUE) молча оставляла дефолт, и прогон
// сравнивал не те параметры. Ошибки накапливаются через errors.Join,
// чтобы за один запуск было видно сразу все битые переменные.
//
// Поля RequiredAcks и Compression здесь только присваиваются как строки —
// их валидация выполняется в Validate через ParseRequiredAcks/ParseCompression.
func ApplyProducerEnvOverrides(cfg *KafkaClientConfig) error {
	var errs []error

	if v := os.Getenv("PRODUCER_LINGER"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("PRODUCER_LINGER=%q: %w", v, err))
		} else {
			cfg.Producer.Linger = Duration(d)
		}
	}
	if v := os.Getenv("PRODUCER_BATCH_MAX_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			errs = append(errs, fmt.Errorf("PRODUCER_BATCH_MAX_BYTES=%q: %w", v, err))
		} else {
			cfg.Producer.BatchMaxBytes = int32(n)
		}
	}
	if v := os.Getenv("PRODUCER_REQUIRED_ACKS"); v != "" {
		cfg.Producer.RequiredAcks = v
	}
	if v := os.Getenv("PRODUCER_DISABLE_IDEMPOTENCY"); v != "" {
		// strconv.ParseBool принимает "1", "t", "T", "TRUE", "true", "True"
		// и симметричный набор для false. Раньше проверка была жёстко
		// v == "true" || v == "1" — "TRUE"/"True" тихо игнорировались.
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("PRODUCER_DISABLE_IDEMPOTENCY=%q: %w", v, err))
		} else {
			cfg.Producer.DisableIdempotency = b
		}
	}
	if v := os.Getenv("MESSAGE_COUNT"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("MESSAGE_COUNT=%q: %w", v, err))
		} else {
			cfg.Producer.MessageCount = n
		}
	}
	if v := os.Getenv("PRODUCER_COMPRESSION"); v != "" {
		cfg.Producer.Compression = v
	}
	if v := os.Getenv("PRODUCER_METRICS_ADDR"); v != "" {
		cfg.Producer.MetricsAddr = v
	}
	if v := os.Getenv("PRODUCER_MAX_BUFFERED_RECORDS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("PRODUCER_MAX_BUFFERED_RECORDS=%q: %w", v, err))
		} else {
			cfg.Producer.MaxBufferedRecords = n
		}
	}
	if v := os.Getenv("PRODUCER_METRICS_HOLD"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("PRODUCER_METRICS_HOLD=%q: %w", v, err))
		} else {
			cfg.Producer.MetricsHold = Duration(d)
		}
	}

	return errors.Join(errs...)
}

// ApplyConsumerEnvOverrides применяет env-переменные консьюмера.
// CONSUMER_GROUP_ID задаёт имя группы (по умолчанию load-test-group);
// через него же удобно менять группу у локальных/docker-реплик одного бинаря.
//
// Возвращает error ради симметрии: обе переменные — свободные строки,
// валидация GroupID/StartOffset выполняется в Validate.
func ApplyConsumerEnvOverrides(cfg *KafkaClientConfig) error {
	if v := os.Getenv("CONSUMER_GROUP_ID"); v != "" {
		cfg.Consumer.GroupID = v
	}
	if v := os.Getenv("CONSUMER_START_OFFSET"); v != "" {
		cfg.Consumer.StartOffset = v
	}
	if v := os.Getenv("CONSUMER_METRICS_ADDR"); v != "" {
		cfg.Consumer.MetricsAddr = v
	}
	return nil
}

// Validate нормализует конфиг и возвращает ошибку при некорректных значениях,
// которые нельзя безопасно подставить дефолтом (пустой список брокеров,
// невалидный required_acks и т.п.). Часть полей (MessageTemplate, MetricsAddr,
// GroupID, MaxBatchSize) при нулевом значении подставляются дефолтами — это
// осознанно: пропуск поля в YAML не должен ронять стенд.
func (c *KafkaClientConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers list cannot be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.Producer.MessageCount <= 0 {
		return fmt.Errorf("message_count must be > 0")
	}
	if c.Producer.BatchMaxBytes <= 0 {
		return fmt.Errorf("batch_max_bytes must be > 0")
	}
	if c.Producer.MaxBufferedRecords <= 0 {
		return fmt.Errorf("max_buffered_records must be > 0")
	}
	if c.Producer.MessageTemplate == "" {
		c.Producer.MessageTemplate = defaultMessageTemplate
	}
	if c.Producer.MetricsAddr == "" {
		c.Producer.MetricsAddr = defaultMetricsAddr
	}
	if _, err := ParseRequiredAcks(c.Producer.RequiredAcks); err != nil {
		return err
	}
	if _, err := ParseCompression(c.Producer.Compression); err != nil {
		return err
	}
	if c.Consumer.GroupID == "" {
		c.Consumer.GroupID = "load-test-group"
	}
	if c.Consumer.MaxBatchSize <= 0 {
		c.Consumer.MaxBatchSize = 1000
	}
	if c.Consumer.MetricsAddr == "" {
		c.Consumer.MetricsAddr = defaultConsumerMetricsAddr
	}
	if c.Consumer.SessionTimeout.Duration() <= 0 {
		c.Consumer.SessionTimeout = Duration(30 * time.Second)
	}
	if c.Consumer.RebalanceTimeout.Duration() <= 0 {
		c.Consumer.RebalanceTimeout = Duration(60 * time.Second)
	}
	if c.Producer.MetricsHold < 0 {
		c.Producer.MetricsHold = Duration(20 * time.Second)
	}
	if _, err := ParseStartOffset(c.Consumer.StartOffset); err != nil {
		return err
	}
	return nil
}

// ToKgoProducerOptions конвертирует секцию Producer в набор kgo.Opt.
// Возвращает ошибку при невалидном RequiredAcks/Compression — Validate уже
// проверяла их, но повторная проверка нужна для сценария прямого вызова.
func (c *KafkaClientConfig) ToKgoProducerOptions() ([]kgo.Opt, error) {
	acks, err := ParseRequiredAcks(c.Producer.RequiredAcks)
	if err != nil {
		return nil, err
	}
	comp, err := ParseCompression(c.Producer.Compression)
	if err != nil {
		return nil, err
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.DefaultProduceTopic(c.Topic),
		kgo.RequiredAcks(acks),
		kgo.ProducerBatchMaxBytes(c.Producer.BatchMaxBytes),
		kgo.ProducerLinger(c.Producer.Linger.Duration()),
		kgo.MaxBufferedRecords(c.Producer.MaxBufferedRecords),
		kgo.ProducerBatchCompression(comp),
	}
	if c.Producer.DisableIdempotency {
		// Отключение идемпотентности снимает ограничение на количество
		// in-flight запросов и ускоряет прогон, но допускает дубликаты
		// при ретраях. Для нагрузочного стенда — приемлемо.
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	return opts, nil
}

// ToKgoConsumerOptions конвертирует секцию Consumer в набор kgo.Opt.
//
// Auto-commit ВСЕГДА выключен. Это не опция, а инвариант корректности:
// BaseConsumer коммитит оффсеты вручную через CommitRecords после успешной
// обработки каждого чанка. Включение auto-commit привело бы к коммиту
// записей, которые не попали в обработку (тихая потеря данных), как только
// размер фетча превысит MaxBatchSize.
func (c *KafkaClientConfig) ToKgoConsumerOptions() ([]kgo.Opt, error) {
	offset, err := ParseStartOffset(c.Consumer.StartOffset)
	if err != nil {
		return nil, err
	}

	return []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.Consumer.GroupID),
		kgo.ConsumeTopics(c.Topic),
		kgo.ConsumeResetOffset(offset),
		kgo.SessionTimeout(c.Consumer.SessionTimeout.Duration()),
		kgo.RebalanceTimeout(c.Consumer.RebalanceTimeout.Duration()),
		kgo.DisableAutoCommit(),
	}, nil
}

// ParseRequiredAcks разбирает строковое значение required_acks в kgo.Acks.
// Поддерживает три уровня надёжности: all (-1, все ISR), leader (1, только
// лидер), none (0, без подтверждения). none заметно быстрее, но при падении
// лидера сообщения могут потеряться — только для нагрузочных экспериментов.
func ParseRequiredAcks(value string) (kgo.Acks, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "all", "-1":
		return kgo.AllISRAcks(), nil
	case "leader", "one", "1":
		return kgo.LeaderAck(), nil
	case "none", "0":
		return kgo.NoAck(), nil
	default:
		return kgo.LeaderAck(), fmt.Errorf("invalid required_acks: %s (use leader, all, none)", value)
	}
}

// ParseCompression разбирает строковое имя кодека сжатия в kgo.CompressionCodec.
// Пустая строка трактуется как "none" — так удобнее в YAML (поле можно опустить).
func ParseCompression(value string) (kgo.CompressionCodec, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none":
		return kgo.NoCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	default:
		return kgo.NoCompression(), fmt.Errorf("invalid compression: %s", value)
	}
}

// ParseStartOffset разбирает строковое значение start_offset в kgo.Offset.
// earliest — с начала партиции (дефолт стенда: не теряем сообщения, если
// продюсер успел отправить до завершения join группы).
// latest — только новые сообщения (нужен старт консьюмеров до продюсера).
// Пустая строка трактуется как earliest — как в DefaultKafkaClientConfig.
func ParseStartOffset(value string) (kgo.Offset, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "earliest", "at_start", "":
		return kgo.NewOffset().AtStart(), nil
	case "latest", "at_end":
		return kgo.NewOffset().AtEnd(), nil
	default:
		return kgo.NewOffset().AtStart(), fmt.Errorf("invalid start_offset: %s", value)
	}
}
