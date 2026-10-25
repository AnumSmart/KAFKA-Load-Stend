package configs

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMessageTemplate = "привет - я сообщение #%d"
	defaultMetricsAddr     = ":8081"
	defaultTopic           = "load-test"
)

// KafkaClientConfig — единый конфиг клиента стенда.
type KafkaClientConfig struct {
	Brokers  []string       `yaml:"brokers"`
	Topic    string         `yaml:"topic"`
	Producer ProducerConfig `yaml:"producer"`
	Consumer ConsumerConfig `yaml:"consumer"`
}

// ProducerConfig — параметры нагрузочного продюсера (ТЗ §6.2).
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
}

// ConsumerConfig — параметры консьюмера стенда.
type ConsumerConfig struct {
	GroupID           string   `yaml:"group_id"`
	StartOffset       string   `yaml:"start_offset"`
	DisableAutoCommit bool     `yaml:"disable_auto_commit"`
	CommitInterval    Duration `yaml:"commit_interval"`
	MaxBatchSize      int64    `yaml:"max_batch_size"`
	SessionTimeout    Duration `yaml:"session_timeout"`
	RebalanceTimeout  Duration `yaml:"rebalance_timeout"`
}

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
			MessageCount:       10_000_000,
			MessageTemplate:    defaultMessageTemplate,
			MetricsAddr:        defaultMetricsAddr,
		},
		Consumer: ConsumerConfig{
			GroupID:           "group-1",
			StartOffset:       "latest",
			DisableAutoCommit: false,
			CommitInterval:    Duration(5 * time.Second),
			MaxBatchSize:      1000,
			SessionTimeout:    Duration(30 * time.Second),
			RebalanceTimeout:  Duration(60 * time.Second),
		},
	}
}

func LoadKafkaClientConfig(configPath string) (*KafkaClientConfig, error) {
	cfg, err := LoadYAMLConfig(configPath, DefaultKafkaClientConfig)
	if err != nil {
		return nil, err
	}
	ApplyCommonEnvOverrides(cfg)
	ApplyProducerEnvOverrides(cfg)
	ApplyConsumerEnvOverrides(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func ApplyCommonEnvOverrides(cfg *KafkaClientConfig) {
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
}

func ApplyProducerEnvOverrides(cfg *KafkaClientConfig) {
	if v := os.Getenv("PRODUCER_LINGER"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Producer.Linger = Duration(d)
		}
	}
	if v := os.Getenv("PRODUCER_BATCH_MAX_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			cfg.Producer.BatchMaxBytes = int32(n)
		}
	}
	if v := os.Getenv("PRODUCER_REQUIRED_ACKS"); v != "" {
		cfg.Producer.RequiredAcks = v
	}
	if v := os.Getenv("PRODUCER_DISABLE_IDEMPOTENCY"); v != "" {
		cfg.Producer.DisableIdempotency = v == "true" || v == "1"
	}
	if v := os.Getenv("MESSAGE_COUNT"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Producer.MessageCount = n
		}
	}
	if v := os.Getenv("PRODUCER_COMPRESSION"); v != "" {
		cfg.Producer.Compression = v
	}
	if v := os.Getenv("PRODUCER_METRICS_ADDR"); v != "" {
		cfg.Producer.MetricsAddr = v
	}
}

func ApplyConsumerEnvOverrides(cfg *KafkaClientConfig) {
	if v := os.Getenv("CONSUMER_GROUP_ID"); v != "" {
		cfg.Consumer.GroupID = v
	}
	if v := os.Getenv("CONSUMER_START_OFFSET"); v != "" {
		cfg.Consumer.StartOffset = v
	}
}

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
		c.Consumer.GroupID = "group-1"
	}
	if c.Consumer.MaxBatchSize <= 0 {
		c.Consumer.MaxBatchSize = 1000
	}
	if _, err := ParseStartOffset(c.Consumer.StartOffset); err != nil {
		return err
	}
	return nil
}

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
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	return opts, nil
}

func (c *KafkaClientConfig) ToKgoConsumerOptions() ([]kgo.Opt, error) {
	offset, err := ParseStartOffset(c.Consumer.StartOffset)
	if err != nil {
		return nil, err
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.Consumer.GroupID),
		kgo.ConsumeTopics(c.Topic),
		kgo.ConsumeResetOffset(offset),
		kgo.SessionTimeout(c.Consumer.SessionTimeout.Duration()),
		kgo.RebalanceTimeout(c.Consumer.RebalanceTimeout.Duration()),
	}

	if c.Consumer.DisableAutoCommit || c.Consumer.CommitInterval.Duration() == 0 {
		opts = append(opts, kgo.DisableAutoCommit())
	} else {
		opts = append(opts, kgo.AutoCommitInterval(c.Consumer.CommitInterval.Duration()))
	}

	return opts, nil
}

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

func ParseStartOffset(value string) (kgo.Offset, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "earliest", "at_start":
		return kgo.NewOffset().AtStart(), nil
	case "latest", "at_end", "":
		return kgo.NewOffset().AtEnd(), nil
	default:
		return kgo.NewOffset().AtEnd(), fmt.Errorf("invalid start_offset: %s", value)
	}
}
