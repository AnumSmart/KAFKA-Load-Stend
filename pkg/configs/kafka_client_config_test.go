package configs

import (
	"strings"
	"testing"
	"time"
)

func TestParseRequiredAcks(t *testing.T) {
	cases := []string{"leader", "all", "none", "1", "-1", "0"}
	for _, c := range cases {
		if _, err := ParseRequiredAcks(c); err != nil {
			t.Fatalf("ParseRequiredAcks(%q) error: %v", c, err)
		}
	}
	if _, err := ParseRequiredAcks("invalid"); err == nil {
		t.Fatal("expected error for invalid acks")
	}
}

func TestParseCompression(t *testing.T) {
	cases := []string{"none", "snappy", "lz4", "gzip", "zstd"}
	for _, c := range cases {
		if _, err := ParseCompression(c); err != nil {
			t.Fatalf("ParseCompression(%q) error: %v", c, err)
		}
	}
	if _, err := ParseCompression("brotli"); err == nil {
		t.Fatal("expected error for invalid compression")
	}
}

func TestToKgoProducerOptions(t *testing.T) {
	cfg := DefaultKafkaClientConfig()
	opts, err := cfg.ToKgoProducerOptions()
	if err != nil {
		t.Fatalf("ToKgoProducerOptions: %v", err)
	}
	if len(opts) == 0 {
		t.Fatal("expected non-empty options")
	}
}

func TestToKgoConsumerOptions(t *testing.T) {
	cfg := DefaultKafkaClientConfig()
	opts, err := cfg.ToKgoConsumerOptions()
	if err != nil {
		t.Fatalf("ToKgoConsumerOptions: %v", err)
	}
	if len(opts) == 0 {
		t.Fatal("expected non-empty options")
	}
}

// TestDefaultKafkaClientConfigMessageCountIs100k фиксирует целевой объём
// прогона (PR #2). Регресс: до PR #2 дефолт был 10_000_000, что давало
// прогон на порядок длиннее задуманного и скрывало короткие эксперименты.
func TestDefaultKafkaClientConfigMessageCountIs100k(t *testing.T) {
	cfg := DefaultKafkaClientConfig()
	if cfg.Producer.MessageCount != 100_000 {
		t.Fatalf("default message_count = %d, want 100000", cfg.Producer.MessageCount)
	}
}

func TestApplyProducerEnvOverrides(t *testing.T) {
	t.Setenv("PRODUCER_LINGER", "200ms")
	t.Setenv("PRODUCER_BATCH_MAX_BYTES", "2097152")
	t.Setenv("PRODUCER_REQUIRED_ACKS", "all")
	t.Setenv("PRODUCER_DISABLE_IDEMPOTENCY", "true")
	t.Setenv("MESSAGE_COUNT", "1000")

	cfg := DefaultKafkaClientConfig()
	if err := ApplyProducerEnvOverrides(cfg); err != nil {
		t.Fatalf("ApplyProducerEnvOverrides: %v", err)
	}

	if cfg.Producer.Linger.Duration() != 200*time.Millisecond {
		t.Fatalf("linger = %s", cfg.Producer.Linger)
	}
	if cfg.Producer.BatchMaxBytes != 2097152 {
		t.Fatalf("batch = %d", cfg.Producer.BatchMaxBytes)
	}
	if cfg.Producer.RequiredAcks != "all" {
		t.Fatalf("acks = %s", cfg.Producer.RequiredAcks)
	}
	if !cfg.Producer.DisableIdempotency {
		t.Fatal("expected disable_idempotency=true")
	}
	if cfg.Producer.MessageCount != 1000 {
		t.Fatalf("count = %d", cfg.Producer.MessageCount)
	}
}

// TestApplyProducerEnvOverridesDisableIdempotency — регресс на bug #5:
// раньше проверка была v == "true" || v == "1", из-за чего "TRUE"/"True"/"t"
// тихо игнорировались и настройка не применялась. Теперь через
// strconv.ParseBool принимается весь канонический набор.
func TestApplyProducerEnvOverridesDisableIdempotency(t *testing.T) {
	// Нейтрализуем остальные читаемые переменные, чтобы единственным
	// источником возможной ошибки была тестируемая.
	t.Setenv("PRODUCER_LINGER", "50ms")
	t.Setenv("PRODUCER_BATCH_MAX_BYTES", "1048576")
	t.Setenv("MESSAGE_COUNT", "1000")

	cases := []struct {
		value string
		want  bool
	}{
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"t", true},
		{"T", true},
		{"1", true},
		{"false", false},
		{"FALSE", false},
		{"False", false},
		{"f", false},
		{"F", false},
		{"0", false},
	}
	for _, c := range cases {
		t.Run(c.value, func(t *testing.T) {
			t.Setenv("PRODUCER_DISABLE_IDEMPOTENCY", c.value)

			cfg := DefaultKafkaClientConfig()
			if err := ApplyProducerEnvOverrides(cfg); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Producer.DisableIdempotency != c.want {
				t.Fatalf("DisableIdempotency = %v, want %v",
					cfg.Producer.DisableIdempotency, c.want)
			}
		})
	}
}

// TestApplyProducerEnvOverridesInvalid — регресс на bug #4: раньше ошибки
// парсинга молча глотались (Warn в лог + дефолт), и эксперимент тихо
// сравнивал не те параметры. Теперь Apply возвращает error, а
// LoadKafkaClientConfig роняет запуск.
func TestApplyProducerEnvOverridesInvalid(t *testing.T) {
	cases := []struct {
		name string
		key  string
		val  string
	}{
		{"linger", "PRODUCER_LINGER", "not-a-duration"},
		{"batch", "PRODUCER_BATCH_MAX_BYTES", "not-a-number"},
		{"disable_idempotency", "PRODUCER_DISABLE_IDEMPOTENCY", "maybe"},
		{"message_count", "MESSAGE_COUNT", "not-a-number"},
		{"message_bytes", "PRODUCER_MESSAGE_BYTES", "nope"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Валидные значения для всего остального, чтобы ошибка была
			// ровно одна и её можно было проверить по тексту.
			t.Setenv("PRODUCER_LINGER", "50ms")
			t.Setenv("PRODUCER_BATCH_MAX_BYTES", "1048576")
			t.Setenv("PRODUCER_DISABLE_IDEMPOTENCY", "true")
			t.Setenv("MESSAGE_COUNT", "1000")
			t.Setenv(c.key, c.val)

			cfg := DefaultKafkaClientConfig()
			err := ApplyProducerEnvOverrides(cfg)
			if err == nil {
				t.Fatalf("expected error for %s=%q", c.key, c.val)
			}
			if !strings.Contains(err.Error(), c.key) {
				t.Fatalf("error should mention %s, got: %v", c.key, err)
			}
		})
	}
}

func TestApplyCommonEnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka1:19092, kafka2:19092,kafka3:19092")
	t.Setenv("KAFKA_TOPIC", "load-test")

	cfg := DefaultKafkaClientConfig()
	if err := ApplyCommonEnvOverrides(cfg); err != nil {
		t.Fatalf("ApplyCommonEnvOverrides: %v", err)
	}
	if len(cfg.Brokers) != 3 || cfg.Brokers[0] != "kafka1:19092" {
		t.Fatalf("brokers = %v", cfg.Brokers)
	}
	if cfg.Topic != "load-test" {
		t.Fatalf("topic = %s", cfg.Topic)
	}
}

func TestApplyConsumerEnvOverrides(t *testing.T) {
	t.Setenv("CONSUMER_GROUP_ID", "group-3")
	t.Setenv("CONSUMER_METRICS_ADDR", ":8084")

	cfg := DefaultKafkaClientConfig()
	if err := ApplyConsumerEnvOverrides(cfg); err != nil {
		t.Fatalf("ApplyConsumerEnvOverrides: %v", err)
	}
	if cfg.Consumer.GroupID != "group-3" {
		t.Fatalf("group = %s", cfg.Consumer.GroupID)
	}
	if cfg.Consumer.MetricsAddr != ":8084" {
		t.Fatalf("metrics_addr = %s", cfg.Consumer.MetricsAddr)
	}
}

func TestValidateRejectsEmptyBrokers(t *testing.T) {
	cfg := DefaultKafkaClientConfig()
	cfg.Brokers = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateEmptyGroupIDDefaultsToLoadTestGroup(t *testing.T) {
	cfg := DefaultKafkaClientConfig()
	cfg.Consumer.GroupID = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg.Consumer.GroupID != "load-test-group" {
		t.Fatalf("group = %s, want load-test-group", cfg.Consumer.GroupID)
	}
}

func TestDefaultStartOffsetIsEarliest(t *testing.T) {
	cfg := DefaultKafkaClientConfig()
	if cfg.Consumer.StartOffset != "earliest" {
		t.Fatalf("start_offset = %q, want earliest", cfg.Consumer.StartOffset)
	}
}

func TestParseStartOffsetEmptyIsEarliest(t *testing.T) {
	off, err := ParseStartOffset("")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := ParseStartOffset("earliest")
	if off != want {
		t.Fatalf("empty start_offset should equal earliest")
	}
}

func TestApplyProducerEnvOverridesMaxBufferedAndHold(t *testing.T) {
	t.Setenv("PRODUCER_MAX_BUFFERED_RECORDS", "500000")
	t.Setenv("PRODUCER_METRICS_HOLD", "15s")

	cfg := DefaultKafkaClientConfig()
	if err := ApplyProducerEnvOverrides(cfg); err != nil {
		t.Fatalf("ApplyProducerEnvOverrides: %v", err)
	}
	if cfg.Producer.MaxBufferedRecords != 500000 {
		t.Fatalf("max_buffered = %d", cfg.Producer.MaxBufferedRecords)
	}
	if cfg.Producer.MetricsHold.Duration() != 15*time.Second {
		t.Fatalf("metrics_hold = %s", cfg.Producer.MetricsHold)
	}
}

func TestApplyProducerEnvOverridesMessageBytes(t *testing.T) {
	t.Setenv("PRODUCER_MESSAGE_BYTES", "102400")

	cfg := DefaultKafkaClientConfig()
	if err := ApplyProducerEnvOverrides(cfg); err != nil {
		t.Fatalf("ApplyProducerEnvOverrides: %v", err)
	}
	if cfg.Producer.MessageBytes != 102400 {
		t.Fatalf("message_bytes = %d", cfg.Producer.MessageBytes)
	}
}

func TestValidateNormalizesZeroSessionTimeouts(t *testing.T) {
	cfg := DefaultKafkaClientConfig()
	cfg.Consumer.SessionTimeout = 0
	cfg.Consumer.RebalanceTimeout = 0
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Consumer.SessionTimeout.Duration() != 30*time.Second {
		t.Fatalf("session = %s", cfg.Consumer.SessionTimeout)
	}
	if cfg.Consumer.RebalanceTimeout.Duration() != 60*time.Second {
		t.Fatalf("rebalance = %s", cfg.Consumer.RebalanceTimeout)
	}
}
