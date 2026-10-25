package configs

import (
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

func TestApplyProducerEnvOverrides(t *testing.T) {
	t.Setenv("PRODUCER_LINGER", "200ms")
	t.Setenv("PRODUCER_BATCH_MAX_BYTES", "2097152")
	t.Setenv("PRODUCER_REQUIRED_ACKS", "all")
	t.Setenv("PRODUCER_DISABLE_IDEMPOTENCY", "true")
	t.Setenv("MESSAGE_COUNT", "1000")

	cfg := DefaultKafkaClientConfig()
	ApplyProducerEnvOverrides(cfg)

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

func TestApplyCommonEnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka1:19092, kafka2:19092,kafka3:19092")
	t.Setenv("KAFKA_TOPIC", "load-test")
	cfg := DefaultKafkaClientConfig()
	ApplyCommonEnvOverrides(cfg)
	if len(cfg.Brokers) != 3 || cfg.Brokers[0] != "kafka1:19092" {
		t.Fatalf("brokers = %v", cfg.Brokers)
	}
	if cfg.Topic != "load-test" {
		t.Fatalf("topic = %s", cfg.Topic)
	}
}

func TestApplyConsumerEnvOverrides(t *testing.T) {
	t.Setenv("CONSUMER_GROUP_ID", "group-3")
	cfg := DefaultKafkaClientConfig()
	ApplyConsumerEnvOverrides(cfg)
	if cfg.Consumer.GroupID != "group-3" {
		t.Fatalf("group = %s", cfg.Consumer.GroupID)
	}
}

func TestValidateRejectsEmptyBrokers(t *testing.T) {
	cfg := DefaultKafkaClientConfig()
	cfg.Brokers = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}
