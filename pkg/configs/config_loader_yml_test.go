package configs

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadYAMLConfigDefaultWhenMissing(t *testing.T) {
	cfg, err := LoadYAMLConfig("missing.yml", DefaultKafkaClientConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Topic != "load-test" {
		t.Fatalf("topic = %s, want load-test", cfg.Topic)
	}
}

func TestLoadYAMLConfigReadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "client.yml")
	content := []byte("topic: custom-topic\nproducer:\n  linger: 100ms\n  message_count: 42\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadYAMLConfig(path, DefaultKafkaClientConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Topic != "custom-topic" {
		t.Fatalf("topic = %s, want custom-topic", cfg.Topic)
	}
	if cfg.Producer.Linger.Duration() != 100*time.Millisecond {
		t.Fatalf("linger = %s, want 100ms", cfg.Producer.Linger)
	}
	if cfg.Producer.MessageCount != 42 {
		t.Fatalf("message_count = %d, want 42", cfg.Producer.MessageCount)
	}
}
