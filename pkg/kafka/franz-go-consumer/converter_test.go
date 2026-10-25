package franzgoconsumer

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestToKafkaMessage(t *testing.T) {
	now := time.Now()
	record := &kgo.Record{
		Topic:     "load-test",
		Partition: 2,
		Offset:    17,
		Timestamp: now,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers:   []kgo.RecordHeader{{Key: "h", Value: []byte("1")}},
	}

	msg := ToKafkaMessage(record)
	if msg.Topic != "load-test" || msg.Partition != 2 || msg.Offset != 17 {
		t.Fatalf("unexpected message: %+v", msg)
	}
	if string(msg.Key) != "k" || string(msg.Value) != "v" {
		t.Fatalf("unexpected payload: key=%s value=%s", msg.Key, msg.Value)
	}
	if string(msg.Headers["h"]) != "1" {
		t.Fatalf("unexpected header: %v", msg.Headers)
	}
}
