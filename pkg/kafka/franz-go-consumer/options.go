package franzgoconsumer

import "time"

type ConsumerOptions struct {
	StatsPrintInterval time.Duration
	CommitInterval     time.Duration
	ShutdownTimeout    time.Duration
	MaxBatchSize       int64
	EnableDebugLog     bool
}

func DefaultOptions() *ConsumerOptions {
	return &ConsumerOptions{
		StatsPrintInterval: 10 * time.Second,
		CommitInterval:     5 * time.Second,
		ShutdownTimeout:    25 * time.Second,
		MaxBatchSize:       1000,
		EnableDebugLog:     false,
	}
}

func (o *ConsumerOptions) Validate() error {
	if o.StatsPrintInterval <= 0 {
		o.StatsPrintInterval = 10 * time.Second
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 25 * time.Second
	}
	if o.MaxBatchSize <= 0 {
		o.MaxBatchSize = 1000
	}
	return nil
}
