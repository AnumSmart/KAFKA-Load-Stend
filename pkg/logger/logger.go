package logger

import (
	"log/slog"
	"os"
	"sync"
)

var (
	globalLogger *slog.Logger
	once         sync.Once
)

func InitLogger(cfg *LoggerConfig) {
	once.Do(func() {
		globalLogger = NewLoggerFromConfig(cfg)
	})
}

func GetLogger() *slog.Logger {
	once.Do(func() {
		if globalLogger == nil {
			globalLogger = NewLoggerFromConfig(nil)
		}
	})
	return globalLogger
}

func NewLoggerFromConfig(cfg *LoggerConfig) *slog.Logger {
	if cfg == nil {
		cfg = DefaultLoggerConfig()
	}

	opts := &slog.HandlerOptions{
		Level:     parseLevel(cfg.Level),
		AddSource: cfg.AddSource,
	}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	if cfg.Service != "" {
		handler = handler.WithAttrs([]slog.Attr{
			slog.String("service", cfg.Service),
		})
	}

	return slog.New(handler)
}

func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
