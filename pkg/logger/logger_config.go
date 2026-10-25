package logger

// LoggerConfig — конфигурация slog.
type LoggerConfig struct {
	Level     string `yaml:"level"`
	Format    string `yaml:"format"`
	AddSource bool   `yaml:"add_source"`
	Service   string `yaml:"service"`
}

func DefaultLoggerConfig() *LoggerConfig {
	return &LoggerConfig{
		Level:     "info",
		Format:    "text",
		AddSource: false,
		Service:   "kafka-stend",
	}
}
