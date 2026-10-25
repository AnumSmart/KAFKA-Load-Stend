package configs

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration — обёртка time.Duration с разбором строк вида "50ms".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		parsed, parseErr := time.ParseDuration(s)
		if parseErr != nil {
			return fmt.Errorf("invalid duration %q: %w", s, parseErr)
		}
		*d = Duration(parsed)
		return nil
	}

	var n int64
	if err := value.Decode(&n); err != nil {
		return fmt.Errorf("duration must be a string like 50ms")
	}
	*d = Duration(n)
	return nil
}

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d Duration) String() string {
	return time.Duration(d).String()
}
