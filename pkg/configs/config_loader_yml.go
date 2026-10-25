package configs

import (
	"errors"
	"io/fs"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadYAMLConfig загружает YAML в структуру T.
// Если путь пустой или файла нет — возвращаются значения из конструктора fn.
func LoadYAMLConfig[T any](configPath string, fn func() *T) (*T, error) {
	config := fn()

	if configPath == "" {
		return config, nil
	}

	if _, err := os.Stat(configPath); errors.Is(err, fs.ErrNotExist) {
		return config, nil
	}

	yamlFile, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(yamlFile, config); err != nil {
		return nil, err
	}

	return config, nil
}
