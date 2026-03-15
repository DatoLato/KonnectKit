package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadConfig - загрузка конфигурации из YAML файла
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("error parsing config file: %w", err)
	}

	for name, producer := range config.Kafka.Producer {
		applyBaseConfig(&producer.BaseConfig, &config.Kafka.Base)
		config.Kafka.Producer[name] = producer
	}

	for name, consumer := range config.Kafka.Consumer {
		applyBaseConfig(&consumer.BaseConfig, &config.Kafka.Base)
		config.Kafka.Consumer[name] = consumer
	}

	return &config, nil
}

// applyBaseConfig - TODO: заменить на валидацию
func applyBaseConfig(target, source *BaseConfig) {
	if len(target.Brokers) == 0 {
		target.Brokers = source.Brokers
	}
	if target.Timeout == 0 {
		target.Timeout = source.Timeout
	}
	if target.DialTimeout == 0 {
		target.DialTimeout = source.DialTimeout
	}
	if target.ReadTimeout == 0 {
		target.ReadTimeout = source.ReadTimeout
	}
	if target.WriteTimeout == 0 {
		target.WriteTimeout = source.WriteTimeout
	}
}

// GetProducerConfig - получение конфигурации продюсера по имени
func (c *Config) GetProducerConfig(name string) (*ProducerConfig, error) {
	producer, exists := c.Kafka.Producer[name]
	if !exists {
		return nil, fmt.Errorf("producer %s not found", name)
	}
	return &producer, nil
}

// GetConsumerConfig - получение конфигурации консюмера по имени
func (c *Config) GetConsumerConfig(name string) (*ConsumerConfig, error) {
	consumer, exists := c.Kafka.Consumer[name]
	if !exists {
		return nil, fmt.Errorf("consumer %s not found", name)
	}
	return &consumer, nil
}
