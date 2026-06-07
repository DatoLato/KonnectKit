package fixtures

import (
	"fmt"
	"time"

	"KonnectKit/pkg/config"
)

// TestTopics - константы с именами топиков
var TestTopics = struct {
	Single    string
	Multiple  []string
	GroupTest string
}{
	Single:    "test-topic",
	Multiple:  []string{"topic-1", "topic-2", "topic-3"},
	GroupTest: "group-test-topic",
}

// TestMessages - тестовые сообщения
var TestMessages = struct {
	Simple  []byte
	JSON    []byte
	WithKey func(key string) ([]byte, []byte)
}{
	Simple: []byte("Hello, Kafka!"),
	JSON:   []byte(`{"message": "test", "timestamp": "now"}`),
	WithKey: func(key string) ([]byte, []byte) {
		return []byte(key), []byte(fmt.Sprintf("Message for %s", key))
	},
}

// ConfigBuilder - билдер для создания конфигураций
type ConfigBuilder struct {
	broker string
}

// NewConfigBuilder - создает новый билдер конфигурации
func NewConfigBuilder(broker string) *ConfigBuilder {
	return &ConfigBuilder{broker: broker}
}

// BuildBaseConfig - создает базовую конфигурацию
func (b *ConfigBuilder) BuildBaseConfig() config.BaseConfig {
	return config.BaseConfig{
		Brokers:      []string{b.broker},
		Timeout:      30 * time.Second,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// BuildProducerConfig - создает конфигурацию продюсера
func (b *ConfigBuilder) BuildProducerConfig(name string) config.ProducerConfig {
	base := b.BuildBaseConfig()
	return config.ProducerConfig{
		BaseConfig:   base,
		RequiredAcks: 1,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
		Compression:  "snappy",
		Partitioner:  "hash",
	}
}

// BuildConsumerConfig - создает конфигурацию консюмера
func (b *ConfigBuilder) BuildConsumerConfig(name, groupID, startOffset string) config.ConsumerConfig {
	base := b.BuildBaseConfig()
	return config.ConsumerConfig{
		BaseConfig:         base,
		GroupID:            groupID,
		AutoCommit:         true,
		AutoCommitInterval: 1 * time.Second,
		SessionTimeout:     30 * time.Second,
		HeartbeatInterval:  3 * time.Second,
		MaxWaitTime:        250 * time.Millisecond,
		MinFetchSize:       1,
		MaxFetchSize:       1048576,
		StartOffset:        startOffset,
	}
}

// BuildFullConfig - создает полную конфигурацию для тестов
func (b *ConfigBuilder) BuildFullConfig() *config.Config {
	base := b.BuildBaseConfig()

	return &config.Config{
		Kafka: struct {
			Base     config.BaseConfig                `yaml:"base"`
			Producer map[string]config.ProducerConfig `yaml:"producers"`
			Consumer map[string]config.ConsumerConfig `yaml:"consumers"`
		}{
			Base: base,
			Producer: map[string]config.ProducerConfig{
				"test_producer": b.BuildProducerConfig("test_producer"),
			},
			Consumer: map[string]config.ConsumerConfig{
				"test_consumer":        b.BuildConsumerConfig("test_consumer", "test-group", "newest"),
				"test_group_consumer":  b.BuildConsumerConfig("test_group_consumer", "shared-group", "earliest"),
				"multi_topic_consumer": b.BuildConsumerConfig("multi_topic_consumer", "multi-group", "earliest"),
			},
		},
	}
}
