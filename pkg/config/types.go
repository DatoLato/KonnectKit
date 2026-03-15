package config

import (
	"time"
)

// Config - общая структура конфигурации
type Config struct {
	Kafka struct {
		Base     BaseConfig                `yaml:"base"`
		Producer map[string]ProducerConfig `yaml:"producers"`
		Consumer map[string]ConsumerConfig `yaml:"consumers"`
	} `yaml:"kafka"`
}

// BaseConfig - базовая конфигурация Kafka
type BaseConfig struct {
	Brokers      []string      `yaml:"brokers"`
	Timeout      time.Duration `yaml:"timeout"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

// TopicConfig - конфигурация топика
type TopicConfig struct {
	Name          string `yaml:"name"`
	DefaultTopic  bool   `yaml:"default_topic"`  // Флаг, что это топик по умолчанию
	Partitions    int32  `yaml:"partitions"`     // Можно указать ожидаемое количество партиций
	Replication   int16  `yaml:"replication"`    // Фактор репликации
	RetentionDays int    `yaml:"retention_days"` // Дней хранения
}

// ProducerConfig - конфигурация продюсера
type ProducerConfig struct {
	BaseConfig   `yaml:",inline"`
	Topic        string        `yaml:"topic"`  // Топик по умолчанию для этого продюсера
	Topics       []TopicConfig `yaml:"topics"` // Список доступных топиков
	RequiredAcks int           `yaml:"required_acks"`
	MaxRetries   int           `yaml:"max_retries"`
	RetryBackoff time.Duration `yaml:"retry_backoff"`
	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
	Compression  string        `yaml:"compression"`
	Partitioner  string        `yaml:"partitioner"` // hash, round-robin, random
}

// ConsumerConfig - конфигурация консюмера
type ConsumerConfig struct {
	BaseConfig         `yaml:",inline"`
	Topic              string        `yaml:"topic"`  // Топик по умолчанию
	Topics             []TopicConfig `yaml:"topics"` // Список топиков для подписки
	GroupID            string        `yaml:"group_id"`
	AutoCommit         bool          `yaml:"auto_commit"`
	AutoCommitInterval time.Duration `yaml:"auto_commit_interval"`
	SessionTimeout     time.Duration `yaml:"session_timeout"`
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval"`
	MaxWaitTime        time.Duration `yaml:"max_wait_time"`
	MinFetchSize       int           `yaml:"min_fetch_size"`
	MaxFetchSize       int           `yaml:"max_fetch_size"`
	StartOffset        string        `yaml:"start_offset"`
}
