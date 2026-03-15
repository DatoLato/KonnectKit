package kafka

import (
	"fmt"
	"kafkaLib/pkg/config"
	"kafkaLib/pkg/consumer"
	"kafkaLib/pkg/producer"

	"github.com/IBM/sarama"
)

// Client - клиент Kafka
type Client struct {
	config    *config.Config
	producers map[string]sarama.SyncProducer
	consumers map[string]sarama.ConsumerGroup
}

// NewClient - создает новый клиент Kafka
func NewClient(cfg *config.Config) *Client {
	return &Client{
		config:    cfg,
		producers: make(map[string]sarama.SyncProducer),
		consumers: make(map[string]sarama.ConsumerGroup),
	}
}

// createSaramaConfig - создает конфигурацию Sarama из нашей конфигурации
func createSaramaConfig(base *config.BaseConfig) *sarama.Config {
	saramaConfig := sarama.NewConfig()

	// Timeouts
	saramaConfig.Net.DialTimeout = base.DialTimeout
	saramaConfig.Net.ReadTimeout = base.ReadTimeout
	saramaConfig.Net.WriteTimeout = base.WriteTimeout

	return saramaConfig
}

// NewProducer - создает новый продюсер
func (c *Client) NewProducer(name string) (*producer.Producer, error) {
	producerConfig, err := c.config.GetProducerConfig(name)
	if err != nil {
		return nil, err
	}

	saramaConfig := createSaramaConfig(&producerConfig.BaseConfig)

	// Настройки продюсера
	saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(producerConfig.RequiredAcks)
	saramaConfig.Producer.Retry.Max = producerConfig.MaxRetries
	saramaConfig.Producer.Retry.Backoff = producerConfig.RetryBackoff
	saramaConfig.Producer.Compression = getCompression(producerConfig.Compression)

	if producerConfig.BatchSize > 0 {
		saramaConfig.Producer.MaxMessageBytes = producerConfig.BatchSize
	}

	syncProducer, err := sarama.NewSyncProducer(producerConfig.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	c.producers[name] = syncProducer
	return producer.NewProducer(syncProducer, producerConfig), nil
}

// NewConsumer - создает новый консюмер
func (c *Client) NewConsumer(name string, handler consumer.ConsumerHandler) (*consumer.Consumer, error) {
	consumerConfig, err := c.config.GetConsumerConfig(name)
	if err != nil {
		return nil, err
	}

	saramaConfig := createSaramaConfig(&consumerConfig.BaseConfig)

	// Настройки консюмера
	saramaConfig.Consumer.Group.Session.Timeout = consumerConfig.SessionTimeout
	saramaConfig.Consumer.Group.Heartbeat.Interval = consumerConfig.HeartbeatInterval
	saramaConfig.Consumer.MaxWaitTime = consumerConfig.MaxWaitTime
	saramaConfig.Consumer.Fetch.Min = int32(consumerConfig.MinFetchSize)
	saramaConfig.Consumer.Fetch.Max = int32(consumerConfig.MaxFetchSize)
	saramaConfig.Consumer.Offsets.Initial = getOffset(consumerConfig.StartOffset)
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = consumerConfig.AutoCommit
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = consumerConfig.AutoCommitInterval

	consumerGroup, err := sarama.NewConsumerGroup(consumerConfig.Brokers, consumerConfig.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	c.consumers[name] = consumerGroup
	return consumer.NewConsumer(consumerGroup, consumerConfig, handler), nil
}

// Close - закрывает все соединения
func (c *Client) Close() error {
	for name, producer := range c.producers {
		if err := producer.Close(); err != nil {
			return fmt.Errorf("failed to close producer %s: %w", name, err)
		}
	}

	for name, consumer := range c.consumers {
		if err := consumer.Close(); err != nil {
			return fmt.Errorf("failed to close consumer %s: %w", name, err)
		}
	}

	return nil
}

// getCompression - преобразует строку в тип компрессии Sarama
func getCompression(compression string) sarama.CompressionCodec {
	switch compression {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "zstd":
		return sarama.CompressionZSTD
	default:
		return sarama.CompressionNone
	}
}

// getOffset - преобразует строку в значение offset
func getOffset(offset string) int64 {
	switch offset {
	case "oldest":
		return sarama.OffsetOldest
	case "newest":
		return sarama.OffsetNewest
	default:
		return sarama.OffsetNewest
	}
}
