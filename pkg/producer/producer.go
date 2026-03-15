package producer

import (
	"errors"
	"fmt"
	"kafkaLib/pkg/config"

	"github.com/IBM/sarama"
)

// Producer - обертка над sarama.SyncProducer
type Producer struct {
	client       sarama.SyncProducer
	config       *config.ProducerConfig
	topicInfo    map[string]*config.TopicConfig
	defaultTopic string
}

// NewProducer - создает новый продюсер
func NewProducer(client sarama.SyncProducer, cfg *config.ProducerConfig) *Producer {
	p := &Producer{
		client:    client,
		config:    cfg,
		topicInfo: make(map[string]*config.TopicConfig),
	}

	// Загружаем информацию о топиках
	for _, topic := range cfg.Topics {
		p.topicInfo[topic.Name] = &topic
		if topic.DefaultTopic {
			p.defaultTopic = topic.Name
		}
	}

	// Если указан topic по умолчанию в конфиге, используем его
	if cfg.Topic != "" && p.defaultTopic == "" {
		p.defaultTopic = cfg.Topic
	}

	return p
}

// Message - сообщение для отправки
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Partition int32
}

// SendMessage - отправляет сообщение
func (p *Producer) SendMessage(msg *Message) (partition int32, offset int64, err error) {
	// Определяем топик
	topic := msg.Topic
	if topic == "" {
		topic = p.defaultTopic
	}

	if topic == "" {
		return 0, 0, errors.New("no topic specified and no default topic configured")
	}

	// Проверяем, что топик разрешен (опционально)
	if _, exists := p.topicInfo[topic]; !exists && len(p.topicInfo) > 0 {
		return 0, 0, fmt.Errorf("topic '%s' is not configured for this producer", topic)
	}

	// Создаем сообщение для Kafka
	saramaMsg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: msg.Partition,
		Key:       sarama.ByteEncoder(msg.Key),
		Value:     sarama.ByteEncoder(msg.Value),
	}

	// Добавляем заголовки
	if len(msg.Headers) > 0 {
		headers := make([]sarama.RecordHeader, 0, len(msg.Headers))
		for key, value := range msg.Headers {
			headers = append(headers, sarama.RecordHeader{
				Key:   []byte(key),
				Value: value,
			})
		}
		saramaMsg.Headers = headers
	}

	return p.client.SendMessage(saramaMsg)
}

// Send - упрощенный метод отправки
func (p *Producer) Send(topic string, key, value []byte) (partition int32, offset int64, err error) {
	return p.SendMessage(&Message{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}

// SendDefault - отправляет в топик по умолчанию
func (p *Producer) SendDefault(key, value []byte) (partition int32, offset int64, err error) {
	return p.SendMessage(&Message{
		Key:   key,
		Value: value,
	})
}

// GetTopics - возвращает список настроенных топиков
func (p *Producer) GetTopics() []string {
	topics := make([]string, 0, len(p.topicInfo))
	for topic := range p.topicInfo {
		topics = append(topics, topic)
	}
	return topics
}

// GetDefaultTopic - возвращает топик по умолчанию
func (p *Producer) GetDefaultTopic() string {
	return p.defaultTopic
}

// Close - закрывает продюсер
func (p *Producer) Close() error {
	return p.client.Close()
}
