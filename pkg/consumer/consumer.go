package consumer

import (
	"context"
	"fmt"
	"kafkaLib/pkg/config"
	"sync"

	"github.com/IBM/sarama"
)

// ConsumerHandler - интерфейс обработчика сообщений
type ConsumerHandler interface {
	HandleMessage(msg *sarama.ConsumerMessage) error
}

// Consumer - обертка над sarama.ConsumerGroup
type Consumer struct {
	client  sarama.ConsumerGroup
	config  *config.ConsumerConfig
	handler ConsumerHandler
	topics  []string
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewConsumer - создание нового консюмера
func NewConsumer(client sarama.ConsumerGroup, cfg *config.ConsumerConfig, handler ConsumerHandler) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	topics := make([]string, 0)
	if cfg.Topic != "" {
		topics = append(topics, cfg.Topic)
	}

	for _, topic := range cfg.Topics {
		topics = append(topics, topic.Name)
	}

	return &Consumer{
		client:  client,
		config:  cfg,
		handler: handler,
		topics:  topics,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Consume - потребление сообщений из настроенных топиков
func (c *Consumer) Consume() error {
	if len(c.topics) == 0 {
		return fmt.Errorf("no topics configured for consumer")
	}

	fmt.Printf("Starting consumer for topics: %v\n", c.topics)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			// Проверяем, не остановлен ли консюмер
			if c.ctx.Err() != nil {
				return
			}

			err := c.client.Consume(c.ctx, c.topics, c)
			if err != nil {
				fmt.Printf("Error from consumer: %v\n", err)
			}
		}
	}()

	return nil
}

// ConsumeTopics - позволяет переопределить топики для подписки
func (c *Consumer) ConsumeTopics(topics []string) error {
	if len(topics) == 0 {
		return fmt.Errorf("no topics specified")
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if c.ctx.Err() != nil {
				return
			}

			err := c.client.Consume(c.ctx, topics, c)
			if err != nil {
				fmt.Printf("Error from consumer: %v\n", err)
			}
		}
	}()

	return nil
}

// Setup - вызывается при старте консюмера
func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer session started")
	return nil
}

// Cleanup - вызывается при остановке консюмера
func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer session ended")
	return nil
}

// ConsumeClaim - обрабатывает сообщения из партиции
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// Обрабатываем сообщение
		if err := c.handler.HandleMessage(message); err != nil {
			fmt.Printf("Error handling message: %v\n", err)
			continue
		}
		// Подтверждаем обработку сообщения
		session.MarkMessage(message, "")
	}
	return nil
}

// Stop - останавливает консюмер
func (c *Consumer) Stop() error {
	c.cancel()
	c.wg.Wait()
	return c.client.Close()
}

// Pause - приостанавливает потребление из указанных партиций
func (c *Consumer) Pause(partitions map[string][]int32) {
	c.client.Pause(partitions)
}

// Resume - возобновляет потребление из указанных партиций
func (c *Consumer) Resume(partitions map[string][]int32) {
	c.client.Resume(partitions)
}

// GetConfiguredTopics - возвращает топики из конфига
func (c *Consumer) GetConfiguredTopics() []string {
	return c.topics
}
