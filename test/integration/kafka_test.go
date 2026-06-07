package integration

import (
	"KonnectKit/pkg/kafka"
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainer "github.com/testcontainers/testcontainers-go/modules/kafka"

	"KonnectKit/pkg/config"
)

// TestKafkaIntegration - основной интеграционный тест
func TestKafkaIntegration(t *testing.T) {
	ctx := context.Background()

	// 1. Запускаем Kafka контейнер
	kafkaContainer, err := testcontainer.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		testcontainer.WithClusterID("test-cluster"),
	)
	require.NoError(t, err)

	// 2. Обеспечиваем остановку контейнера после тестов
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	// 3. Получаем адрес брокера
	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)

	t.Logf("Kafka broker address: %v", brokers)

	// 4. Создаем тестовый конфиг с динамическим адресом брокера
	cfg := createTestConfig(brokers[0])

	// 5. Создаем клиент
	client := kafka.NewClient(cfg)
	defer client.Close()

	// 6. Запускаем тесты
	t.Run("Producer and Consumer", func(t *testing.T) {
		testProducerConsumer(t, client)
	})

	t.Run("Multiple Topics", func(t *testing.T) {
		testMultipleTopics(t, client)
	})

	t.Run("Consumer Group", func(t *testing.T) {
		testConsumerGroup(t, client)
	})
}

// TestHandler - обработчик для тестов
type TestHandler struct {
	msgChan chan *sarama.ConsumerMessage
}

func (h *TestHandler) HandleMessage(msg *sarama.ConsumerMessage) error {
	select {
	case h.msgChan <- msg:
	default:
	}
	return nil
}

// createTestConfig - создает тестовую конфигурацию
func createTestConfig(broker string) *config.Config {
	// Загружаем базовый конфиг из YAML
	cfg, err := config.LoadConfig("testdata/test_config.yaml")
	if err != nil {
		panic(err)
	}

	// Обновляем адрес брокера во всех конфигурациях
	cfg.Kafka.Base.Brokers = []string{broker}

	for name := range cfg.Kafka.Producer {
		producer := cfg.Kafka.Producer[name]
		producer.Brokers = []string{broker}
		// Устанавливаем таймауты из базовой конфигурации
		producer.DialTimeout = cfg.Kafka.Base.DialTimeout
		producer.ReadTimeout = cfg.Kafka.Base.ReadTimeout
		producer.WriteTimeout = cfg.Kafka.Base.WriteTimeout
		producer.Timeout = cfg.Kafka.Base.Timeout
		cfg.Kafka.Producer[name] = producer
	}

	for name := range cfg.Kafka.Consumer {
		consumer := cfg.Kafka.Consumer[name]
		consumer.Brokers = []string{broker}
		// Устанавливаем таймауты из базовой конфигурации
		consumer.DialTimeout = cfg.Kafka.Base.DialTimeout
		consumer.ReadTimeout = cfg.Kafka.Base.ReadTimeout
		consumer.WriteTimeout = cfg.Kafka.Base.WriteTimeout
		consumer.Timeout = cfg.Kafka.Base.Timeout
		cfg.Kafka.Consumer[name] = consumer
	}

	return cfg
}

// testProducerConsumer - тест отправки и получения сообщения
func testProducerConsumer(t *testing.T, client *kafka.Client) {
	topic := "test-topic"

	// 1. Сначала создаем канал и хендлер
	msgChan := make(chan *sarama.ConsumerMessage, 1)
	handler := &TestHandler{msgChan: msgChan}

	// 2. Создаем консюмера
	cons, err := client.NewConsumer("test_consumer", handler)
	require.NoError(t, err)
	defer cons.Stop()

	// 3. Запускаем консюмера ДО отправки сообщения
	err = cons.ConsumeTopics([]string{topic})
	require.NoError(t, err)

	// 4. Ждем, пока консюмер подключится к Kafka
	time.Sleep(2 * time.Second)
	t.Log("Consumer started and ready")

	// 5. Создаем продюсера
	prod, err := client.NewProducer("test_producer")
	require.NoError(t, err)

	// 6. Отправляем сообщение
	testMessage := []byte("Hello, Kafka!")
	partition, offset, err := prod.Send(topic, []byte("test-key"), testMessage)
	require.NoError(t, err)

	t.Logf("Message sent: partition=%d, offset=%d", partition, offset)

	// 7. Ждем получения сообщения
	select {
	case msg := <-msgChan:
		assert.Equal(t, testMessage, msg.Value)
		assert.Equal(t, "test-key", string(msg.Key))
		assert.Equal(t, topic, msg.Topic)
		t.Logf("Message received successfully: %s", string(msg.Value))
	case <-time.After(15 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

// testMultipleTopics - тест работы с несколькими топиками
func testMultipleTopics(t *testing.T, client *kafka.Client) {
	topics := []string{"topic-1", "topic-2", "topic-3"}
	receivedCount := make(map[string]int)

	// Канал для получения сообщений
	msgChan := make(chan *sarama.ConsumerMessage, 10)
	handler := &TestHandler{msgChan: msgChan}

	// Создаем консюмера
	cons, err := client.NewConsumer("multi_topic_consumer", handler)
	require.NoError(t, err)
	defer cons.Stop()

	// Подписываемся на все топики
	err = cons.ConsumeTopics(topics)
	require.NoError(t, err)

	// Создаем продюсера
	prod, err := client.NewProducer("test_producer")
	require.NoError(t, err)

	// Отправляем сообщения в разные топики
	for i, topic := range topics {
		msg := []byte(fmt.Sprintf("Message %d for %s", i, topic))
		_, _, err := prod.Send(topic, []byte("key"), msg)
		require.NoError(t, err)
		t.Logf("Sent to %s: %s", topic, string(msg))
	}

	// Ждем получения сообщений
	timeout := time.After(10 * time.Second)
	receivedTotal := 0
	expectedTotal := len(topics)

	for receivedTotal < expectedTotal {
		select {
		case msg := <-msgChan:
			receivedCount[msg.Topic]++
			receivedTotal++
			t.Logf("Received from %s: %s", msg.Topic, string(msg.Value))
		case <-timeout:
			t.Fatalf("Timeout: received %d/%d messages", receivedTotal, expectedTotal)
		}
	}

	// Проверяем, что все топики получили сообщения
	for _, topic := range topics {
		assert.Equal(t, 1, receivedCount[topic], "Topic %s should have 1 message", topic)
	}
}

// testConsumerGroup - тест consumer group
func testConsumerGroup(t *testing.T, client *kafka.Client) {
	topic := "group-test-topic"
	messageCount := 10

	// Создаем продюсера и отправляем 10 сообщений
	prod, err := client.NewProducer("test_producer")
	require.NoError(t, err)

	for i := 0; i < messageCount; i++ {
		msg := []byte(fmt.Sprintf("Message %d", i))
		_, _, err := prod.Send(topic, []byte(fmt.Sprintf("key-%d", i)), msg)
		require.NoError(t, err)
	}
	t.Logf("Sent %d messages to %s", messageCount, topic)

	// Создаем два консюмера в одной группе
	messages1 := make(chan *sarama.ConsumerMessage, messageCount)
	messages2 := make(chan *sarama.ConsumerMessage, messageCount)

	handler1 := &TestHandler{msgChan: messages1}
	handler2 := &TestHandler{msgChan: messages2}

	consumer1, err := client.NewConsumer("test_group_consumer", handler1)
	require.NoError(t, err)
	defer consumer1.Stop()

	consumer2, err := client.NewConsumer("test_group_consumer", handler2)
	require.NoError(t, err)
	defer consumer2.Stop()

	// Запускаем обоих консюмеров
	err = consumer1.ConsumeTopics([]string{topic})
	require.NoError(t, err)
	err = consumer2.ConsumeTopics([]string{topic})
	require.NoError(t, err)

	// Ждем распределения сообщений между консюмерами
	time.Sleep(5 * time.Second)

	// Собираем статистику
	count1 := len(messages1)
	count2 := len(messages2)
	totalReceived := count1 + count2

	t.Logf("Consumer1 received: %d messages", count1)
	t.Logf("Consumer2 received: %d messages", count2)
	t.Logf("Total received: %d/%d", totalReceived, messageCount)

	// Сообщения должны распределиться между консюмерами
	assert.Greater(t, totalReceived, 0, "Should receive messages")
	assert.LessOrEqual(t, totalReceived, messageCount, "Should not receive more than sent")
}
