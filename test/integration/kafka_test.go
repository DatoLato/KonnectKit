package integration

import (
	"KonnectKit/test/integration/fixtures"
	"KonnectKit/test/integration/handlers"
	"KonnectKit/test/integration/suite"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	testsuite "github.com/stretchr/testify/suite"
)

// TestKafkaSuite - запуск тестового suite
func TestKafkaSuite(t *testing.T) {
	testsuite.Run(t, new(KafkaTestSuite))
}

// KafkaTestSuite - структура тестов
type KafkaTestSuite struct {
	suite.KafkaTestSuite
}

// TestProducerConsumerSimple - простой тест продюсер-консюмер
func (s *KafkaTestSuite) TestProducerConsumerSimple() {
	// Given
	handler := handlers.NewTestHandler(10)
	consumer := s.CreateTestConsumer(handler)
	producer := s.CreateTestProducer()

	// When
	err := consumer.ConsumeTopics([]string{fixtures.TestTopics.Single})
	s.Require().NoError(err)

	time.Sleep(2 * time.Second) // Ждем инициализации

	_, _, err = producer.Send(fixtures.TestTopics.Single,
		[]byte("test-key"),
		fixtures.TestMessages.Simple)
	s.Require().NoError(err)

	// Then
	msg, err := handler.WaitForMessage(10 * time.Second)
	s.Require().NoError(err)
	assert.Equal(s.T(), fixtures.TestMessages.Simple, msg.Value)
	assert.Equal(s.T(), "test-key", string(msg.Key))

	// Cleanup
	consumer.Stop()
}

// TestMultipleTopics - тест с несколькими топиками
func (s *KafkaTestSuite) TestMultipleTopics() {
	// Given
	handler := handlers.NewTestHandler(10)
	consumer := s.CreateTestConsumer(handler)
	producer := s.CreateTestProducer()

	// When
	err := consumer.ConsumeTopics(fixtures.TestTopics.Multiple)
	s.Require().NoError(err)

	time.Sleep(2 * time.Second)

	for _, topic := range fixtures.TestTopics.Multiple {
		msg := []byte("Message for " + topic)
		_, _, err := producer.Send(topic, []byte("key"), msg)
		s.Require().NoError(err)
		s.T().Logf("Sent to %s: %s", topic, string(msg))
	}

	// Then
	messages, err := handler.WaitForMessages(len(fixtures.TestTopics.Multiple), 10*time.Second)
	s.Require().NoError(err)

	receivedTopics := make(map[string]int)
	for _, msg := range messages {
		receivedTopics[msg.Topic]++
	}

	for _, topic := range fixtures.TestTopics.Multiple {
		assert.Equal(s.T(), 1, receivedTopics[topic], "Topic %s should have 1 message", topic)
	}

	consumer.Stop()
}

// TestConsumerGroup - тест consumer group
func (s *KafkaTestSuite) TestConsumerGroup() {
	// Given
	messageCount := 10
	handler1 := handlers.NewTestHandler(messageCount)
	handler2 := handlers.NewTestHandler(messageCount)

	consumer1, err := s.GetClient().NewConsumer("test_group_consumer", handler1)
	s.Require().NoError(err)
	defer consumer1.Stop()

	consumer2, err := s.GetClient().NewConsumer("test_group_consumer", handler2)
	s.Require().NoError(err)
	defer consumer2.Stop()

	producer := s.CreateTestProducer()

	// When
	err = consumer1.ConsumeTopics([]string{fixtures.TestTopics.GroupTest})
	s.Require().NoError(err)

	err = consumer2.ConsumeTopics([]string{fixtures.TestTopics.GroupTest})
	s.Require().NoError(err)

	time.Sleep(2 * time.Second)

	for i := 0; i < messageCount; i++ {
		msg := []byte("Message " + string(rune('0'+i)))
		_, _, err := producer.Send(fixtures.TestTopics.GroupTest, []byte("key"), msg)
		s.Require().NoError(err)
	}

	// Then
	time.Sleep(5 * time.Second)

	totalReceived := handler1.GetMessageCount() + handler2.GetMessageCount()
	s.T().Logf("Consumer1: %d, Consumer2: %d, Total: %d/%d",
		handler1.GetMessageCount(), handler2.GetMessageCount(), totalReceived, messageCount)

	assert.Greater(s.T(), totalReceived, 0, "Should receive at least one message")
	assert.LessOrEqual(s.T(), totalReceived, messageCount, "Should not receive more than sent")
}

// TestWithMultipleConfigurations - тест с разными конфигурациями
func (s *KafkaTestSuite) TestWithMultipleConfigurations() {
	tests := []struct {
		name       string
		configName string
		topic      string
		message    []byte
		shouldWork bool
	}{
		{"Simple message", "test_producer", "test-topic", []byte("test1"), true},
		{"JSON message", "test_producer", "test-topic", []byte(`{"test": true}`), true},
		{"Empty message", "test_producer", "test-topic", []byte(""), true},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			handler := handlers.NewTestHandler(1)
			consumer := s.CreateTestConsumer(handler)
			producer := s.CreateTestProducer()

			err := consumer.ConsumeTopics([]string{tt.topic})
			s.Require().NoError(err)

			time.Sleep(2 * time.Second)

			_, _, err = producer.Send(tt.topic, nil, tt.message)
			if tt.shouldWork {
				s.Require().NoError(err)

				msg, err := handler.WaitForMessage(10 * time.Second)
				s.Require().NoError(err)
				assert.Equal(s.T(), tt.message, msg.Value)
			}

			consumer.Stop()
		})
	}
}
