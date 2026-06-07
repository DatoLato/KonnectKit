package suite

import (
	"github.com/stretchr/testify/suite"

	"KonnectKit/pkg/config"
	"KonnectKit/pkg/kafka"
	"KonnectKit/test/integration/fixtures"
	"KonnectKit/test/integration/handlers"
	"KonnectKit/test/integration/setup"
)

// KafkaTestSuite - основной тестовый suite
type KafkaTestSuite struct {
	suite.Suite
	container *setup.KafkaContainerWrapper
	client    *kafka.Client
	config    *config.Config
}

// SetupSuite - запускается один раз перед всеми тестами
func (s *KafkaTestSuite) SetupSuite() {
	s.T().Log("Starting Kafka container...")
	s.container = setup.NewKafkaContainer(s.T())

	s.T().Log("Building test configuration...")
	builder := fixtures.NewConfigBuilder(s.container.GetBroker())
	s.config = builder.BuildFullConfig()

	s.T().Log("Creating Kafka client...")
	s.client = kafka.NewClient(s.config)
}

// TearDownSuite - запускается один раз после всех тестов
func (s *KafkaTestSuite) TearDownSuite() {
	s.T().Log("Cleaning up...")
	if s.client != nil {
		s.client.Close()
	}
	if s.container != nil {
		s.container.Close()
	}
}

// SetupTest - запускается перед каждым тестом
func (s *KafkaTestSuite) SetupTest() {
}

// TearDownTest - запускается после каждого теста
func (s *KafkaTestSuite) TearDownTest() {
}

// GetClient - возвращает Kafka клиента
func (s *KafkaTestSuite) GetClient() *kafka.Client {
	return s.client
}

// CreateTestProducer - создание тестового продюсера
func (s *KafkaTestSuite) CreateTestProducer() interface {
	Send(topic string, key, value []byte) (int32, int64, error)
} {
	producer, err := s.client.NewProducer("test_producer")
	s.Require().NoError(err)
	return producer
}

// CreateTestConsumer - создание тестового консюмера
func (s *KafkaTestSuite) CreateTestConsumer(handler *handlers.TestHandler) interface {
	Consume() error
	ConsumeTopics(topics []string) error
	Stop() error
} {
	consumer, err := s.client.NewConsumer("test_consumer", handler)
	s.Require().NoError(err)
	return consumer
}
