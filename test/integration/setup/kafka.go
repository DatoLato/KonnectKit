package setup

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

// KafkaContainerWrapper - обертка над Kafka контейнером
type KafkaContainerWrapper struct {
	Container *kafka.KafkaContainer
	Brokers   []string
	ctx       context.Context
}

// NewKafkaContainer - создает и запускает новый Kafka контейнер
func NewKafkaContainer(t *testing.T) *KafkaContainerWrapper {
	ctx := context.Background()

	container, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err)

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)

	t.Logf("Kafka container started: %v", brokers)

	return &KafkaContainerWrapper{
		Container: container,
		Brokers:   brokers,
		ctx:       ctx,
	}
}

// Close - закрывает и удаляет контейнер
func (w *KafkaContainerWrapper) Close() {
	if err := testcontainers.TerminateContainer(w.Container); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}
}

// GetBroker - возвращает первый адрес брокера
func (w *KafkaContainerWrapper) GetBroker() string {
	if len(w.Brokers) == 0 {
		return ""
	}
	return w.Brokers[0]
}
