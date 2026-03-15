package main

import (
	"fmt"
	"kafkaLib/pkg/config"
	"kafkaLib/pkg/kafka"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// UserEventHandler - пример обработчика сообщений
type UserEventHandler struct{}

func (h *UserEventHandler) HandleMessage(msg *sarama.ConsumerMessage) error {
	fmt.Printf("Received message: topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	return nil
}

func main() {
	// Загружаем конфигурацию
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Создаем клиент
	client := kafka.NewClient(cfg)
	defer client.Close()

	// ===== ПРИМЕР 1: Продюсер с топиком из конфига =====
	fmt.Println("\n=== Testing User Events Producer ===")
	userProducer, err := client.NewProducer("user_events_producer")
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	// Отправляем в топик по умолчанию (user-events)
	partition, offset, err := userProducer.SendDefault(
		[]byte("user123"),
		[]byte("user logged in"),
	)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	} else {
		fmt.Printf("Sent to default topic: partition=%d, offset=%d\n", partition, offset)
	}

	// Отправляем в другой настроенный топик
	partition, offset, err = userProducer.Send("user-errors",
		[]byte("user123"),
		[]byte("payment failed"),
	)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	} else {
		fmt.Printf("Sent to user-errors: partition=%d, offset=%d\n", partition, offset)
	}

	// ===== ПРИМЕР 2: Продюсер для заказов =====
	fmt.Println("\n=== Testing Order Producer ===")
	orderProducer, err := client.NewProducer("order_processor")
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	// Отправляем заказ
	partition, offset, err = orderProducer.Send("orders",
		[]byte("order456"),
		[]byte(`{"id": "456", "total": 100.50, "status": "new"}`),
	)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	} else {
		fmt.Printf("Order sent: partition=%d, offset=%d\n", partition, offset)
	}

	// ===== ПРИМЕР 3: Консюмер с подпиской из конфига =====
	fmt.Println("\n=== Testing User Events Consumer ===")
	userHandler := &UserEventHandler{}
	userConsumer, err := client.NewConsumer("user_events_consumer", userHandler)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Автоматически подписывается на топики из конфига
	err = userConsumer.Consume()
	if err != nil {
		log.Printf("Failed to start consumer: %v", err)
	}

	// ===== ПРИМЕР 4: Консюмер с ручным указанием топиков =====
	fmt.Println("\n=== Testing Order Consumer ===")
	orderHandler := &UserEventHandler{}
	orderConsumer, err := client.NewConsumer("order_consumer", orderHandler)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Можно подписаться на все топики из конфига
	err = orderConsumer.Consume()
	if err != nil {
		log.Printf("Failed to start consumer: %v", err)
	}

	// Даем время для получения сообщений
	fmt.Println("\nWaiting for messages...")
	time.Sleep(10 * time.Second)

	// Останавливаем консюмеры
	fmt.Println("\nStopping consumers...")
	if err := userConsumer.Stop(); err != nil {
		log.Printf("Error stopping user consumer: %v", err)
	}
	if err := orderConsumer.Stop(); err != nil {
		log.Printf("Error stopping order consumer: %v", err)
	}

	fmt.Println("Done!")
}
