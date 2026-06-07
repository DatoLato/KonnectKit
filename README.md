# KonnectKit

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue)](https://golang.org/)
[![Kafka Version](https://img.shields.io/badge/Kafka-2.6%2B-231F20)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**KonnectKit** — это гибкая Golang библиотека для работы с Apache Kafka. Она предоставляет простое API для продюсеров и консюмеров с декларативной настройкой через YAML-конфигурацию.

Ключевая особенность — возможность определять несколько продюсеров и консюмеров с разными профилями (топики, таймауты, стратегии партиционирования) в одном файле `config.yaml`.

## Возможности

- **YAML-конфигурация**: Вся настройка брокеров, топиков, таймаутов и стратегий — в одном файле.
- **Множественные продюсеры и консюмеры**: Создавайте и используйте разных клиентов с разными настройками.
- **Безопасность и валидация**: Продюсер может быть ограничен набором топиков, а консюмер — подписан только на разрешенные.
- **Гибкие таймауты**: Настройка `dial_timeout`, `read_timeout`, `write_timeout` для каждого клиента.
- **Стратегии партиционирования**: Поддержка `hash`, `round-robin`, `random` для продюсера.
- **Стандартные offset'ы**: `oldest` или `newest` для консюмера.

## 📦 Установка

```bash
go get github.com/DatoLato/KonnectKit
```

## Быстрый старт

`Создайте конфигурационный файл config.yaml`

```yaml
kafka:
  base:
    brokers:
      - localhost:9092
    timeout: 30s
    dial_timeout: 5s
    read_timeout: 10s
    write_timeout: 10s

  producers:
    order_producer:
      topic: orders  # топик по умолчанию
      topics:
        - name: orders
          default_topic: true
        - name: order-events
      required_acks: 1
      max_retries: 3
      compression: snappy
      partitioner: hash

  consumers:
    order_consumer:
      topics:
        - name: orders
        - name: order-events
      group_id: order-group
      auto_commit: true
      start_offset: newest
```
## Пример использования

```go
package main

import (
    "fmt"
    "log"
    "github.com/DatoLato/KonnectKit/pkg/config"
    "github.com/DatoLato/KonnectKit/pkg/kafka"
    "github.com/IBM/sarama"
)

// Обработчик сообщений для консюмера
type MyHandler struct{}

func (h *MyHandler) HandleMessage(msg *sarama.ConsumerMessage) error {
    fmt.Printf("Got message: %s = %s\n", msg.Topic, string(msg.Value))
    return nil
}

func main() {
    // 1. Загружаем конфиг
    cfg, err := config.LoadConfig("config.yaml")
    if err != nil {
        log.Fatal(err)
    }

    // 2. Создаем клиент
    client := kafka.NewClient(cfg)
    defer client.Close()

    // 3. Создаем продюсера и отправляем сообщение
    producer, _ := client.NewProducer("order_producer")
    producer.SendDefault([]byte("key-123"), []byte(`{"order": "123", "status": "new"}`))

    // 4. Создаем консюмера и начинаем слушать
    handler := &MyHandler{}
    consumer, _ := client.NewConsumer("order_consumer", handler)
    consumer.Consume()

    // Даем поработать
    select {}
}
```

## Важно!!!

* Версия Kafka: 2.6+
* Go: 1.21+

## Как внести вклад

Мы приветствуем любые улучшения! Форкайте репозиторий, создавайте pull request и отправляйте issues с идеями или багами.

* Форкните проект
* Создайте ветку для фичи (git checkout -b feature/amazing-feature)
* Зафиксируйте изменения (git commit -m 'Add amazing feature')
* Запушьте ветку (git push origin feature/amazing-feature)
* Откройте Pull Request