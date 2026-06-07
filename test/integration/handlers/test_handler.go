package handlers

import (
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// TestHandler - тестовый обработчик с синхронизацией
type TestHandler struct {
	msgChan  chan *sarama.ConsumerMessage
	mu       sync.RWMutex
	messages []*sarama.ConsumerMessage
	errors   []error
}

// NewTestHandler - создание нового тестового обработчика
func NewTestHandler(bufferSize int) *TestHandler {
	return &TestHandler{
		msgChan:  make(chan *sarama.ConsumerMessage, bufferSize),
		messages: make([]*sarama.ConsumerMessage, 0),
		errors:   make([]error, 0),
	}
}

// HandleMessage - реализация интерфейса ConsumerHandler
func (h *TestHandler) HandleMessage(msg *sarama.ConsumerMessage) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.messages = append(h.messages, msg)

	select {
	case h.msgChan <- msg:
	default:
	}

	return nil
}

// HandleError - обработка ошибок
func (h *TestHandler) HandleError(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errors = append(h.errors, err)
}

// GetMessages - возвращает все полученные сообщения
func (h *TestHandler) GetMessages() []*sarama.ConsumerMessage {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.messages
}

// GetMessageCount - возвращает количество полученных сообщений
func (h *TestHandler) GetMessageCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.messages)
}

// WaitForMessage - ожидает сообщение с таймаутом
func (h *TestHandler) WaitForMessage(timeout time.Duration) (*sarama.ConsumerMessage, error) {
	select {
	case msg := <-h.msgChan:
		return msg, nil
	case <-time.After(timeout):
		return nil, ErrTimeout
	}
}

// WaitForMessages - ожидает N сообщений с таймаутом
func (h *TestHandler) WaitForMessages(count int, timeout time.Duration) ([]*sarama.ConsumerMessage, error) {
	messages := make([]*sarama.ConsumerMessage, 0, count)
	timeoutCh := time.After(timeout)

	for len(messages) < count {
		select {
		case msg := <-h.msgChan:
			messages = append(messages, msg)
		case <-timeoutCh:
			return messages, ErrTimeout
		}
	}

	return messages, nil
}

// Clear - очищает полученные сообщения
func (h *TestHandler) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = make([]*sarama.ConsumerMessage, 0)
	h.errors = make([]error, 0)

	// Drain channel
	for len(h.msgChan) > 0 {
		<-h.msgChan
	}
}

// ErrTimeout - ошибка таймаута
var ErrTimeout = fmt.Errorf("timeout waiting for messages")
