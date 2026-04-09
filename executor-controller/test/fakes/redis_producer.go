package fakes

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// PublishedMessage represents a message published to Redis
type PublishedMessage struct {
	Values map[string]interface{}
}

// FakeRedisProducer is a fake implementation of Redis Producer for testing
type FakeRedisProducer struct {
	mu              sync.RWMutex
	publishedMsgs   []PublishedMessage
	publishErr      error
	messageIDPrefix string
}

// NewFakeRedisProducer creates a new FakeRedisProducer
func NewFakeRedisProducer() *FakeRedisProducer {
	return &FakeRedisProducer{
		publishedMsgs:   make([]PublishedMessage, 0),
		messageIDPrefix: "fake-msg-",
	}
}

// SetPublishError sets an error to be returned by Publish
func (f *FakeRedisProducer) SetPublishError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishErr = err
}

// Publish records a published message
func (f *FakeRedisProducer) Publish(ctx context.Context, values map[string]interface{}) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.publishErr != nil {
		return "", f.publishErr
	}

	// Deep copy values to avoid mutation
	valuesCopy := make(map[string]interface{})
	for k, v := range values {
		valuesCopy[k] = v
	}

	f.publishedMsgs = append(f.publishedMsgs, PublishedMessage{
		Values: valuesCopy,
	})

	messageID := fmt.Sprintf("%s%s", f.messageIDPrefix, uuid.New().String()[:8])
	return messageID, nil
}

// GetPublishedMessages returns all published messages
func (f *FakeRedisProducer) GetPublishedMessages() []PublishedMessage {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Return a copy
	msgs := make([]PublishedMessage, len(f.publishedMsgs))
	copy(msgs, f.publishedMsgs)
	return msgs
}

// GetMessageCount returns the number of published messages
func (f *FakeRedisProducer) GetMessageCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.publishedMsgs)
}

// Reset clears all published messages and errors
func (f *FakeRedisProducer) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishedMsgs = make([]PublishedMessage, 0)
	f.publishErr = nil
}
