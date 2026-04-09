package model

import (
	"time"

	"github.com/google/uuid"
)

// DownstreamNode represents a downstream table node ready for execution
type DownstreamNode struct {
	ServiceName string
	Schema      string
	TableName   string
	NodeType    string // raw string from Cypher; validated in ProcessStatusHandler
}

// OutboxEntry represents an entry in the outbox table for reliable event publishing
type OutboxEntry struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"` // Link to source message
	AggregateType       string     `db:"aggregate_type"`
	AggregateID         uuid.UUID  `db:"aggregate_id"`
	EventType           string     `db:"event_type"`
	Payload             []byte     `db:"payload"` // JSON payload
	StreamName          string     `db:"stream_name"`
	CreatedAt           time.Time  `db:"created_at"`
	ProcessedAt         *time.Time `db:"processed_at"`
	Status              string     `db:"status"` // 'pending', 'processed', 'failed'
	RetryCount          int        `db:"retry_count"`
	MaxRetries          int        `db:"max_retries"`
	ErrorMessage        *string    `db:"error_message"`
}

// OutboxStatus represents valid states for outbox entries
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusProcessed OutboxStatus = "processed"
	OutboxStatusFailed    OutboxStatus = "failed"
)

// MessageProcessingState represents the processing state of a consumed message
type MessageProcessingState string

const (
	MessageProcessingStateProcessing MessageProcessingState = "processing"
	MessageProcessingStateCompleted  MessageProcessingState = "completed"
	MessageProcessingStateAcked      MessageProcessingState = "acked"
)

// MessageProcessing tracks consumed Redis messages for exactly-once processing
type MessageProcessing struct {
	ID         uuid.UUID              `db:"id"`
	MessageID  string                 `db:"message_id"`  // Redis Stream message ID
	StreamName string                 `db:"stream_name"` // e.g., "update.table:v1"
	State      MessageProcessingState `db:"state"`       // processing, completed, acked
	Payload    []byte                 `db:"payload"`     // JSONB - full message for debugging
	Error      *string                `db:"error"`       // Error message if processing failed
	CreatedAt  time.Time              `db:"created_at"`
	UpdatedAt  time.Time              `db:"updated_at"`
}

// PublishedMessage tracks successfully published outbox entries
type PublishedMessage struct {
	ID             uuid.UUID `db:"id"`
	OutboxEntryID  uuid.UUID `db:"outbox_entry_id"`  // References outbox.id
	RedisMessageID *string   `db:"redis_message_id"` // Message ID from Redis XADD
	PublishedAt    time.Time `db:"published_at"`
}
