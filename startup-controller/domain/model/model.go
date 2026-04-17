package model

import (
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// NodeInfo represents a Neo4j node (table) information
type NodeInfo struct {
	SchemaName  string
	TableName   string
	ServiceName string
	TaskID      string           // UUID from task_tracker table
	JobName     string           // K8s-compliant job identifier
	NodeType    pkg_model.NodeType // new
}

// OutboxEntry represents an entry in the outbox table for reliable event publishing
type OutboxEntry struct {
	ID            uuid.UUID `db:"id"`
	AggregateType string    `db:"aggregate_type"`
	AggregateID   uuid.UUID `db:"aggregate_id"`
	EventType     string    `db:"event_type"`
	Payload       []byte    `db:"payload"` // JSON payload
	StreamName    string    `db:"stream_name"`
	CreatedAt     time.Time `db:"created_at"`
	ProcessedAt   *time.Time `db:"processed_at"`
	Status        string    `db:"status"` // 'pending', 'processed', 'failed'
	RetryCount    int       `db:"retry_count"`
	MaxRetries    int       `db:"max_retries"`
	ErrorMessage  *string   `db:"error_message"`
}

// OutboxStatus represents valid states for outbox entries
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusProcessed OutboxStatus = "processed"
	OutboxStatusFailed    OutboxStatus = "failed"
)
