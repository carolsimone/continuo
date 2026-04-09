package model

import (
	"time"

	"github.com/google/uuid"
)

// DeploymentOutboxEntry represents an entry in the deployment_outbox table
// for reliable K8s Job deployment using the outbox pattern
type DeploymentOutboxEntry struct {
	ID           uuid.UUID  `db:"id"`
	TaskID       uuid.UUID  `db:"task_id"`
	ScheduleID   uuid.UUID  `db:"schedule_id"`
	ScheduleName string     `db:"schedule_name"`
	ServiceName  string     `db:"service_name"`
	Schema       string     `db:"schema_name"`
	TableName    string     `db:"table_name"`
	JobName      string     `db:"job_name"`
	NodeType     string     `db:"node_type"`
	Status       string     `db:"status"` // 'pending', 'processed', 'failed'
	CreatedAt    time.Time  `db:"created_at"`
	ProcessedAt  *time.Time `db:"processed_at"`
	RetryCount   int        `db:"retry_count"`
	MaxRetries   int        `db:"max_retries"`
	ErrorMessage *string    `db:"error_message"`
}

// OutboxStatus represents valid states for outbox entries
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusProcessed OutboxStatus = "processed"
	OutboxStatusFailed    OutboxStatus = "failed"
)
