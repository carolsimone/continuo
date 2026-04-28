package model

import (
	"time"

	"github.com/google/uuid"
)

// DeploymentOutboxEntry represents an entry in the deployment_outbox table
// for reliable K8s Job deployment using the outbox pattern
type DeploymentOutboxEntry struct {
	ID             uuid.UUID  `db:"id"`
	TaskID         uuid.UUID  `db:"task_id"`
	ScheduleID     uuid.UUID  `db:"schedule_id"`
	ScheduleName   string     `db:"schedule_name"`
	ServiceName    string     `db:"service_name"`
	SchemaName     string     `db:"schema_name"`
	TableName      string     `db:"table_name"`
	JobName        string     `db:"job_name"`
	NodeType       string     `db:"node_type"`
	ImageTag       string     `db:"image_tag"`
	TaskRetryCount    int        `db:"task_retry_count"`    // task-level retry count (k8s retries)
	TaskMaxRetries    int        `db:"task_max_retries"`    // maximum k8s retries allowed
	Status            string     `db:"status"`              // 'pending', 'processed', 'failed'
	CreatedAt         time.Time  `db:"created_at"`
	ProcessedAt       *time.Time `db:"processed_at"`
	OutboxRetryCount  int        `db:"retry_count"`         // outbox delivery attempts
	OutboxMaxRetries  int        `db:"max_retries"`         // max outbox delivery attempts
	ErrorMessage   *string    `db:"error_message"`
}

// OutboxStatus represents valid states for outbox entries
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusProcessed OutboxStatus = "processed"
	OutboxStatusFailed    OutboxStatus = "failed"
)
