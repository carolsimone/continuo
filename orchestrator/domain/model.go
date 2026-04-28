package domain

import (
	"time"

	"github.com/google/uuid"
)

type Criticality string

const (
	CriticalityUnspecified Criticality = "UNSPECIFIED"
	CriticalityRegulatory  Criticality = "REGULATORY"
	CriticalityCore        Criticality = "CORE"
	CriticalitySecondary   Criticality = "SECONDARY"
)

type NodeStatus string

const (
	NodeStatusPending   NodeStatus = "PENDING"
	NodeStatusRunning   NodeStatus = "RUNNING"
	NodeStatusSucceeded NodeStatus = "SUCCEEDED"
	NodeStatusFailed    NodeStatus = "FAILED"
	NodeStatusSkipped   NodeStatus = "SKIPPED"
)

func (s NodeStatus) IsTerminal() bool {
	return s == NodeStatusSucceeded || s == NodeStatusFailed || s == NodeStatusSkipped
}

type GraphEdge struct {
	FromNodeID string // "{service_name}.{schema_name}.{table_name}"
	ToNodeID   string
}

type RunSummary struct {
	RunID          string
	ScheduleName   string
	TerminalStatus string
	CreatedAt      time.Time
	CompletedAt    time.Time
}

type TableNode struct {
	TableName       string
	SchemaName      string
	ServiceName     string
	Owner           string
	ScheduleName    string
	Criticality     Criticality
	LastUpdatedAt   time.Time
	CreatedAt       time.Time
	NodeType        string
	Status          string
	TaskID          string
	ManifestVersion string
	ImageTag        string
}

type ScheduleGraph struct {
	Nodes []*TableNode
	Edges []*GraphEdge
}

// OutboxEntry represents an event staged for publishing
type OutboxEntry struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"`
	AggregateType       string     `db:"aggregate_type"`
	AggregateID         uuid.UUID  `db:"aggregate_id"`
	EventType           string     `db:"event_type"`
	Payload             []byte     `db:"payload"`
	StreamName          string     `db:"stream_name"`
	CreatedAt           time.Time  `db:"created_at"`
	ProcessedAt         *time.Time `db:"processed_at"`
	Status              string     `db:"status"`
	RetryCount          int        `db:"retry_count"`
	MaxRetries          int        `db:"max_retries"`
	ErrorMessage        *string    `db:"error_message"`
}

// MessageProcessing tracks consumed messages for exactly-once
type MessageProcessing struct {
	ID         uuid.UUID `db:"id"`
	MessageID  string    `db:"message_id"`
	StreamName string    `db:"stream_name"`
	State      string    `db:"state"` // processing, completed, acked
	Payload    []byte    `db:"payload"`
	Error      *string   `db:"error"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}

// PublishedMessage tracks published outbox entries for dedup
type PublishedMessage struct {
	ID             uuid.UUID `db:"id"`
	OutboxEntryID  uuid.UUID `db:"outbox_entry_id"`
	RedisMessageID *string   `db:"redis_message_id"`
	PublishedAt    time.Time `db:"published_at"`
}

// CascadeTaskSkipped is the event payload written to task.status.updated:v1 outbox entries
// for downstream nodes that become unreachable due to an upstream failure.
type CascadeTaskSkipped struct {
	TaskID     string `json:"task_id"`
	ScheduleID string `json:"schedule_id"`
}

// NodeReadyForExecution is the event payload written to query.model:v1 outbox entries
type NodeReadyForExecution struct {
	ScheduleID      string `json:"schedule_id"`
	ScheduleName    string `json:"schedule_name"`
	ServiceName     string `json:"service_name"`
	SchemaName      string `json:"schema_name"`
	TableName       string `json:"table_name"`
	TaskID          string `json:"task_id"`
	JobName         string `json:"job_name"`
	NodeType        string `json:"node_type"`
	ManifestVersion string `json:"manifest_version"`
	ImageTag        string `json:"image_tag"`
}
