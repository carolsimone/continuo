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
	ID                  uuid.UUID
	MessageProcessingID *uuid.UUID
	AggregateType       string
	AggregateID         uuid.UUID
	EventType           string
	Payload             []byte
	StreamName          string
	CreatedAt           time.Time
	ProcessedAt         *time.Time
	Status              string
	RetryCount          int
	MaxRetries          int
	ErrorMessage        *string
}

// MessageProcessing tracks consumed messages for exactly-once
type MessageProcessing struct {
	ID         uuid.UUID
	MessageID  string
	StreamName string
	State      string // processing, completed, acked
	Payload    []byte
	Error      *string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// PublishedMessage tracks published outbox entries for dedup
type PublishedMessage struct {
	ID             uuid.UUID
	OutboxEntryID  uuid.UUID
	RedisMessageID *string
	PublishedAt    time.Time
}

// CascadeTaskSkipped is the event payload written to task.status.updated:v1 outbox entries
// for downstream nodes that become unreachable due to an upstream failure.
type CascadeTaskSkipped struct {
	TaskID     string
	ScheduleID string
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
