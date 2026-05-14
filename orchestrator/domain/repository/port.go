package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/google/uuid"
)

// OutboxRepository defines operations for the outbox table.
type OutboxRepository interface {
	Create(ctx context.Context, entry *domain.OutboxEntry) error
	GetPendingBatch(ctx context.Context, limit int) ([]*domain.OutboxEntry, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
	UpdateStatus(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error
}

// MessageProcessingRepository handles message_processing table operations.
type MessageProcessingRepository interface {
	InsertIfNotExists(ctx context.Context, msgProc *domain.MessageProcessing) (uuid.UUID, bool, error)
	GetByMessageID(ctx context.Context, messageID string) (*domain.MessageProcessing, error)
	UpdateState(ctx context.Context, id uuid.UUID, state string) error
}

// PublishedMessagesRepository handles published_messages table operations.
type PublishedMessagesRepository interface {
	Exists(ctx context.Context, outboxEntryID uuid.UUID) (bool, error)
	Create(ctx context.Context, pm *domain.PublishedMessage) error
}

// CancelledSchedulesRepository tracks schedule IDs that have been cancelled by
// an upstream control-plane signal. Used to short-circuit terminal-state
// processing for already-cancelled runs.
type CancelledSchedulesRepository interface {
	Insert(ctx context.Context, scheduleID uuid.UUID) error
	Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}

// RejectedTopologyRepository writes forensics rows for permanently-rejected
// manifest.loaded:v1 messages. Used from a non-transactional context — the
// consumer ACKs after this call regardless of outcome, so a failed Insert
// must NOT turn a permanent error into a transient one.
type RejectedTopologyRepository interface {
	// Insert writes a forensics row. payload must be valid JSON.
	Insert(ctx context.Context, messageID, reason string, payload json.RawMessage) error
}

// TopologyStateRepository tracks the monotonic topology_generation counter.
type TopologyStateRepository interface {
	IncrementGeneration(ctx context.Context) (int64, error)
	GetGeneration(ctx context.Context) (int64, error)
}

// TopologyRepository is the write/read interface for the topology graph.
// Implementations are responsible for snapshot atomicity and cross-generation
// consistency.
type TopologyRepository interface {
	ApplySnapshot(ctx context.Context, nodes []*topology.TopologyNode, topologyGeneration int64) error
	SetServiceMetadata(ctx context.Context, serviceMetadata map[string]map[string]string, topologyGeneration int64) error
	GetScheduleGraph(ctx context.Context, scheduleName string) ([]*topology.Node, []*topology.UpstreamDependency, error)
}
