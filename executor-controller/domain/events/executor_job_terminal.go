package events

import (
	"time"

	"github.com/google/uuid"
)

// ExecutorJobTerminal is the typed executor.job.terminal:v1 event: the
// Kubernetes Job created for ExecutorDeploymentID has settled, so the execution
// slot that deployment reserved can be released.
//
// It carries capacity accounting only. The Job's business outcome — task status,
// per-node validation result, retry — arrives on its own stream and is handled
// there; nothing about this event advances task or node lifecycle state.
//
// OutboxEntryID is the producing k8s-controller outbox row, carried as
// provenance for dedup with an outbox_entry_id fallback. Zero value (uuid.Nil)
// means the inbound message did not carry the field.
type ExecutorJobTerminal struct {
	OutboxEntryID        uuid.UUID
	ExecutorDeploymentID uuid.UUID
	JobName              string
	TerminalStatus       string
	// CompletedAt is when the Job settled. Zero when the Job reported no
	// completion instant, e.g. one that vanished before running.
	CompletedAt time.Time
}
