// executor-controller/adapters/redis/job_terminal_parser.go
package redis

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// terminalStatuses are the Job statuses that settle a Job. A status outside this
// set does not end the Job's occupancy of its execution slot, so it must never
// release one.
var terminalStatuses = map[string]bool{
	"succeeded": true,
	"failed":    true,
	"unknown":   true,
}

// ParseExecutorJobTerminal translates an executor.job.terminal:v1 XMessage into
// a typed domain event. The wire format is a single "payload" field carrying the
// JSON body (matching k8s-controller's outbox publisher); outbox_entry_id rides
// alongside as provenance.
//
// All errors are permanent (malformed input never becomes valid on retry); the
// binding wraps them with events.ErrPermanent so the consumer ACKs rather than
// redelivering forever. The deployment id must name a row and the status must
// genuinely settle the Job, because either one being wrong would release a slot
// that is still in use — or none at all.
func ParseExecutorJobTerminal(msg goredis.XMessage) (events.ExecutorJobTerminal, error) {
	raw := stringField(msg.Values, "payload")
	if raw == "" {
		return events.ExecutorJobTerminal{}, fmt.Errorf("missing payload field")
	}

	var dto pkgevents.ExecutorJobTerminal
	if err := json.Unmarshal([]byte(raw), &dto); err != nil {
		return events.ExecutorJobTerminal{}, fmt.Errorf("unmarshal payload: %w", err)
	}

	deploymentID, err := uuid.Parse(dto.ExecutorDeploymentID)
	if err != nil {
		return events.ExecutorJobTerminal{}, fmt.Errorf("invalid executor_deployment_id: %w", err)
	}
	if !terminalStatuses[dto.TerminalStatus] {
		return events.ExecutorJobTerminal{},
			fmt.Errorf("invalid terminal_status %q (want \"succeeded\", \"failed\" or \"unknown\")", dto.TerminalStatus)
	}

	// A Job that never ran reports no completion instant. The slot is released
	// either way, so an absent value is valid and stays zero.
	var completedAt time.Time
	if dto.CompletedAt != "" {
		completedAt, err = time.Parse(time.RFC3339Nano, dto.CompletedAt)
		if err != nil {
			return events.ExecutorJobTerminal{}, fmt.Errorf("invalid completed_at: %w", err)
		}
	}

	// outbox_entry_id is the k8s-controller outbox row ID, carried as provenance.
	// Absent or empty → uuid.Nil (dedup falls back to (msg.ID, stream_name)).
	// Present-but-malformed → permanent error.
	var outboxEntryID uuid.UUID
	if s := stringField(msg.Values, "outbox_entry_id"); s != "" {
		outboxEntryID, err = uuid.Parse(s)
		if err != nil {
			return events.ExecutorJobTerminal{}, fmt.Errorf("invalid outbox_entry_id: %w", err)
		}
	}

	return events.ExecutorJobTerminal{
		OutboxEntryID:        outboxEntryID,
		ExecutorDeploymentID: deploymentID,
		JobName:              dto.JobName,
		TerminalStatus:       dto.TerminalStatus,
		CompletedAt:          completedAt,
	}, nil
}
