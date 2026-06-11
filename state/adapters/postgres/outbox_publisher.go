package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// OutboxPublisher translates run.DomainEvent values to pkg/outbox.Entry rows
// and writes them inside the bound transaction.
// RunDispatchFailed events are informational and produce no outbox row.
type OutboxPublisher struct {
	tx     *sqlx.Tx
	logger *slog.Logger
}

// NewOutboxPublisher constructs the publisher bound to tx. Append writes inside
// tx (which may be nil outside a transaction, in which case Append errors).
func NewOutboxPublisher(tx *sqlx.Tx, logger *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{tx: tx, logger: logger}
}

// Append writes one outbox entry per event into the bound transaction.
// The per-event mapping (stream_name, event_type, aggregate_type, retry budget,
// payload shape) is defined in translateRunEvent. RunDispatchFailed is skipped
// (no outbox row). An unknown event type returns an error.
func (p *OutboxPublisher) Append(ctx context.Context, events []run.DomainEvent, msgProcID uuid.UUID) error {
	if p.tx == nil {
		return fmt.Errorf("Append requires an active transaction")
	}
	repo := pkgoutbox.NewPostgresRepository(p.tx, "state_outbox", p.logger)
	for _, evt := range events {
		entry, skip, err := translateRunEvent(evt, msgProcID)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		if err := repo.Create(ctx, entry); err != nil {
			return fmt.Errorf("create outbox entry for %T: %w", evt, err)
		}
	}
	return nil
}

// translateRunEvent maps one run.DomainEvent to a pkg/outbox.Entry.
// Returns (entry, false, nil) on success, (nil, true, nil) when the event
// produces no row (RunDispatchFailed), and (nil, false, err) on marshal failure
// or an unknown event type.
func translateRunEvent(evt run.DomainEvent, msgProcID uuid.UUID) (*pkgoutbox.Entry, bool, error) {
	var msgProcPtr *uuid.UUID
	if msgProcID != uuid.Nil {
		mp := msgProcID
		msgProcPtr = &mp
	}

	switch e := evt.(type) {
	case run.RunStarted:
		payload, err := json.Marshal(map[string]interface{}{
			"runner_id":        e.ID.String(),
			"schedule_name":    e.Name,
			"service_metadata": toServiceMetadataDTOs(e.ServiceMetadata),
			"kind":             string(e.K),
			"source_run_id":    sourceIDStr(e.SourceID),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "scheduler_started", streams.SchedulerStartedV1, payload, 3, msgProcPtr), false, nil

	case run.RunFinalized:
		payload, err := json.Marshal(pkgevents.RunFinalized{
			ScheduleID:   e.ID.String(),
			ScheduleName: e.Name,
			Status:       string(e.Outcome),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler_tracker", "run.finalized:v1", streams.RunFinalizedV1, payload, 5, msgProcPtr), false, nil

	case run.RunCancelled:
		payload, err := json.Marshal(map[string]string{
			"schedule_id":   e.ID.String(),
			"schedule_name": e.Name,
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "schedule_cancelled", streams.ScheduleCancelledV1, payload, 3, msgProcPtr), false, nil

	case run.RerunRequested:
		payload, err := json.Marshal(map[string]string{
			"schedule_id":   e.ID.String(),
			"schedule_name": e.Name,
			"kind":          "rerun",
			"source_run_id": e.SourceID.String(),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "rerun", streams.TriggerRerunV1, payload, 3, msgProcPtr), false, nil

	case run.RebaseRequested:
		payload, err := json.Marshal(map[string]string{
			"schedule_id":   e.ID.String(),
			"schedule_name": e.Name,
			"kind":          "rebase",
			"source_run_id": e.SourceID.String(),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "rebase", streams.TriggerRebaseV1, payload, 3, msgProcPtr), false, nil

	case run.SingleNodeRunRequested:
		payload, err := json.Marshal(map[string]string{
			"schedule_id":     e.ID.String(),
			"schedule_name":   e.Name,
			"service_name":    e.Target.ServiceName,
			"schema_name":     e.Target.SchemaName,
			"table_name":      e.Target.TableName,
			"kind":            "single_node_run",
			"metadata_source": string(e.MetadataSource),
			"source_run_id":   sourceIDStr(e.SourceID),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "single_node_run", streams.TriggerSingleNodeRunV1, payload, 3, msgProcPtr), false, nil

	case run.RunDispatchFailed:
		// Informational event — no downstream stream yet; omit from outbox.
		return nil, true, nil

	default:
		return nil, false, fmt.Errorf("OutboxPublisher: unknown event type %T", evt)
	}
}

// buildEntry constructs a pkg/outbox.Entry with a generated ID and current timestamp.
func buildEntry(
	aggregateID uuid.UUID,
	aggregateType, eventType, streamName string,
	payload []byte,
	maxRetries int,
	msgProcID *uuid.UUID,
) *pkgoutbox.Entry {
	return &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: msgProcID,
		AggregateType:       aggregateType,
		AggregateID:         aggregateID,
		EventType:           eventType,
		Payload:             payload,
		StreamName:          streamName,
		Status:              "pending",
		MaxRetries:          maxRetries,
		RetryCount:          0,
	}
}

// sourceIDStr converts an optional UUID pointer to its string representation,
// returning an empty string when nil.
func sourceIDStr(p *uuid.UUID) string {
	if p == nil {
		return ""
	}
	return p.String()
}

// Compile-time assertion: OutboxPublisher satisfies ports.OutboxPublisher.
var _ ports.OutboxPublisher = (*OutboxPublisher)(nil)
