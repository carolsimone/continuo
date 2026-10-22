package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// OutboxTable is the outbox table this service writes its events to and its
// relay publishes from. Its insert trigger notifies the Postgres channel of the
// same name, on which the relay's waker listens.
const OutboxTable = "state_outbox"

// OutboxPublisher translates run.DomainEvent values to pkg/outbox.Entry rows
// and writes them inside the bound transaction.
// RunDispatchTerminal events are informational and produce no outbox row.
type OutboxPublisher struct {
	tx     *sqlx.Tx
	logger *slog.Logger
}

// NewOutboxPublisher constructs the publisher bound to tx. Append writes inside
// tx (which may be nil outside a transaction, in which case Append errors).
func NewOutboxPublisher(tx *sqlx.Tx, logger *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{tx: tx, logger: logger}
}

// Append writes one outbox entry per event into the bound transaction, in
// event order, with as few INSERTs as pkg/outbox.CreateAll needs. The per-event
// mapping (stream_name, event_type, aggregate_type, payload shape) is defined
// in translateRunEvent. RunDispatchTerminal is omitted (no outbox row). An
// unknown event type returns an error and writes nothing.
func (p *OutboxPublisher) Append(ctx context.Context, events []run.DomainEvent, msgProcID uuid.UUID) error {
	if p.tx == nil {
		return fmt.Errorf("Append requires an active transaction")
	}
	entries := make([]*pkgoutbox.Entry, 0, len(events))
	for _, evt := range events {
		entry, skip, err := translateRunEvent(evt, msgProcID)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil
	}
	if err := pkgoutbox.CreateAll(ctx, p.tx, OutboxTable, entries); err != nil {
		return fmt.Errorf("append %d outbox entries: %w", len(entries), err)
	}
	return nil
}

// translateRunEvent maps one run.DomainEvent to a pkg/outbox.Entry.
// Returns (entry, false, nil) on success, (nil, true, nil) when the event
// produces no row (RunFinalized, RunDispatchTerminal), and (nil, false, err) on
// marshal failure or an unknown event type.
func translateRunEvent(evt run.DomainEvent, msgProcID uuid.UUID) (*pkgoutbox.Entry, bool, error) {
	var msgProcPtr *uuid.UUID
	if msgProcID != uuid.Nil {
		mp := msgProcID
		msgProcPtr = &mp
	}

	switch e := evt.(type) {
	case run.RunFinalized:
		// A run's terminal status is its row; no stream carries it.
		return nil, true, nil
	case run.RunCancelled:
		payload, err := json.Marshal(map[string]string{
			"schedule_id":   e.ID.String(),
			"schedule_name": e.Name,
			"cancelled_by":  identity.OrSystem(e.By),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "schedule_cancelled", streams.ScheduleCancelledV1, payload, msgProcPtr), false, nil

	case run.TaskDispatched:
		payload, err := json.Marshal(pkgevents.TaskDispatch{
			RunID:          e.ID.String(),
			TaskID:         e.TaskID.String(),
			Attempt:        e.Attempt,
			KnownStatus:    string(e.KnownStatus),
			ScheduleName:   e.ScheduleName,
			NodeKey:        string(e.Node.Key),
			ServiceName:    e.Node.ServiceName,
			SchemaName:     e.Node.SchemaName,
			TableName:      e.Node.TableName,
			NodeType:       e.Node.NodeType,
			Operation:      string(e.Operation),
			ImageTag:       e.Node.ImageTag,
			SecretRef:      e.Node.SecretRef,
			JobName:        e.JobName,
			TimeoutSeconds: int64(e.Timeout / time.Second),
		})
		if err != nil {
			return nil, false, err
		}
		// The row's event_type is the stream name; the publisher renders the
		// envelope from the row. Every dispatch of a run shares the run as its
		// aggregate, so the relay publishes them in the order they were
		// written.
		return buildEntry(e.ID, "scheduler", streams.TaskDispatchV1, streams.TaskDispatchV1, payload, msgProcPtr), false, nil
	case run.RunDispatchTerminal:
		// RunDispatchTerminal is informational and maps to no stream.
		// The activation handler logs its reason.
		return nil, true, nil

	default:
		return nil, false, fmt.Errorf("OutboxPublisher: unknown event type %T", evt)
	}
}

// buildEntry constructs a pending pkg/outbox.Entry with a generated ID.
func buildEntry(
	aggregateID uuid.UUID,
	aggregateType, eventType, streamName string,
	payload []byte,
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
		RetryCount:          0,
	}
}

// Compile-time assertion: OutboxPublisher satisfies ports.OutboxPublisher.
var _ ports.OutboxPublisher = (*OutboxPublisher)(nil)
