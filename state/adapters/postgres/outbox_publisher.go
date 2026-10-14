package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

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
// produces no row (RunDispatchTerminal), and (nil, false, err) on marshal failure
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
			"initiated_by":     identity.OrSystem(e.InitiatedBy),
			"operation":        string(e.Operation),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "scheduler_started", streams.SchedulerStartedV1, payload, msgProcPtr), false, nil

	case run.RunFinalized:
		payload, err := json.Marshal(pkgevents.RunFinalized{
			ScheduleID:   e.ID.String(),
			ScheduleName: e.Name,
			Status:       string(e.Outcome),
		})
		if err != nil {
			return nil, false, err
		}
		// The row's event_type is the stream name; the publisher routes every
		// row by stream_name and reads event_type only to spot dead letters.
		return buildEntry(e.ID, "scheduler_tracker", streams.RunFinalizedV1, streams.RunFinalizedV1, payload, msgProcPtr), false, nil

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

	case run.RerunRequested:
		payload, err := json.Marshal(map[string]string{
			"schedule_id":   e.ID.String(),
			"schedule_name": e.Name,
			"kind":          "rerun",
			"source_run_id": e.SourceID.String(),
			"initiated_by":  identity.OrSystem(e.InitiatedBy),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "rerun", streams.TriggerRerunV1, payload, msgProcPtr), false, nil

	case run.RebaseRequested:
		payload, err := json.Marshal(map[string]string{
			"schedule_id":   e.ID.String(),
			"schedule_name": e.Name,
			"kind":          "rebase",
			"source_run_id": e.SourceID.String(),
			"initiated_by":  identity.OrSystem(e.InitiatedBy),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "rebase", streams.TriggerRebaseV1, payload, msgProcPtr), false, nil

	case run.SingleNodeRunRequested:
		payload, err := json.Marshal(map[string]string{
			"schedule_id":     e.ID.String(),
			"schedule_name":   e.Name,
			"service_name":    e.Target.ServiceName,
			"schema_name":     e.Target.SchemaName,
			"table_name":      e.Target.TableName,
			"kind":            "single_node_run",
			"metadata_source": string(e.MetadataSource),
			"operation":       string(e.Operation),
			"source_run_id":   sourceIDStr(e.SourceID),
			"initiated_by":    identity.OrSystem(e.InitiatedBy),
		})
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "single_node_run", streams.TriggerSingleNodeRunV1, payload, msgProcPtr), false, nil

	case run.PromotedSeedsRunRequested:
		nodes := make([]map[string]string, 0, len(e.Nodes))
		for _, n := range e.Nodes {
			nodes = append(nodes, map[string]string{
				"service_name": n.ServiceName,
				"schema_name":  n.SchemaName,
				"table_name":   n.TableName,
				"node_type":    n.NodeType,
				"image_tag":    n.ImageTag,
			})
		}
		fields := map[string]any{
			"schedule_id":   e.ID.String(),
			"schedule_name": e.Name,
			"release_id":    e.ReleaseID,
			"kind":          "promote_seed",
			"nodes":         nodes,
			"initiated_by":  "system",
		}
		// promotion_seq is the live promotion seq the run was planned against;
		// orchestrator projects the run only once its graph has reached it.
		if e.PromotionSeq > 0 {
			fields["promotion_seq"] = strconv.FormatInt(e.PromotionSeq, 10)
		}
		payload, err := json.Marshal(fields)
		if err != nil {
			return nil, false, err
		}
		return buildEntry(e.ID, "scheduler", "promote_seed", streams.TriggerPromotedSeedsV1, payload, msgProcPtr), false, nil

	case run.TaskDispatched:
		payload, err := json.Marshal(pkgevents.QueryModel{
			ScheduleID:   e.ID.String(),
			ScheduleName: e.ScheduleName,
			TaskID:       e.TaskID.String(),
			ServiceName:  e.Node.ServiceName,
			SchemaName:   e.Node.SchemaName,
			TableName:    e.Node.TableName,
			JobName:      e.JobName,
			NodeType:     e.Node.NodeType,
			ImageTag:     e.Node.ImageTag,
			SecretRef:    e.Node.SecretRef,
			Operation:    string(e.Operation),
			MaxRetries:   e.MaxRetries,
		}.Fields())
		if err != nil {
			return nil, false, err
		}
		// The row's event_type is the stream name, as for run.finalized:v1;
		// every dispatch of a run shares the run as its aggregate, so the relay
		// publishes them in the order they were written.
		return buildEntry(e.ID, "scheduler", streams.QueryModelV1, streams.QueryModelV1, payload, msgProcPtr), false, nil
	case run.RunDispatchTerminal:
		// RunDispatchTerminal is informational and maps to no stream.
		// The dispatch handlers log its reason.
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
