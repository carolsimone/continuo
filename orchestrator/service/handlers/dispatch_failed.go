package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// DispatchFailedParams carries the inputs for EmitDispatchFailed.
//
// StreamName and EventType are NOT parameters: they are constants
// ("run.entries.dispatch_failed:v1" and "run_entries_dispatch_failed")
// owned by EmitDispatchFailed itself.
type DispatchFailedParams struct {
	RunID               string
	ScheduleName        string
	MessageProcessingID uuid.UUID
	Reason              pkgEvents.DispatchFailedReason
}

// EmitDispatchFailed writes one run.entries.dispatch_failed:v1 outbox
// entry. Callers MUST only invoke this when dispatchFailedReason(err)
// returned (reason, true). Calling it for a transient or permanent error
// would terminally fail a run that could otherwise self-heal (transient)
// or mask the upstream signal (permanent).
func EmitDispatchFailed(
	ctx context.Context,
	u uow.UnitOfWork,
	logger *slog.Logger,
	p DispatchFailedParams,
) error {
	scheduleUUID, err := uuid.Parse(p.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", p.RunID, err)
	}

	evt := pkgEvents.RunEntriesDispatchFailed{
		ScheduleID:   p.RunID,
		ScheduleName: p.ScheduleName,
		Reason:       p.Reason,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal run.entries.dispatch_failed payload: %w", err)
	}

	msgProcID := p.MessageProcessingID
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "run_entries_dispatch_failed",
		Payload:             payload,
		StreamName:          streams.RunEntriesDispatchFailedV1,
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write run.entries.dispatch_failed to outbox: %w", err)
	}

	logger.Info("Emitted run.entries.dispatch_failed:v1",
		"run_id", p.RunID,
		"schedule_name", p.ScheduleName,
		"reason", string(p.Reason),
	)
	return nil
}

// dispatchFailedReason maps an error returned by SnapshotService.Snapshot
// to a DispatchFailedReason for the run.entries.dispatch_failed:v1 stream.
//
// Returns (reason, true) only for the two sentinels that express an
// expected "no work to dispatch" outcome:
//   - snapshot.ErrTargetNotFound  -> DispatchFailedReasonTargetNotFound
//   - snapshot.ErrEmptyProjection -> DispatchFailedReasonEmptyProjection
//
// Returns ("", false) for every other error. When false is returned the
// caller MUST propagate err unchanged (typically wrapped with
// fmt.Errorf) so the Redis consumer's existing classification still
// applies: a transient error gets NACKed and reclaimed via XCLAIM; an
// error wrapping events.ErrPermanent gets ACKed and dropped (see
// docs/arch/05-error-classification.md). Synthesising a dispatch_failed
// event for a transient error would terminally fail a recoverable run;
// doing so for a permanent error would be redundant and would mask the
// upstream operator signal.
func dispatchFailedReason(err error) (pkgEvents.DispatchFailedReason, bool) {
	switch {
	case errors.Is(err, snapshot.ErrTargetNotFound):
		return pkgEvents.DispatchFailedReasonTargetNotFound, true
	case errors.Is(err, snapshot.ErrEmptyProjection):
		return pkgEvents.DispatchFailedReasonEmptyProjection, true
	default:
		return "", false
	}
}
