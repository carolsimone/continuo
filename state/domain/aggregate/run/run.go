package run

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/google/uuid"
)

// Run is the aggregate root. State is unexported and mutated only through
// methods on this type. The repository adapter uses the package-internal
// HydrateRun constructor to rebuild a Run from persisted rows; application
// code never calls Hydrate.
type Run struct {
	scheduleID   uuid.UUID
	scheduleName string

	// scheduler_tracker columns
	status      SchedulerStatus
	initStatus  InitStatus
	kind        Kind
	sourceRunID *uuid.UUID
	// initiatedBy records the user who triggered this run, or the "system"
	// sentinel for cron / platform-initiated runs. Stamped at creation and
	// immutable thereafter.
	initiatedBy string
	// operation is the dbt verb this run applies to its nodes (run/test/build).
	// Stamped at construction; stays with the run so its child tasks can be
	// stamped identically at dispatch.
	operation          model.Operation
	createdAt          time.Time
	startedAt          *time.Time
	completedAt        *time.Time
	lastHeartbeatAt    *time.Time
	cancelledAt        *time.Time
	cancelledBy        *string
	cancellationReason *string

	// totalTaskCount is nil until the run's child set is known (it is stamped on
	// AcceptDispatch). nil mirrors a NULL total_task_count column.
	totalTaskCount    *int32
	terminalTaskCount int32

	serviceMetadata map[string]ServiceMetadata

	changes changeSet
}

// changeSet records which scheduler_tracker columns were mutated since the
// aggregate was loaded. The repository adapter consults it on SaveRun to
// dispatch to the correct tuned SQL method.
type changeSet struct {
	created                bool
	statusDirty            bool
	initStatusDirty        bool
	totalTaskCountDirty    bool
	terminalTaskCountDirty bool
	cancelDirty            bool
	startedDirty           bool
	completedDirty         bool
}

// HydrateRun rebuilds a Run from persisted scheduler_tracker columns. Used by
// the postgres adapter inside LoadRunForUpdate / GetRun. Application code MUST
// NOT call this — use one of the New* constructors instead.
func HydrateRun(
	scheduleID uuid.UUID,
	scheduleName string,
	status SchedulerStatus,
	initStatus InitStatus,
	kind Kind,
	sourceRunID *uuid.UUID,
	initiatedBy string,
	operation model.Operation,
	createdAt time.Time,
	startedAt, completedAt, lastHeartbeatAt, cancelledAt *time.Time,
	cancelledBy, cancellationReason *string,
	total *int32,
	terminal int32,
	serviceMetadata map[string]ServiceMetadata,
) *Run {
	return &Run{
		scheduleID:         scheduleID,
		scheduleName:       scheduleName,
		status:             status,
		initStatus:         initStatus,
		kind:               kind,
		sourceRunID:        sourceRunID,
		initiatedBy:        initiatedBy,
		operation:          operation,
		createdAt:          createdAt,
		startedAt:          startedAt,
		completedAt:        completedAt,
		lastHeartbeatAt:    lastHeartbeatAt,
		cancelledAt:        cancelledAt,
		cancelledBy:        cancelledBy,
		cancellationReason: cancellationReason,
		totalTaskCount:     total,
		terminalTaskCount:  terminal,
		serviceMetadata:    serviceMetadata,
	}
}

// NewPendingRun creates a fresh PENDING Run for activation. Caller flow:
// SchedulePolicy.ScheduleExistsInCatalog → SchedulePolicy.IsScheduleAvailable
// → NewPendingRun → SaveRun → OutboxPublisher.Append.
//
// operation selects the dbt verb (run/test/build) the downstream orchestrator
// applies to the whole DAG; state itself does not act on it — it only
// forwards the value onto the emitted RunStarted event. Callers other than a
// manual TriggerSchedule (cron, etc.) pass model.OperationRun.
func NewPendingRun(
	scheduleName string,
	kind Kind,
	sourceRunID *uuid.UUID,
	initiatedBy string,
	metadata map[string]ServiceMetadata,
	operation model.Operation,
	now time.Time,
) (*Run, DomainEvent, error) {
	if scheduleName == "" {
		return nil, nil, ErrScheduleNameRequired
	}
	if !kind.IsValid() {
		return nil, nil, ErrInvalidKind
	}
	if metadata == nil {
		metadata = map[string]ServiceMetadata{}
	}
	id := uuid.New()
	r := &Run{
		scheduleID:      id,
		scheduleName:    scheduleName,
		status:          SchedulerStatusPending,
		initStatus:      InitStatusInProgress,
		kind:            kind,
		sourceRunID:     sourceRunID,
		initiatedBy:     initiatedBy,
		operation:       operation,
		createdAt:       now,
		serviceMetadata: metadata,
	}
	r.changes.created = true
	evt := RunStarted{ID: id, Name: scheduleName, K: kind, SourceID: sourceRunID, InitiatedBy: initiatedBy, ServiceMetadata: metadata, Operation: operation}
	return r, evt, nil
}

// NewDerivedRun creates a PENDING Run for rerun/rebase. The derived run
// inherits the source run's operation: rerun/rebase re-issue the source
// run's dbt verb (run/build) against the derived task set. A test source
// is rejected upstream in orchestrator before a derived run is requested.
// Stamps last_heartbeat_at so dashboards show the new run immediately.
func NewDerivedRun(
	scheduleName string,
	kind Kind,
	sourceRunID uuid.UUID,
	initiatedBy string,
	operation model.Operation,
	now time.Time,
) (*Run, DomainEvent, error) {
	if scheduleName == "" {
		return nil, nil, ErrScheduleNameRequired
	}
	if kind != KindRerun && kind != KindRebase {
		return nil, nil, ErrInvalidKind
	}
	id := uuid.New()
	r := &Run{
		scheduleID:      id,
		scheduleName:    scheduleName,
		status:          SchedulerStatusPending,
		initStatus:      InitStatusInProgress,
		kind:            kind,
		sourceRunID:     &sourceRunID,
		initiatedBy:     initiatedBy,
		operation:       operation,
		createdAt:       now,
		lastHeartbeatAt: &now,
		serviceMetadata: map[string]ServiceMetadata{},
	}
	r.changes.created = true
	var evt DomainEvent
	if kind == KindRerun {
		evt = RerunRequested{ID: id, Name: scheduleName, SourceID: sourceRunID, InitiatedBy: initiatedBy}
	} else {
		evt = RebaseRequested{ID: id, Name: scheduleName, SourceID: sourceRunID, InitiatedBy: initiatedBy}
	}
	return r, evt, nil
}

// NewSingleNodeRun creates a RUNNING single-task Run. operation selects the
// dbt verb (run/test/build) the downstream orchestrator applies to the
// target node; state itself does not act on it — it only forwards the value
// onto the emitted SingleNodeRunRequested event.
func NewSingleNodeRun(
	scheduleName string,
	target NodeID,
	metadataSource MetadataSource,
	operation model.Operation,
	sourceRunID *uuid.UUID,
	initiatedBy string,
	now time.Time,
) (*Run, DomainEvent, error) {
	if scheduleName == "" {
		return nil, nil, ErrScheduleNameRequired
	}
	id := uuid.New()
	r := &Run{
		scheduleID:      id,
		scheduleName:    scheduleName,
		status:          SchedulerStatusRunning,
		initStatus:      InitStatusInProgress,
		kind:            KindSingleNodeRun,
		sourceRunID:     sourceRunID,
		initiatedBy:     initiatedBy,
		operation:       operation,
		createdAt:       now,
		lastHeartbeatAt: &now,
		serviceMetadata: map[string]ServiceMetadata{},
	}
	r.changes.created = true
	evt := SingleNodeRunRequested{
		ID: id, Name: scheduleName, Target: target,
		MetadataSource: metadataSource, Operation: operation,
		SourceID: sourceRunID, InitiatedBy: initiatedBy,
	}
	return r, evt, nil
}

// NewPromotedSeedsRun creates a RUNNING Run for the seeds a promoted release
// changed, one task per node.
//
// Unlike every other constructor here the run id is supplied rather than
// generated. A promotion is announced by an event that can be redelivered, and
// the caller derives the id deterministically from the release id, so a
// redelivered promotion resolves to the same run instead of minting a second one
// that would rebuild the same seeds.
//
// The run carries no operation: a seed is materialised by its own dbt verb,
// chosen downstream from the node type, not by a verb selected here.
func NewPromotedSeedsRun(
	id uuid.UUID,
	scheduleName string,
	releaseID string,
	nodes []NodeID,
	now time.Time,
) (*Run, DomainEvent, error) {
	if scheduleName == "" {
		return nil, nil, ErrScheduleNameRequired
	}
	if id == uuid.Nil {
		return nil, nil, ErrRunIDRequired
	}
	if releaseID == "" {
		return nil, nil, ErrReleaseIDRequired
	}
	if len(nodes) == 0 {
		return nil, nil, ErrNoNodes
	}
	r := &Run{
		scheduleID:      id,
		scheduleName:    scheduleName,
		status:          SchedulerStatusRunning,
		initStatus:      InitStatusInProgress,
		kind:            KindPromoteSeed,
		initiatedBy:     "system",
		createdAt:       now,
		lastHeartbeatAt: &now,
		serviceMetadata: map[string]ServiceMetadata{},
	}
	r.changes.created = true
	evt := PromotedSeedsRunRequested{
		ID:        id,
		Name:      scheduleName,
		ReleaseID: releaseID,
		Nodes:     nodes,
	}
	return r, evt, nil
}

// ============================================================================
// Getters (read-only — adapters and gRPC mappers use these)
// ============================================================================

func (r *Run) ScheduleID() uuid.UUID            { return r.scheduleID }
func (r *Run) ScheduleName() string             { return r.scheduleName }
func (r *Run) Status() SchedulerStatus          { return r.status }
func (r *Run) Kind() Kind                       { return r.kind }
func (r *Run) SourceRunID() *uuid.UUID          { return r.sourceRunID }
func (r *Run) InitiatedBy() string              { return r.initiatedBy }
func (r *Run) Operation() model.Operation       { return r.operation }
func (r *Run) InitializationStatus() InitStatus { return r.initStatus }
func (r *Run) CreatedAt() time.Time             { return r.createdAt }
func (r *Run) StartedAt() *time.Time            { return r.startedAt }
func (r *Run) CompletedAt() *time.Time          { return r.completedAt }
func (r *Run) LastHeartbeatAt() *time.Time      { return r.lastHeartbeatAt }
func (r *Run) CancelledAt() *time.Time          { return r.cancelledAt }
func (r *Run) CancelledBy() *string             { return r.cancelledBy }
func (r *Run) CancellationReason() *string      { return r.cancellationReason }
func (r *Run) TotalTaskCount() *int32           { return r.totalTaskCount }
func (r *Run) TerminalTaskCount() int32         { return r.terminalTaskCount }
func (r *Run) ServiceMetadata() map[string]ServiceMetadata {
	out := make(map[string]ServiceMetadata, len(r.serviceMetadata))
	for k, v := range r.serviceMetadata {
		out[k] = v
	}
	return out
}

func (r *Run) IsTerminal() bool { return r.status.IsTerminal() }
func (r *Run) IsActive() bool {
	return r.status == SchedulerStatusPending || r.status == SchedulerStatusRunning
}

// ============================================================================
// Change tracking exposed to the repository adapter
// ============================================================================

// Changes returns the changeSet for the repository adapter. The Save method
// reads it to choose tuned SQL. After SaveRun completes, the adapter resets
// the changeSet via ResetChanges.
func (r *Run) Changes() changeSet { return r.changes }
func (r *Run) ResetChanges()      { r.changes = changeSet{} }

// changeSet accessors — exposed so the postgres adapter can read which fields
// were mutated without coupling to the unexported struct fields.
func (c changeSet) IsCreated() bool                { return c.created }
func (c changeSet) IsStatusDirty() bool            { return c.statusDirty }
func (c changeSet) IsInitStatusDirty() bool        { return c.initStatusDirty }
func (c changeSet) IsTotalTaskCountDirty() bool    { return c.totalTaskCountDirty }
func (c changeSet) IsTerminalTaskCountDirty() bool { return c.terminalTaskCountDirty }
func (c changeSet) IsCancelDirty() bool            { return c.cancelDirty }
func (c changeSet) IsStartedDirty() bool           { return c.startedDirty }
func (c changeSet) IsCompletedDirty() bool         { return c.completedDirty }

// AcceptDispatch consumes the run.entries.dispatched:v1 projection. The
// projection IS the full child set (no prior tasks exist), so the aggregate
// makes the auto-rollup decision in one pass over the input.
//
// Behaviour:
//   - No-op (returns no events, no mutations) when r is already terminal.
//     The handler binding still commits the surrounding tx so dedup is recorded.
//   - BulkCreate every projected task.
//   - total_task_count = len(projection); terminal_task_count seeded from
//     already-terminal projected rows.
//   - init_status = completed always after dispatch.
//   - Status transition:
//     every projected task terminal && all succeeded → SUCCEEDED + completed_at
//     every projected task terminal && any non-succeeded → FAILED + completed_at
//     otherwise → RUNNING
//   - Records a RunFinalized event only on the auto-rollup branch.
func (r *Run) AcceptDispatch(
	ctx context.Context,
	tasks TaskCollection,
	projection []DispatchedTask,
	now time.Time,
) ([]DomainEvent, error) {
	if r.IsTerminal() {
		return nil, nil
	}

	built := make([]Task, 0, len(projection))
	for _, p := range projection {
		jobName, err := domain.ComputeJobName(p.ServiceName, p.SchemaName, p.TableName, r.scheduleID.String())
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidDispatchedTask, err)
		}
		built = append(built, Task{
			TaskID:              p.TaskID,
			ScheduleID:          r.scheduleID,
			CreatedAt:           now,
			ServiceName:         p.ServiceName,
			SchemaName:          p.SchemaName,
			TableName:           p.TableName,
			JobName:             jobName,
			Status:              p.Status,
			MaxRetries:          int(p.MaxRetries),
			ManifestVersion:     p.ManifestVersion,
			ImageTag:            p.ImageTag,
			InheritedFromTaskID: p.InheritedFromTaskID,
			Operation:           r.operation,
		})
	}

	if err := tasks.BulkCreate(ctx, built); err != nil {
		return nil, fmt.Errorf("bulk create tasks: %w", err)
	}

	allTerminal, allSucceeded := true, true
	terminal := int32(0)
	for _, p := range projection {
		if !p.Status.IsTerminal() {
			allTerminal, allSucceeded = false, false
			continue
		}
		terminal++
		if p.Status != TaskStatusSucceeded {
			allSucceeded = false
		}
	}

	total := num.ClampInt32(len(projection))
	r.totalTaskCount = &total
	r.terminalTaskCount = terminal
	r.initStatus = InitStatusCompleted
	r.changes.totalTaskCountDirty = true
	r.changes.terminalTaskCountDirty = true
	r.changes.initStatusDirty = true

	if allTerminal {
		outcome := SchedulerStatusFailed
		if allSucceeded {
			outcome = SchedulerStatusSucceeded
		}
		r.status = outcome
		completedAt := now
		r.completedAt = &completedAt
		r.changes.statusDirty = true
		r.changes.completedDirty = true
		evt := RunFinalized{ID: r.scheduleID, Name: r.scheduleName, Outcome: outcome}
		return []DomainEvent{evt}, nil
	}

	r.status = SchedulerStatusRunning
	startedAt := now
	r.startedAt = &startedAt
	r.changes.statusDirty = true
	r.changes.startedDirty = true
	return nil, nil
}

// Cancel marks r as cancelled (a terminal transition), bulk-cancels its child
// tasks, and records RunCancelled (work-suppression guard) plus RunFinalized
// (terminal projection). Returns ErrAlreadyTerminal when r is already terminal.
func (r *Run) Cancel(
	ctx context.Context,
	tasks TaskCollection,
	by, reason string,
	now time.Time,
) ([]DomainEvent, error) {
	if r.IsTerminal() {
		return nil, ErrAlreadyTerminal
	}
	r.status = SchedulerStatusCancelled
	cancelledAt := now
	r.cancelledAt = &cancelledAt
	// Cancellation is terminal, so stamp completed_at just as SUCCEEDED/FAILED do.
	// The cancel path persists through cancelDirty -> CancelTx (which writes
	// cancelled_at and completed_at together), not completedDirty/FinalizeRunTx;
	// this in-memory value keeps CompletedAt() consistent and feeds RunFinalized.
	completedAt := now
	r.completedAt = &completedAt
	if by != "" {
		b := by
		r.cancelledBy = &b
	}
	if reason != "" {
		rs := reason
		r.cancellationReason = &rs
	}
	r.changes.cancelDirty = true
	r.changes.statusDirty = true

	if _, err := tasks.BulkCancel(ctx, r.scheduleID, by, now); err != nil {
		return nil, fmt.Errorf("bulk cancel tasks: %w", err)
	}

	cancelledEvt := RunCancelled{ID: r.scheduleID, Name: r.scheduleName, By: by, CancellationReason: reason}
	finalizedEvt := RunFinalized{ID: r.scheduleID, Name: r.scheduleName, Outcome: SchedulerStatusCancelled}
	return []DomainEvent{cancelledEvt, finalizedEvt}, nil
}

// RecordTaskStatus is the hot-path mutator driven by task.status.updated:v1.
// Returns no events when:
//   - the run is already terminal (stale event)
//   - the task row exists but the status did not change (replay)
//   - the incoming RUNNING is a stale duplicate of an attempt that already
//     reached terminal (see the attempt-monotonic guard below)
//   - the new status is non-terminal (running) — handler ACKs, no finalize
//   - the new status is terminal but terminal_task_count != total_task_count
//     or retryable-failed remains, so finalize is deferred
//
// Returns ErrTaskRowNotProjected when the task row is not yet present; the
// adapter binding treats this as a transient error and the consumer redelivers.
//
// task.status.updated:v1 has two producers — executor-controller emits RUNNING,
// k8s-controller emits the terminal status — so the two messages for one task
// can be processed out of order. retry_count carries the attempt number and is
// stamped identically on a RUNNING and the terminal of the same attempt; a
// genuine retry is a strictly newer attempt. The aggregate uses that to honor a
// real retry's un-fill while ignoring a stale RUNNING re-delivered after its own
// terminal.
func (r *Run) RecordTaskStatus(
	ctx context.Context,
	tasks TaskCollection,
	taskID uuid.UUID,
	newStatus TaskStatus,
	retryCount int32,
	now time.Time,
) ([]DomainEvent, error) {
	if r.IsTerminal() {
		return nil, nil
	}
	prev, prevAttempt, exists, err := tasks.LoadStatusAndAttempt(ctx, taskID)
	if err != nil {
		// LoadStatusAndAttempt locks the row FOR UPDATE; a failure here is a
		// transient I/O error, not "no prior status". Swallowing it would let
		// the newer-attempt branch below adopt this delivery and overwrite a
		// newer terminal already recorded for the task, defeating the
		// attempt-monotonic guard. Propagate so the consumer redelivers once
		// the read recovers.
		return nil, fmt.Errorf("load task status and attempt: %w", err)
	}

	// Guard: if the row is not present, return a transient error so the
	// consumer redelivers after the projection arrives.
	if !exists {
		present, exErr := tasks.Exists(ctx, taskID)
		if exErr != nil {
			return nil, fmt.Errorf("exists check: %w", exErr)
		}
		if !present {
			return nil, ErrTaskRowNotProjected
		}
		// Row reported absent by the status read but present on re-check
		// (a concurrent insert landed between the two reads); proceed with
		// empty prev so the new-attempt branch adopts it.
	}

	prevKnown := prev != ""
	isTerminal := newStatus.IsTerminal()
	prevWasTerminal := prevKnown && prev.IsTerminal()

	// Order the update by attempt (retry_count). The two/three producers of
	// task.status.updated:v1 act independently, so messages for one task can be
	// processed in any order; the attempt number makes the projection
	// order-independent.

	// Older attempt — superseded. Covers both a stale RUNNING and a stale
	// terminal left over from an attempt newer state has already moved past.
	if prevKnown && retryCount < prevAttempt {
		return nil, nil
	}

	// Same attempt as the stored row.
	if prevKnown && retryCount == prevAttempt {
		if prevWasTerminal {
			// Terminal already recorded for this attempt: a replayed terminal,
			// or a RUNNING re-delivered after its own terminal. No-op — do not
			// regress status or un-fill the slot.
			return nil, nil
		}
		if !isTerminal {
			// Non-terminal progress within the attempt (e.g. PENDING→RUNNING).
			// Persist the status change; the slot is unaffected.
			if newStatus == prev {
				return nil, nil
			}
			if _, err := tasks.SetStatusAndAttempt(ctx, taskID, newStatus, retryCount); err != nil {
				return nil, fmt.Errorf("set task status: %w", err)
			}
			return nil, nil
		}
		// First terminal for this attempt — fill the slot.
		affected, err := tasks.SetStatusAndAttempt(ctx, taskID, newStatus, retryCount)
		if err != nil {
			return nil, fmt.Errorf("set task status: %w", err)
		}
		if affected == 0 {
			return nil, nil // cancelled or vanished — leave counters untouched.
		}
		r.fillSlot()
		return r.finalizeIfComplete(ctx, tasks, now)
	}

	// Newer attempt (retryCount > prevAttempt) or no prior status — adopt it.
	affected, err := tasks.SetStatusAndAttempt(ctx, taskID, newStatus, retryCount)
	if err != nil {
		return nil, fmt.Errorf("set task status: %w", err)
	}
	if affected == 0 {
		return nil, nil // cancelled or vanished — leave counters untouched.
	}
	switch {
	case prevWasTerminal && !isTerminal:
		// Genuine retry running again — un-fill the slot.
		r.unfillSlot()
		return nil, nil
	case !prevWasTerminal && isTerminal:
		// Terminal of a running/new attempt — fill the slot.
		r.fillSlot()
		return r.finalizeIfComplete(ctx, tasks, now)
	case prevWasTerminal && isTerminal:
		// A newer attempt's terminal arriving after an older attempt's terminal
		// (e.g. the retry also failed, or it succeeded). The slot stays filled —
		// no count change — but the task's retryability may have flipped, so
		// re-check finalization with the now-current attempt persisted.
		return r.finalizeIfComplete(ctx, tasks, now)
	default:
		// Non-terminal after non-terminal (newer attempt already running) — the
		// slot was empty and stays empty.
		return nil, nil
	}
}

// fillSlot marks one more task as terminal in the run's bookkeeping.
func (r *Run) fillSlot() {
	r.terminalTaskCount++
	r.changes.terminalTaskCountDirty = true
}

// unfillSlot reverses fillSlot when a terminal task starts a new attempt,
// clamping at zero defensively.
func (r *Run) unfillSlot() {
	r.terminalTaskCount--
	if r.terminalTaskCount < 0 {
		r.terminalTaskCount = 0
	}
	r.changes.terminalTaskCountDirty = true
}

// finalizeIfComplete finalizes the scheduler when every task is terminal, the
// graph has finished loading, the run is still RUNNING, and no failed task is
// still retryable. Returns the RunFinalized event(s), or nil when finalization
// is deferred.
func (r *Run) finalizeIfComplete(ctx context.Context, tasks TaskCollection, now time.Time) ([]DomainEvent, error) {
	if r.totalTaskCount == nil || r.terminalTaskCount != *r.totalTaskCount {
		return nil, nil
	}
	if r.initStatus != InitStatusCompleted {
		return nil, nil
	}
	if r.status != SchedulerStatusRunning {
		return nil, nil
	}
	hasRetryable, err := tasks.HasRetryableFailed(ctx, r.scheduleID)
	if err != nil {
		return nil, fmt.Errorf("has retryable failed: %w", err)
	}
	if hasRetryable {
		return nil, nil
	}
	anyFailed, err := tasks.HasFailed(ctx, r.scheduleID)
	if err != nil {
		return nil, fmt.Errorf("has failed: %w", err)
	}

	outcome := SchedulerStatusSucceeded
	if anyFailed {
		outcome = SchedulerStatusFailed
	}
	return r.finalize(outcome, now), nil
}

// MarkDispatchTerminal is called when orchestrator emits
// run.entries.dispatch_failed:v1. A benign outcome (no work to do — a Test run
// with no tests) finalizes the run as `skipped`; any other reason finalizes it
// as `failed`. reason is recorded on the RunDispatchTerminal side event for
// observability regardless of outcome. No-op when r is already terminal.
func (r *Run) MarkDispatchTerminal(benign bool, reason string, now time.Time) ([]DomainEvent, error) {
	if r.IsTerminal() {
		return nil, nil
	}
	outcome := SchedulerStatusFailed
	if benign {
		outcome = SchedulerStatusSkipped
	}
	evts := r.finalize(outcome, now)
	side := RunDispatchTerminal{ID: r.scheduleID, Name: r.scheduleName, Reason: reason}
	return append(evts, side), nil
}

func (r *Run) finalize(outcome SchedulerStatus, now time.Time) []DomainEvent {
	r.status = outcome
	completedAt := now
	r.completedAt = &completedAt
	r.changes.statusDirty = true
	r.changes.completedDirty = true
	evt := RunFinalized{ID: r.scheduleID, Name: r.scheduleName, Outcome: outcome}
	return []DomainEvent{evt}
}

// CanBeRerunSource enforces the eligibility check for a run to serve as a
// rerun source: status must be FAILED or CANCELLED, and at least one task
// must be in a non-SUCCEEDED state. The aggregate orchestrates the predicate
// call itself; application services do not pre-query.
func (r *Run) CanBeRerunSource(ctx context.Context, tasks TaskCollection) error {
	return r.canBeDerivedSource(ctx, tasks, ErrNothingToRerun)
}

// CanBeRebaseSource shares semantics with CanBeRerunSource — same status set,
// same "at least one non-SUCCEEDED" requirement. Returns ErrNothingToRebase
// to give the gRPC adapter a stable error variant for the rebase code path.
func (r *Run) CanBeRebaseSource(ctx context.Context, tasks TaskCollection) error {
	return r.canBeDerivedSource(ctx, tasks, ErrNothingToRebase)
}

// canBeDerivedSource is the shared eligibility predicate behind
// CanBeRerunSource and CanBeRebaseSource: the source must be terminally
// FAILED or CANCELLED and carry at least one non-SUCCEEDED task. nothingErr
// is the kind-specific sentinel (ErrNothingToRerun / ErrNothingToRebase)
// returned when no non-SUCCEEDED task exists, so callers can tell the two
// trigger paths apart.
func (r *Run) canBeDerivedSource(ctx context.Context, tasks TaskCollection, nothingErr error) error {
	if r.status != SchedulerStatusFailed && r.status != SchedulerStatusCancelled {
		return ErrSourceMustBeTerminallyFailedOrCancelled
	}
	has, err := tasks.HasNonSucceeded(ctx, r.scheduleID)
	if err != nil {
		return fmt.Errorf("has non-succeeded: %w", err)
	}
	if !has {
		return nothingErr
	}
	return nil
}

// HasTaskAt is used by TriggerSingleNodeRun's stale-mode precondition: the
// source run must carry a task at the requested node.
func (r *Run) HasTaskAt(ctx context.Context, tasks TaskCollection, node NodeID) (bool, error) {
	_, err := tasks.GetByNode(ctx, r.scheduleID, node)
	if err == ErrTaskNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get by node: %w", err)
	}
	return true, nil
}
