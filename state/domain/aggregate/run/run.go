package run

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
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
	// release is the release this run runs on; the zero stamp when the run
	// names none.
	release ReleaseStamp
	// initiatedBy records the user who triggered this run, or the "system"
	// sentinel for cron / platform-initiated runs. Stamped at creation and
	// immutable thereafter.
	initiatedBy string
	// operation is the dbt verb this run applies to its nodes (run/test/build/full_refresh).
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
	// plannedAt is when Start chose and stored the run's tasks; nil on a run
	// whose tasks state did not choose.
	plannedAt *time.Time

	// totalTaskCount is nil until the run's child set is known (it is stamped by
	// Start). nil mirrors a NULL total_task_count column.
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
	heartbeatDirty         bool
	plannedDirty           bool
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

// HydrateRelease restores the release stamp read from scheduler_tracker.
// Adapter-only: domain code receives the stamp through a constructor.
func (r *Run) HydrateRelease(s ReleaseStamp) { r.release = s }

// HydratePlannedAt restores planned_at read from scheduler_tracker.
// Adapter-only: domain code plans a run through Start.
func (r *Run) HydratePlannedAt(at *time.Time) { r.plannedAt = at }

// NewPendingRun creates a fresh PENDING Run of a schedule, fired by cron or
// triggered. Caller flow: SchedulePolicy.ScheduleExistsInCatalog →
// SchedulePolicy.IsScheduleAvailable → NewPendingRun → SaveRun → Start → SaveRun.
//
// operation is the dbt verb (run/test/build) the run applies to every node it
// executes. Callers other than a manual TriggerSchedule (cron, etc.) pass
// model.OperationRun.
//
// stamp is the release the run runs on: the live release when the run is created.
func NewPendingRun(
	scheduleName string,
	kind Kind,
	sourceRunID *uuid.UUID,
	initiatedBy string,
	metadata map[string]ServiceMetadata,
	operation model.Operation,
	stamp ReleaseStamp,
	now time.Time,
) (*Run, error) {
	if scheduleName == "" {
		return nil, ErrScheduleNameRequired
	}
	if !kind.IsValid() {
		return nil, ErrInvalidKind
	}
	if metadata == nil {
		metadata = map[string]ServiceMetadata{}
	}
	r := &Run{
		scheduleID:      uuid.New(),
		scheduleName:    scheduleName,
		status:          SchedulerStatusPending,
		initStatus:      InitStatusInProgress,
		kind:            kind,
		sourceRunID:     sourceRunID,
		initiatedBy:     initiatedBy,
		operation:       operation,
		release:         stamp,
		createdAt:       now,
		serviceMetadata: metadata,
	}
	r.changes.created = true
	return r, nil
}

// NewDerivedRun creates a PENDING Run for rerun/rebase. The derived run
// inherits the source run's operation: rerun/rebase re-issue the source
// run's dbt verb (run/build) against the derived task set; the planner refuses
// a test source. Stamps last_heartbeat_at so dashboards show the new run
// immediately.
//
// stamp is the release the run runs on: the source run's for a rerun, the
// live release for a rebase.
func NewDerivedRun(
	scheduleName string,
	kind Kind,
	sourceRunID uuid.UUID,
	stamp ReleaseStamp,
	initiatedBy string,
	operation model.Operation,
	now time.Time,
) (*Run, error) {
	if scheduleName == "" {
		return nil, ErrScheduleNameRequired
	}
	if kind != KindRerun && kind != KindRebase {
		return nil, ErrInvalidKind
	}
	r := &Run{
		scheduleID:      uuid.New(),
		scheduleName:    scheduleName,
		status:          SchedulerStatusPending,
		initStatus:      InitStatusInProgress,
		kind:            kind,
		sourceRunID:     &sourceRunID,
		initiatedBy:     initiatedBy,
		operation:       operation,
		release:         stamp,
		createdAt:       now,
		lastHeartbeatAt: &now,
		serviceMetadata: map[string]ServiceMetadata{},
	}
	r.changes.created = true
	return r, nil
}

// NewSingleNodeRun creates a RUNNING single-task Run. operation is the dbt
// verb (run/test/build/full_refresh) the run applies to its node.
//
// stamp is the release the run runs on; the zero stamp names none.
func NewSingleNodeRun(
	scheduleName string,
	operation model.Operation,
	sourceRunID *uuid.UUID,
	initiatedBy string,
	stamp ReleaseStamp,
	now time.Time,
) (*Run, error) {
	if scheduleName == "" {
		return nil, ErrScheduleNameRequired
	}
	r := &Run{
		scheduleID:      uuid.New(),
		scheduleName:    scheduleName,
		status:          SchedulerStatusRunning,
		initStatus:      InitStatusInProgress,
		kind:            KindSingleNodeRun,
		sourceRunID:     sourceRunID,
		initiatedBy:     initiatedBy,
		operation:       operation,
		release:         stamp,
		createdAt:       now,
		lastHeartbeatAt: &now,
		serviceMetadata: map[string]ServiceMetadata{},
	}
	r.changes.created = true
	return r, nil
}

// NewPromotedSeedsRun creates a RUNNING Run that loads seeds of a release into
// the production schema, one task per seed.
//
// Unlike every other constructor here the run id is supplied rather than
// generated: the caller derives it deterministically from the release, so
// planning the same release again resolves to a run that already exists
// instead of minting a second one that would load the same seeds.
//
// The run records stamp, the live release whose seeds it loads.
//
// The run carries no operation: a seed is materialised by its own dbt verb,
// chosen downstream from the node type, not by a verb selected here.
func NewPromotedSeedsRun(
	id uuid.UUID,
	scheduleName string,
	stamp ReleaseStamp,
	now time.Time,
) (*Run, error) {
	if scheduleName == "" {
		return nil, ErrScheduleNameRequired
	}
	if id == uuid.Nil {
		return nil, ErrRunIDRequired
	}
	if stamp.ReleaseID == "" {
		return nil, ErrReleaseIDRequired
	}
	r := &Run{
		scheduleID:      id,
		scheduleName:    scheduleName,
		status:          SchedulerStatusRunning,
		initStatus:      InitStatusInProgress,
		kind:            KindPromoteSeed,
		release:         stamp,
		initiatedBy:     "system",
		createdAt:       now,
		lastHeartbeatAt: &now,
		serviceMetadata: map[string]ServiceMetadata{},
	}
	r.changes.created = true
	return r, nil
}

// ============================================================================
// Getters (read-only — adapters and gRPC mappers use these)
// ============================================================================

func (r *Run) ScheduleID() uuid.UUID            { return r.scheduleID }
func (r *Run) ScheduleName() string             { return r.scheduleName }
func (r *Run) Status() SchedulerStatus          { return r.status }
func (r *Run) Kind() Kind                       { return r.kind }
func (r *Run) SourceRunID() *uuid.UUID          { return r.sourceRunID }
func (r *Run) Release() ReleaseStamp            { return r.release }
func (r *Run) ReleaseID() string                { return r.release.ReleaseID }
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
func (r *Run) PlannedAt() *time.Time            { return r.plannedAt }
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
func (c changeSet) IsHeartbeatDirty() bool         { return c.heartbeatDirty }
func (c changeSet) IsPlannedDirty() bool           { return c.plannedDirty }

// Start stores the tasks plan chose for r and dispatches the plan's ready
// tasks. g is the slice of r's release the plan was chosen from; every task
// reads its node there.
//
// Each planned task gets the id TaskIDFor derives for tenantID, r and its node,
// its node's Job name, r's operation and the node's image. A pending task gets
// status pending and maxRetries; an inherited task gets status succeeded, no
// retry budget and the task it inherits from, and is never dispatched. Each
// ready task is stamped dispatched at now and recorded as a TaskDispatched, in
// key order. The run records the plan: planned_at, the total task count, the
// inherited tasks as terminal, init status completed, status running,
// started_at and its progress clock. A plan without a pending task leaves
// nothing to run, so the run finalizes succeeded at once.
//
// Start refuses, before it stores anything: a terminal run (ErrAlreadyTerminal),
// a run whose tasks are already stored (ErrAlreadyPlanned), a plan without
// tasks (ErrNoNodes), a plan naming a node g does not hold
// (ErrPlanNodeNotInGraph), and a plan whose ready list names a task that is
// not one of its pending tasks (ErrPlanReadyNotPending).
func (r *Run) Start(
	ctx context.Context,
	tasks TaskCollection,
	tenantID string,
	plan runplan.Plan,
	g topology.Graph,
	maxRetries int32,
	now time.Time,
) ([]DomainEvent, error) {
	if r.IsTerminal() {
		return nil, ErrAlreadyTerminal
	}
	if r.plannedAt != nil || r.initStatus == InitStatusCompleted {
		return nil, ErrAlreadyPlanned
	}
	if len(plan.Tasks) == 0 {
		return nil, ErrNoNodes
	}
	pending := make(map[topology.NodeKey]bool, len(plan.Tasks))
	for _, p := range plan.Tasks {
		if !p.Inherited {
			pending[p.Key] = true
		}
	}
	ready := make(map[topology.NodeKey]bool, len(plan.Ready))
	for _, k := range plan.Ready {
		if !pending[k] {
			return nil, fmt.Errorf("%w: %s", ErrPlanReadyNotPending, k)
		}
		ready[k] = true
	}

	built := make([]Task, 0, len(plan.Tasks))
	var events []DomainEvent
	inherited := int32(0)
	for _, p := range plan.Tasks {
		n, ok := g.Node(p.Key)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrPlanNodeNotInGraph, p.Key)
		}
		jobName, err := domain.ComputeJobName(n.ServiceName, n.SchemaName, n.TableName, r.scheduleID.String(), 0)
		if err != nil {
			return nil, fmt.Errorf("name the job of %s: %w", p.Key, err)
		}
		t := Task{
			TaskID:      TaskIDFor(tenantID, r.scheduleID, p.Key),
			ScheduleID:  r.scheduleID,
			CreatedAt:   now,
			ServiceName: n.ServiceName,
			SchemaName:  n.SchemaName,
			TableName:   n.TableName,
			NodeKey:     p.Key,
			JobName:     jobName,
			Status:      TaskStatusPending,
			MaxRetries:  int(maxRetries),
			ImageTag:    n.ImageTag,
			Operation:   r.operation,
		}
		switch {
		case p.Inherited:
			root := p.InheritedFrom
			t.Status = TaskStatusSucceeded
			t.MaxRetries = 0
			t.InheritedFromTaskID = &root
			inherited++
		case ready[p.Key]:
			at := now
			t.DispatchedAt = &at
			events = append(events, TaskDispatched{
				ID:           r.scheduleID,
				ScheduleName: r.scheduleName,
				TaskID:       t.TaskID,
				Node:         n,
				JobName:      jobName,
				Operation:    r.operation,
				MaxRetries:   maxRetries,
			})
		}
		built = append(built, t)
	}

	if err := tasks.BulkCreate(ctx, built); err != nil {
		return nil, fmt.Errorf("bulk create tasks: %w", err)
	}

	total := num.ClampInt32(len(built))
	r.totalTaskCount = &total
	r.terminalTaskCount = inherited
	r.initStatus = InitStatusCompleted
	planned := now
	r.plannedAt = &planned
	r.status = SchedulerStatusRunning
	started := now
	r.startedAt = &started
	r.changes.totalTaskCountDirty = true
	r.changes.terminalTaskCountDirty = true
	r.changes.initStatusDirty = true
	r.changes.plannedDirty = true
	r.changes.statusDirty = true
	r.changes.startedDirty = true
	r.recordProgress(now)

	if !plan.HasPending() {
		return append(events, r.finalize(SchedulerStatusSucceeded, now)...), nil
	}
	return events, nil
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
//
// On a run whose tasks state planned (planned_at set), a terminal status that
// takes effect also moves the rest of the run, reading how its tasks connect
// through graph:
//   - a success dispatches every pending, undispatched task it makes ready
//     (each of its in-run upstreams succeeded), stamping dispatched_at and
//     recording a TaskDispatched per task, in key order;
//   - a failure on the task's last allowed attempt (retryCount >= the task's
//     max_retries) skips every pending, undispatched task below it in the run,
//     which can no longer run, and counts them terminal.
//
// A run whose tasks state did not plan reads nothing through graph: its tasks
// do not record when they were dispatched, so neither readiness nor a cascade
// can be read from them.
//
// Returns no events when:
//   - the run is already terminal (stale event)
//   - the task row exists but the status did not change (replay)
//   - the incoming RUNNING is a stale duplicate of an attempt that already
//     reached terminal (see the attempt-monotonic guard below)
//   - the new status is non-terminal (running) — handler ACKs, no finalize
//   - the new status is terminal but terminal_task_count != total_task_count
//     or retryable-failed remains, so finalize is deferred
//
// The boolean result reports whether this call persisted a terminal SUCCEEDED
// for the task: the status write took effect and newStatus is SUCCEEDED. It is
// false for every ignored delivery (a run already terminal, a superseded
// attempt, a replay of a finished attempt, a write the task row refused) and for
// any other status, so a caller that acts on a task's success acts only on the
// delivery that made it. It does not depend on finalization: an accepted
// success that leaves other tasks open returns no events and true.
//
// Returns ErrTaskNotHeld when the run holds no task with taskID.
//
// task.status.updated:v1 carries up to two messages per attempt:
// execution-controller's observer announces RUNNING once, when the attempt's
// main container starts, then the terminal status once the attempt ends (only
// the terminal status when it ends before it was seen running). Both are
// written for the same task aggregate, so the per-aggregate FIFO outbox
// publishes them in creation order, but they still arrive here as separate
// consumer deliveries and a redelivery can repeat one of them. retry_count
// carries the attempt number and is stamped identically on a RUNNING and the
// terminal of the same attempt; a genuine retry is a strictly newer attempt.
// The aggregate uses that to honor a real retry's un-fill while ignoring a
// stale RUNNING re-delivered after its own terminal.
//
// Every task write that takes effect stamps the run's progress clock
// (last_heartbeat_at) with now. A replay, a superseded attempt, or a write the
// task row refused (affected == 0) leaves the clock where it was.
func (r *Run) RecordTaskStatus(
	ctx context.Context,
	tasks TaskCollection,
	graph TaskGraph,
	taskID uuid.UUID,
	newStatus TaskStatus,
	retryCount int32,
	now time.Time,
) ([]DomainEvent, bool, error) {
	if r.IsTerminal() {
		return nil, false, nil
	}
	attempt, exists, err := tasks.LoadAttemptState(ctx, taskID)
	if err != nil {
		// LoadAttemptState locks the row FOR UPDATE; a failure here is a
		// transient I/O error, not "no prior status". Swallowing it would let
		// the newer-attempt branch below adopt this delivery and overwrite a
		// newer terminal already recorded for the task, defeating the
		// attempt-monotonic guard. Propagate so the consumer redelivers once
		// the read recovers.
		return nil, false, fmt.Errorf("load task attempt: %w", err)
	}
	prev, prevAttempt := attempt.Status, attempt.RetryCount

	if !exists {
		return nil, false, ErrTaskNotHeld
	}

	prevKnown := prev != ""
	isTerminal := newStatus.IsTerminal()
	prevWasTerminal := prevKnown && prev.IsTerminal()

	// Order the update by attempt (retry_count). Deliveries of
	// task.status.updated:v1 for one task can be processed in any order (a
	// redelivery can repeat an earlier one); the attempt number makes the
	// result order-independent.

	// Older attempt — superseded. Covers both a stale RUNNING and a stale
	// terminal left over from an attempt newer state has already moved past.
	if prevKnown && retryCount < prevAttempt {
		return nil, false, nil
	}

	// Same attempt as the stored row.
	if prevKnown && retryCount == prevAttempt {
		if prevWasTerminal {
			// Terminal already recorded for this attempt: a replayed terminal,
			// or a RUNNING re-delivered after its own terminal. No-op — do not
			// regress status or un-fill the slot.
			return nil, false, nil
		}
		if !isTerminal {
			// Non-terminal progress within the attempt (e.g. PENDING→RUNNING).
			// Persist the status change; the slot is unaffected.
			if newStatus == prev {
				return nil, false, nil
			}
			affected, err := tasks.SetStatusAndAttempt(ctx, taskID, newStatus, retryCount)
			if err != nil {
				return nil, false, fmt.Errorf("set task status: %w", err)
			}
			if affected > 0 {
				r.recordProgress(now)
			}
			return nil, false, nil
		}
		// First terminal for this attempt — fill the slot.
		affected, err := tasks.SetStatusAndAttempt(ctx, taskID, newStatus, retryCount)
		if err != nil {
			return nil, false, fmt.Errorf("set task status: %w", err)
		}
		if affected == 0 {
			return nil, false, nil // cancelled or vanished — leave counters untouched.
		}
		r.recordProgress(now)
		r.fillSlot()
		return r.applyTerminal(ctx, tasks, graph, attempt, newStatus, retryCount, now)
	}

	// Newer attempt (retryCount > prevAttempt) or no prior status — adopt it.
	affected, err := tasks.SetStatusAndAttempt(ctx, taskID, newStatus, retryCount)
	if err != nil {
		return nil, false, fmt.Errorf("set task status: %w", err)
	}
	if affected == 0 {
		return nil, false, nil // cancelled or vanished — leave counters untouched.
	}
	r.recordProgress(now)
	switch {
	case prevWasTerminal && !isTerminal:
		// Genuine retry running again — un-fill the slot.
		r.unfillSlot()
		return nil, false, nil
	case !prevWasTerminal && isTerminal:
		// Terminal of a running/new attempt — fill the slot.
		r.fillSlot()
		return r.applyTerminal(ctx, tasks, graph, attempt, newStatus, retryCount, now)
	case prevWasTerminal && isTerminal:
		// A newer attempt's terminal arriving after an older attempt's terminal
		// (e.g. the retry also failed, or it succeeded). The slot stays filled —
		// no count change — but the task's retryability may have flipped, so
		// re-check finalization with the now-current attempt persisted.
		return r.applyTerminal(ctx, tasks, graph, attempt, newStatus, retryCount, now)
	default:
		// Non-terminal after non-terminal (newer attempt already running) — the
		// slot was empty and stays empty.
		return nil, false, nil
	}
}

// recordProgress stamps the run's progress clock with now. The clock reads the
// last dispatch or applied task status change; the dispatch watchdog measures a
// stall from it.
func (r *Run) recordProgress(now time.Time) {
	at := now
	r.lastHeartbeatAt = &at
	r.changes.heartbeatDirty = true
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

// applyTerminal follows a terminal status that took effect for the task whose
// stored state was attempt: on a run whose tasks state planned, a success
// dispatches what it makes ready and a failure on the last allowed attempt
// skips what it blocks; then the run finalizes when its tasks are complete.
// The success flag it returns is applied being SUCCEEDED, independent of
// whether the run finalized.
func (r *Run) applyTerminal(
	ctx context.Context,
	tasks TaskCollection,
	graph TaskGraph,
	attempt TaskAttemptState,
	applied TaskStatus,
	retryCount int32,
	now time.Time,
) ([]DomainEvent, bool, error) {
	var events []DomainEvent
	if r.plannedAt != nil {
		switch {
		case applied == TaskStatusSucceeded:
			dispatched, err := r.dispatchReady(ctx, tasks, graph, attempt.NodeKey, now)
			if err != nil {
				return nil, false, err
			}
			events = dispatched
		case applied == TaskStatusFailed && retryCount >= attempt.MaxRetries:
			if err := r.skipBlocked(ctx, tasks, graph, attempt.NodeKey); err != nil {
				return nil, false, err
			}
		}
	}
	finalized, err := r.finalizeIfComplete(ctx, tasks, now)
	if err != nil {
		return nil, false, err
	}
	return append(events, finalized...), applied == TaskStatusSucceeded, nil
}

// dispatchReady dispatches the tasks of r that the success of the task at key
// makes ready: its pending, undispatched downstream tasks whose every in-run
// upstream has succeeded. Each one MarkDispatched stamps records a
// TaskDispatched, in key order.
func (r *Run) dispatchReady(ctx context.Context, tasks TaskCollection, graph TaskGraph, key topology.NodeKey, now time.Time) ([]DomainEvent, error) {
	candidates, err := graph.ReadyCandidates(ctx, r.scheduleID, r.release.ReleaseID, key)
	if err != nil {
		return nil, fmt.Errorf("read the tasks %s's success may make ready: %w", key, err)
	}
	var ready []ReadyCandidate
	ids := make([]uuid.UUID, 0, len(candidates))
	for _, c := range candidates {
		if c.Ready() {
			ready = append(ready, c)
			ids = append(ids, c.TaskID)
		}
	}
	if len(ready) == 0 {
		return nil, nil
	}
	stamped, err := tasks.MarkDispatched(ctx, ids, now)
	if err != nil {
		return nil, fmt.Errorf("mark the tasks %s's success made ready dispatched: %w", key, err)
	}
	isStamped := make(map[uuid.UUID]bool, len(stamped))
	for _, id := range stamped {
		isStamped[id] = true
	}
	events := make([]DomainEvent, 0, len(stamped))
	for _, c := range ready {
		if !isStamped[c.TaskID] {
			continue
		}
		events = append(events, TaskDispatched{
			ID:           r.scheduleID,
			ScheduleName: r.scheduleName,
			TaskID:       c.TaskID,
			Node:         c.Node,
			JobName:      c.JobName,
			Operation:    r.operation,
			MaxRetries:   c.MaxRetries,
		})
	}
	return events, nil
}

// skipBlocked skips the pending, undispatched tasks of r below the task at
// key, which has failed for good, and counts each one it skips as terminal.
func (r *Run) skipBlocked(ctx context.Context, tasks TaskCollection, graph TaskGraph, key topology.NodeKey) error {
	blocked, err := graph.PendingDescendants(ctx, r.scheduleID, r.release.ReleaseID, key)
	if err != nil {
		return fmt.Errorf("read the tasks %s's failure blocks: %w", key, err)
	}
	skipped, err := tasks.MarkSkipped(ctx, blocked)
	if err != nil {
		return fmt.Errorf("skip the tasks %s's failure blocks: %w", key, err)
	}
	if skipped > 0 {
		r.terminalTaskCount += num.ClampInt32(skipped)
		r.changes.terminalTaskCountDirty = true
	}
	return nil
}

// finalizeIfComplete finalizes the scheduler when every task is terminal, the
// graph has finished loading, the run is still RUNNING, and no failed task is
// still retryable. The outcome is failed when any task is failed or cancelled,
// succeeded otherwise (every task succeeded or skipped). Returns the
// RunFinalized event(s), or nil when finalization is deferred.
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

// MarkDispatchTerminal finalizes a run that was created without tasks: a cron
// run whose selection was refused. A benign outcome (no work to do — a test
// run over nodes with no data tests) finalizes the run as `skipped`; any other
// reason finalizes it as `failed`. reason is recorded on the RunDispatchTerminal
// side event regardless of outcome. No-op when r is already terminal.
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
