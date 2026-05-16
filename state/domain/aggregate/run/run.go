package run

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
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
	status             SchedulerStatus
	initStatus         InitStatus
	kind               Kind
	sourceRunID        *uuid.UUID
	createdAt          time.Time
	startedAt          *time.Time
	completedAt        *time.Time
	lastHeartbeatAt    *time.Time
	cancelledAt        *time.Time
	cancelledBy        *string
	cancellationReason *string

	totalTaskCount    sql.NullInt32
	terminalTaskCount int32

	serviceMetadata map[string]ServiceMetadata

	changes changeSet
	events  []DomainEvent
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
	createdAt time.Time,
	startedAt, completedAt, lastHeartbeatAt, cancelledAt *time.Time,
	cancelledBy, cancellationReason *string,
	total sql.NullInt32,
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
func NewPendingRun(
	scheduleName string,
	kind Kind,
	sourceRunID *uuid.UUID,
	metadata map[string]ServiceMetadata,
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
		createdAt:       now,
		serviceMetadata: metadata,
	}
	r.changes.created = true
	evt := RunStarted{ID: id, Name: scheduleName, K: kind, SourceID: sourceRunID, ServiceMetadata: metadata}
	r.events = append(r.events, evt)
	return r, evt, nil
}

// NewDerivedRun creates a PENDING Run for rerun/rebase. Stamps last_heartbeat_at
// so dashboards show the new run immediately.
func NewDerivedRun(
	scheduleName string,
	kind Kind,
	sourceRunID uuid.UUID,
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
		createdAt:       now,
		lastHeartbeatAt: &now,
		serviceMetadata: map[string]ServiceMetadata{},
	}
	r.changes.created = true
	var evt DomainEvent
	if kind == KindRerun {
		evt = RerunRequested{ID: id, Name: scheduleName, SourceID: sourceRunID}
	} else {
		evt = RebaseRequested{ID: id, Name: scheduleName, SourceID: sourceRunID}
	}
	r.events = append(r.events, evt)
	return r, evt, nil
}

// NewSingleNodeRun creates a RUNNING single-task Run.
func NewSingleNodeRun(
	scheduleName string,
	target NodeID,
	metadataSource MetadataSource,
	sourceRunID *uuid.UUID,
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
		createdAt:       now,
		lastHeartbeatAt: &now,
		serviceMetadata: map[string]ServiceMetadata{},
	}
	r.changes.created = true
	evt := SingleNodeRunRequested{
		ID: id, Name: scheduleName, Target: target,
		MetadataSource: metadataSource, SourceID: sourceRunID,
	}
	r.events = append(r.events, evt)
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
func (r *Run) InitializationStatus() InitStatus { return r.initStatus }
func (r *Run) CreatedAt() time.Time             { return r.createdAt }
func (r *Run) StartedAt() *time.Time            { return r.startedAt }
func (r *Run) CompletedAt() *time.Time          { return r.completedAt }
func (r *Run) LastHeartbeatAt() *time.Time      { return r.lastHeartbeatAt }
func (r *Run) CancelledAt() *time.Time          { return r.cancelledAt }
func (r *Run) CancelledBy() *string             { return r.cancelledBy }
func (r *Run) CancellationReason() *string      { return r.cancellationReason }
func (r *Run) TotalTaskCount() sql.NullInt32    { return r.totalTaskCount }
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

// PullEvents returns the events recorded during this load-mutate cycle and
// clears the internal buffer. The application service calls this after Save
// and before OutboxPublisher.Append. Safe to call multiple times; subsequent
// calls return empty until a new mutation records something.
func (r *Run) PullEvents() []DomainEvent {
	evts := r.events
	r.events = nil
	return evts
}

// Changes returns the changeSet for the repository adapter. The Save method
// reads it to choose tuned SQL. After SaveRun completes, the adapter resets
// the changeSet via ResetChanges.
func (r *Run) Changes() changeSet { return r.changes }
func (r *Run) ResetChanges()      { r.changes = changeSet{} }

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
			return nil, fmt.Errorf("compute job_name: %w", err)
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

	r.totalTaskCount = sql.NullInt32{Int32: int32(len(projection)), Valid: true}
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
		r.events = append(r.events, evt)
		return []DomainEvent{evt}, nil
	}

	r.status = SchedulerStatusRunning
	startedAt := now
	r.startedAt = &startedAt
	r.changes.statusDirty = true
	r.changes.startedDirty = true
	return nil, nil
}

// Cancel marks r as cancelled, bulk-cancels its child tasks, and records a
// RunCancelled event. Returns ErrAlreadyTerminal when r is already terminal.
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

	if _, err := tasks.BulkCancel(ctx, r.scheduleID, by); err != nil {
		return nil, fmt.Errorf("bulk cancel tasks: %w", err)
	}

	evt := RunCancelled{ID: r.scheduleID, Name: r.scheduleName, By: by, CancellationReason: reason}
	r.events = append(r.events, evt)
	return []DomainEvent{evt}, nil
}
