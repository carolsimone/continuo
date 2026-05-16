package run

import (
	"database/sql"
	"time"

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
