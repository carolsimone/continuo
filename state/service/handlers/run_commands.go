package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/commandrequest"
	"github.com/carolsimone/continuo/state/service/commands"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// The command names a command record and its metric carry.
const (
	commandStartRun             = "start_run"
	commandTriggerRerun         = "trigger_rerun"
	commandTriggerRebase        = "trigger_rebase"
	commandTriggerSingleNodeRun = "trigger_single_node_run"
	commandCancelActiveRun      = "cancel_active_run"
)

// ErrNoActiveRun reports a cancel by schedule name of a schedule with no
// active run.
var ErrNoActiveRun = errors.New("the schedule has no active run")

// CommandResult is the run a command created or cancelled. ScheduleName is
// the run's schedule; Replayed is true when the command repeated an earlier
// call of its request id and did nothing.
type CommandResult struct {
	RunID        uuid.UUID
	ScheduleName string
	Replayed     bool
}

// StartRunCommand starts a run of TenantID's schedule ScheduleName applying
// Operation, for Initiator, once per RequestID.
type StartRunCommand struct {
	TenantID     string
	RequestID    uuid.UUID
	ScheduleName string
	Operation    model.Operation
	Initiator    identity.Identity
}

// TriggerRerunCommand reruns TenantID's run SourceRunID, for Initiator, once
// per RequestID.
type TriggerRerunCommand struct {
	TenantID    string
	RequestID   uuid.UUID
	SourceRunID uuid.UUID
	Initiator   identity.Identity
}

// TriggerRebaseCommand is the rebase TriggerRebaseInput asks for, once per
// RequestID.
type TriggerRebaseCommand struct {
	RequestID uuid.UUID
	TriggerRebaseInput
}

// TriggerSingleNodeRunCommand is the single-node run
// TriggerSingleNodeRunInput asks for, once per RequestID.
type TriggerSingleNodeRunCommand struct {
	RequestID uuid.UUID
	TriggerSingleNodeRunInput
}

// CancelActiveRunCommand cancels the active run of TenantID's schedule
// ScheduleName, once per RequestID.
type CancelActiveRunCommand struct {
	TenantID           string
	RequestID          uuid.UUID
	ScheduleName       string
	CancelledBy        string
	CancellationReason string
}

// CancelRunCommand cancels TenantID's run RunID.
type CancelRunCommand struct {
	TenantID           string
	RunID              uuid.UUID
	CancelledBy        string
	CancellationReason string
}

// RunCommands are the commands that start or cancel a run. Each command that
// names a request id runs at most once per request id of its tenant
// (commands.Once): a repeat with the same parameters answers with the first
// call's run and writes nothing, the same id with other parameters is
// commandrequest.ErrReused, and a refused call stores nothing.
type RunCommands struct {
	newUoW  func() uow.UnitOfWork
	metrics ports.RunMetrics
	start   *StartRunHandler
	rerun   *TriggerRerunHandler
	rebase  *TriggerRebaseHandler
	single  *TriggerSingleNodeRunHandler
	cancel  *CancelRunHandler
}

// NewRunCommands builds the commands over newUoW; planner plans the runs they
// start and metrics counts each command record's outcome.
func NewRunCommands(newUoW func() uow.UnitOfWork, metrics ports.RunMetrics, planner runstart.Planner, logger *slog.Logger) *RunCommands {
	return &RunCommands{
		newUoW:  newUoW,
		metrics: metrics,
		start:   NewStartRunHandler(planner, logger),
		rerun:   NewTriggerRerunHandler(planner, logger),
		rebase:  NewTriggerRebaseHandler(planner, logger),
		single:  NewTriggerSingleNodeRunHandler(planner, logger),
		cancel:  NewCancelRunHandler(logger),
	}
}

// StartRun starts a run of the schedule, of kind trigger, once per request id.
// The fingerprint is the schedule and the operation, an empty operation read
// as run. A schedule with an active run is run.ErrScheduleHasActiveRun, and
// a refused selection is the planner's refusal; neither stores a record.
func (c *RunCommands) StartRun(ctx context.Context, cmd StartRunCommand) (CommandResult, error) {
	runID := uuid.New()
	rec := c.record(cmd.TenantID, cmd.RequestID, commandStartRun, runID, cmd.ScheduleName, operationPart(cmd.Operation))
	id, replayed, err := commands.Once(ctx, c.newUoW, c.metrics, rec, func(u uow.UnitOfWork) (uuid.UUID, error) {
		err := c.start.StartInTransaction(ctx, u, StartRunInput{
			RunID: runID, TenantID: cmd.TenantID, ScheduleName: cmd.ScheduleName, Kind: run.KindTrigger,
			Initiator: cmd.Initiator, Operation: cmd.Operation,
		})
		return runID, err
	})
	if err != nil {
		return CommandResult{}, err
	}
	return CommandResult{RunID: id, ScheduleName: cmd.ScheduleName, Replayed: replayed}, nil
}

// TriggerRerun reruns the source run once per request id. The fingerprint is
// the source run.
func (c *RunCommands) TriggerRerun(ctx context.Context, cmd TriggerRerunCommand) (CommandResult, error) {
	runID := uuid.New()
	rec := c.record(cmd.TenantID, cmd.RequestID, commandTriggerRerun, runID, cmd.SourceRunID.String())
	return c.create(ctx, cmd.TenantID, rec, func(u uow.UnitOfWork) (string, error) {
		return c.rerun.HandleInTransaction(ctx, u, runID, cmd.TenantID, cmd.SourceRunID, cmd.Initiator)
	})
}

// TriggerRebase rebases the source run once per request id. The fingerprint
// is the source run, include_changed_upstream and the expected release.
func (c *RunCommands) TriggerRebase(ctx context.Context, cmd TriggerRebaseCommand) (CommandResult, error) {
	runID := uuid.New()
	rec := c.record(cmd.TenantID, cmd.RequestID, commandTriggerRebase, runID,
		cmd.SourceRunID.String(), strconv.FormatBool(cmd.IncludeChangedUpstream), cmd.ExpectedReleaseID)
	return c.create(ctx, cmd.TenantID, rec, func(u uow.UnitOfWork) (string, error) {
		return c.rebase.HandleInTransaction(ctx, u, runID, cmd.TriggerRebaseInput)
	})
}

// TriggerSingleNodeRun runs one node once per request id. The fingerprint is
// the node as service.schema.table, the metadata source, the source run (empty
// when none) and the operation, an empty operation read as run.
func (c *RunCommands) TriggerSingleNodeRun(ctx context.Context, cmd TriggerSingleNodeRunCommand) (CommandResult, error) {
	runID := uuid.New()
	source := ""
	if cmd.SourceRunID != nil {
		source = cmd.SourceRunID.String()
	}
	t := cmd.Target
	rec := c.record(cmd.TenantID, cmd.RequestID, commandTriggerSingleNodeRun, runID,
		t.ServiceName+"."+t.SchemaName+"."+t.TableName, string(cmd.MetadataSource), source, operationPart(cmd.Operation))
	return c.create(ctx, cmd.TenantID, rec, func(u uow.UnitOfWork) (string, error) {
		return c.single.HandleInTransaction(ctx, u, runID, cmd.TriggerSingleNodeRunInput)
	})
}

// CancelActiveRun cancels the schedule's active run once per request id, and
// answers with it. The fingerprint is the schedule. A retry answers with the
// run its first call cancelled even once the schedule has another active
// run. A schedule with no active run is ErrNoActiveRun and stores no record,
// so the same id may cancel the run the schedule starts later.
func (c *RunCommands) CancelActiveRun(ctx context.Context, cmd CancelActiveRunCommand) (CommandResult, error) {
	rec := c.record(cmd.TenantID, cmd.RequestID, commandCancelActiveRun, uuid.Nil, cmd.ScheduleName)
	var scheduleName string
	id, replayed, err := commands.Once(ctx, c.newUoW, c.metrics, rec, func(u uow.UnitOfWork) (uuid.UUID, error) {
		active, err := u.Run().GetActiveRun(ctx, cmd.TenantID, cmd.ScheduleName)
		if err != nil {
			return uuid.Nil, fmt.Errorf("get the active run: %w", err)
		}
		if active == nil {
			return uuid.Nil, nil
		}
		r, err := c.cancel.CancelInTransaction(ctx, u, active.TenantID(), active.RunID(), cmd.CancelledBy, cmd.CancellationReason)
		if err != nil {
			return uuid.Nil, err
		}
		scheduleName = r.ScheduleName()
		return r.RunID(), nil
	})
	if err != nil {
		return CommandResult{}, err
	}
	if id == uuid.Nil {
		return CommandResult{}, fmt.Errorf("%w: %q", ErrNoActiveRun, cmd.ScheduleName)
	}
	if replayed {
		scheduleName = cmd.ScheduleName
	}
	return CommandResult{RunID: id, ScheduleName: scheduleName, Replayed: replayed}, nil
}

// CancelRun cancels the run in a transaction of its own and returns its id.
// A run that is already cancelling or cancelled is left as it is. It takes no
// request id: cancelling a run twice changes nothing.
func (c *RunCommands) CancelRun(ctx context.Context, cmd CancelRunCommand) (uuid.UUID, error) {
	u := c.newUoW()
	if err := u.Begin(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("begin the cancel: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()
	r, err := c.cancel.CancelInTransaction(ctx, u, cmd.TenantID, cmd.RunID, cmd.CancelledBy, cmd.CancellationReason)
	if err != nil {
		return uuid.Nil, err
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, fmt.Errorf("commit the cancel: %w", err)
	}
	committed = true
	return r.RunID(), nil
}

// create runs do, which creates the run rec names and returns its schedule
// name, once per request id. A replay reads the schedule name of the run the
// first call created.
func (c *RunCommands) create(ctx context.Context, tenantID string, rec commandrequest.Record, do func(u uow.UnitOfWork) (string, error)) (CommandResult, error) {
	var scheduleName string
	id, replayed, err := commands.Once(ctx, c.newUoW, c.metrics, rec, func(u uow.UnitOfWork) (uuid.UUID, error) {
		name, err := do(u)
		if err != nil {
			return uuid.Nil, err
		}
		scheduleName = name
		return rec.RunID, nil
	})
	if err != nil {
		return CommandResult{}, err
	}
	if replayed {
		r, err := c.newUoW().Run().GetRun(ctx, tenantID, id)
		if err != nil {
			return CommandResult{}, fmt.Errorf("read the run of request %s: %w", rec.RequestID, err)
		}
		scheduleName = r.ScheduleName()
	}
	return CommandResult{RunID: id, ScheduleName: scheduleName, Replayed: replayed}, nil
}

// record is the command record of a call of command under requestID, naming
// runID, with the fingerprint of parts.
func (c *RunCommands) record(tenantID string, requestID uuid.UUID, command string, runID uuid.UUID, parts ...string) commandrequest.Record {
	return commandrequest.Record{
		TenantID:    tenantID,
		RequestID:   requestID,
		Command:     command,
		Fingerprint: commandrequest.Fingerprint(command, parts...),
		RunID:       runID,
	}
}

// operationPart is an operation as a fingerprint part: the empty operation is
// read as run.
func operationPart(op model.Operation) string {
	if op == model.OperationRun {
		return "run"
	}
	return string(op)
}
