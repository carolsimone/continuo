package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// DefaultStepTimeout bounds one step of a drive: what runs between two commits,
// such as a model call, a pre-flight pass or a gate submission. A step that
// runs longer is stopped and counted as a failure.
const DefaultStepTimeout = 10 * time.Minute

// errStepTimeout is the cause of the context a step runs in once it ran past
// its limit, which tells that end of the context from the end of the drive.
var errStepTimeout = errors.New("step timed out")

// leaseDuration is how long a worker's lease on a remediation lasts after the
// runner last held it. Every step commit holds it again.
const leaseDuration = 5 * time.Minute

// Deps is what a Runner works with. Registry holds the steps built from
// Collab, and Files the flows and types the install loaded. LLM is the model
// every prompt step calls, through the response cache. Owner names the worker
// whose lease every commit holds.
type Deps struct {
	NewUoW   func() uow.UnitOfWork
	Registry *Registry
	Files    workflow.Set
	LLM      ports.LLMProvider
	Collab   Collaborators
	Clock    ports.Clock
	Logger   *slog.Logger
	// Maintenance is MAINTENANCE_ENABLED. While true, a try that comes due is
	// recorded skipped for every node instead of calling the model.
	Maintenance bool
	Owner       string
	// VerificationTimeout is how long a try's verification runs may take. A
	// verifying try's deadline is recorded this long, plus an hour, after the
	// gate started its runs; it is informational, and the gate's own timeouts,
	// which the registry's gate step reads from Collab, end a verification.
	VerificationTimeout time.Duration
	// StepTimeout bounds each step of a drive. Zero is ten minutes.
	StepTimeout time.Duration
}

// Runner runs each remediation as a sequence of committed steps. Every step's
// result and the change it causes commit together, after the runner holds the
// worker's lease, so a restart continues from the last committed step and two
// workers never write to one remediation.
type Runner struct {
	d      Deps
	clock  ports.Clock
	logger *slog.Logger
}

// New builds a Runner. A nil Clock is the system clock and a nil Logger the
// default logger.
func New(d Deps) *Runner {
	r := &Runner{d: d, clock: d.Clock, logger: d.Logger}
	if r.clock == nil {
		r.clock = ports.SystemClock{}
	}
	if r.logger == nil {
		r.logger = slog.Default()
	}
	return r
}

// tryRun is what one step of a drive works from: the remediation as last
// committed, the flow it follows, the trigger it was created from, and the
// journal of its current try.
type tryRun struct {
	snap    remediation.Snapshot
	flow    workflow.Flow
	trigger remediation.Trigger
	journal journal
}

// Drive runs the remediation's steps, one committed step at a time, until it
// waits on something outside the worker, ends, or the worker's lease is lost.
// The caller holds the lease. A remediation with no try, or whose try failed,
// gets its next try opened first. Then every cluster of the try runs its steps
// to an end, and the try is finished: settled when it has nothing to verify,
// otherwise checked across its clusters and submitted to the gate, where it
// waits on the gate's poller. A lost lease stops the drive quietly with nil,
// since another worker holds the remediation. A step that errors is recorded
// on its journal row, and its error is returned; the next drive runs it again.
// A remediation that cannot be loaded, or whose next try cannot open, has the
// error counted on its own row, and the error is returned.
func (r *Runner) Drive(ctx context.Context, id uuid.UUID) error {
	err := r.drive(ctx, id)
	if errors.Is(err, repository.ErrLeaseLost) {
		r.logger.Info("remediation lease lost; stopping the drive", "remediation", id, "owner", r.d.Owner)
		return nil
	}
	return err
}

func (r *Runner) drive(ctx context.Context, id uuid.UUID) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		run, running, err := r.load(ctx, id)
		if err != nil || !running {
			return err
		}
		if run.snap.Try == nil || run.snap.Try.Status == remediation.TryFailed {
			sctx, cancel := r.stepContext(ctx)
			err := r.openTry(sctx, run)
			if err != nil {
				err = r.countRemediationFailure(sctx, id, err)
			}
			cancel()
			if err != nil {
				return err
			}
			continue
		}
		switch run.snap.Try.Status {
		case remediation.TryDrafting:
			sctx, cancel := r.stepContext(ctx)
			if c, open := nextOpenCluster(run.snap.Clusters); open {
				err = r.runClusterStep(sctx, run, c)
			} else {
				err = r.finishTry(sctx, run)
			}
			cancel()
		case remediation.TryChecking:
			sctx, cancel := r.stepContext(ctx)
			err = r.finishTry(sctx, run)
			cancel()
		default:
			// A verifying try waits on the gate's poller, and a passed try
			// ends with its remediation.
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// stepLimit is how long one step may run.
func (r *Runner) stepLimit() time.Duration {
	if r.d.StepTimeout > 0 {
		return r.d.StepTimeout
	}
	return DefaultStepTimeout
}

// stepContext is the context one step runs in: ctx, ended after the step's
// limit. Its end is told from the end of ctx by errStepTimeout being its cause.
func (r *Runner) stepContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, r.stepLimit(), errStepTimeout)
}

// stepTimedOut reports whether ctx, a step's context, ended because the step ran
// past its limit rather than because the drive ended.
func stepTimedOut(ctx context.Context) bool {
	return ctx.Err() != nil && errors.Is(context.Cause(ctx), errStepTimeout)
}

// load reads the remediation as committed. running is false when it has ended,
// and the run is then empty. An error, such as a flow the loaded files do not
// define, is counted on the remediation's own row and returned.
func (r *Runner) load(ctx context.Context, id uuid.UUID) (run tryRun, running bool, err error) {
	run, running, err = r.readRun(ctx, id)
	if err != nil {
		return tryRun{}, false, r.countRemediationFailure(ctx, id, err)
	}
	return run, running, nil
}

// readRun reads the remediation as committed and what it runs on: its flow,
// its trigger and the journal of its current try. The remediation is a Get
// snapshot: the caller holds the lease, so no other worker writes it, and the
// commit that acts on what the step does holds the lease again first.
func (r *Runner) readRun(ctx context.Context, id uuid.UUID) (run tryRun, running bool, err error) {
	var snap remediation.Snapshot
	if err := r.read(ctx, func(u uow.UnitOfWork) error {
		var err error
		snap, err = u.RemediationRepo().Get(ctx, id)
		return err
	}); err != nil {
		return tryRun{}, false, fmt.Errorf("load remediation %s: %w", id, err)
	}
	rem := snap.Remediation
	if rem.Status != remediation.StatusRunning {
		return tryRun{}, false, nil
	}
	flow, ok := r.d.Files.Flows[rem.Flow]
	if !ok {
		return tryRun{}, false, fmt.Errorf("remediation %s follows flow %q, which the loaded files do not define", id, rem.Flow)
	}
	t, err := serialization.DecodeRemediationRequestedV2(rem.TriggerPayload)
	if err != nil {
		return tryRun{}, false, fmt.Errorf("remediation %s: %w", id, err)
	}
	t.TenantID = rem.TenantID
	t.Round = rem.Round
	return tryRun{snap: snap, flow: flow, trigger: t, journal: newJournal(snap.Steps)}, true, nil
}

// nextOpenCluster returns the first cluster, in cluster order, that has not
// reached an end.
func nextOpenCluster(clusters []remediation.Cluster) (remediation.Cluster, bool) {
	for _, c := range clusters {
		switch c.Status {
		case remediation.ClusterChecked, remediation.ClusterHandedOver, remediation.ClusterFellBack:
			continue
		}
		return c, true
	}
	return remediation.Cluster{}, false
}

// countRemediationFailure counts cause on the remediation's own row, for a
// drive that failed before a step's row could hold the error: the remediation
// could not be loaded, or its next try could not open. In a transaction of its
// own after holding the lease, the remediation's failures go up by one and
// cause, without NUL characters, becomes its last error. It returns cause. A
// lost lease is returned as it is, and nothing is recorded for a drive whose
// context ended. A step that ran past its limit is recorded, with a cause that
// says so, in a context that is not ended.
func (r *Runner) countRemediationFailure(ctx context.Context, id uuid.UUID, cause error) error {
	if errors.Is(cause, repository.ErrLeaseLost) {
		return cause
	}
	if ctx.Err() != nil {
		if !stepTimedOut(ctx) {
			return cause
		}
		cause = fmt.Errorf("the step ran longer than %s and was stopped: %w", r.stepLimit(), cause)
		ctx = context.WithoutCancel(ctx)
	}
	lastError := strings.ReplaceAll(cause.Error(), "\x00", "")
	err := r.commit(ctx, id, func(u uow.UnitOfWork) error {
		return u.RemediationRepo().CountFailure(ctx, id, lastError)
	})
	switch {
	case errors.Is(err, repository.ErrLeaseLost):
		return err
	case err != nil:
		return errors.Join(cause, fmt.Errorf("record the failure of remediation %s: %w", id, err))
	}
	r.logger.Warn("remediation failed before a step could run; the next drive tries again",
		"remediation", id, "error", cause)
	return cause
}

// failuresToClear reports whether run's remediation was loaded with failures
// counted on its own row and with a try open: one that has not failed. Such a
// load ended the failures in a row, and the next step that commits clears
// them. A remediation with no try, or whose try failed, keeps its failures
// until a try opens, so a try that cannot open is never cleared by the load
// that precedes the next attempt to open it.
func failuresToClear(run tryRun) bool {
	t := run.snap.Try
	return run.snap.Remediation.Failures > 0 && t != nil && t.Status != remediation.TryFailed
}

// clearFailures clears, in u's transaction, the failures run's remediation
// counted on its own row, when failuresToClear says the load ended them.
func (r *Runner) clearFailures(ctx context.Context, u uow.UnitOfWork, run tryRun) error {
	if !failuresToClear(run) {
		return nil
	}
	id := run.snap.Remediation.ID
	if err := u.RemediationRepo().ClearFailures(ctx, id); err != nil {
		return fmt.Errorf("clear the failures of remediation %s: %w", id, err)
	}
	return nil
}

// read runs fn in a transaction it rolls back.
func (r *Runner) read(ctx context.Context, fn func(u uow.UnitOfWork) error) error {
	u := r.d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()
	return fn(u)
}

// commit runs fn in one transaction after holding the worker's lease on the
// remediation, and commits it. A lease the worker no longer holds is
// repository.ErrLeaseLost, and nothing is written.
func (r *Runner) commit(ctx context.Context, id uuid.UUID, fn func(u uow.UnitOfWork) error) error {
	u := r.d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()
	if err := u.RemediationRepo().Hold(ctx, id, r.d.Owner, r.clock.Now(), leaseDuration); err != nil {
		return err
	}
	if err := fn(u); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// endRemediation moves the remediation to done in u's transaction.
func (r *Runner) endRemediation(ctx context.Context, u uow.UnitOfWork, rem remediation.Remediation) error {
	if !rem.Status.CanMoveTo(remediation.StatusDone) {
		return fmt.Errorf("remediation %s cannot move from %s to %s", rem.ID, rem.Status, remediation.StatusDone)
	}
	rem.Status = remediation.StatusDone
	rem.UpdatedAt = r.clock.Now()
	if err := u.RemediationRepo().SaveRemediation(ctx, rem); err != nil {
		return fmt.Errorf("save remediation: %w", err)
	}
	return nil
}
