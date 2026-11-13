package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

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
	// VerificationTimeout is how long a try's verification runs may take.
	VerificationTimeout time.Duration
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
// to an end. A lost lease stops the drive quietly with nil, since another
// worker holds the remediation. A step that errors is recorded on its journal
// row, and its error is returned; the next drive runs it again.
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
			if err := r.openTry(ctx, run); err != nil {
				return err
			}
			continue
		}
		switch run.snap.Try.Status {
		case remediation.TryDrafting:
			c, open := nextOpenCluster(run.snap.Clusters)
			if !open {
				return r.finishTry(ctx, run)
			}
			if err := r.runClusterStep(ctx, run, c); err != nil {
				return err
			}
		case remediation.TryChecking:
			return r.finishTry(ctx, run)
		default:
			// A verifying try waits on the gate's poller, and a passed try
			// ends with its remediation.
			return nil
		}
	}
}

// load reads the remediation as committed. running is false when it has ended,
// and the run is then empty.
func (r *Runner) load(ctx context.Context, id uuid.UUID) (run tryRun, running bool, err error) {
	var snap remediation.Snapshot
	if err := r.read(ctx, func(u uow.UnitOfWork) error {
		var err error
		snap, err = u.RemediationRepo().Load(ctx, id)
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
