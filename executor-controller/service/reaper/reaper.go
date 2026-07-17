// Package reaper recovers the tasks of workers that stopped reporting. A lease
// is a worker's promise to keep saying it is alive; when the promise lapses the
// executor has to assume the worker is gone, take its task back, and return the
// execution slot the task holds — otherwise a single crashed pod costs the
// executor a slot for good and hangs the run waiting on a node that will never
// report.
package reaper

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/carolsimone/continuo/executor-controller/service/tasklifecycle"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// The failure an expired lease is recorded as. The worker is unreachable, so the
// executor reports what it observed — the heartbeats stopped — rather than
// anything about the dbt run itself.
const (
	expiredErrorClass   = "worker_lease_expired"
	expiredErrorMessage = "worker heartbeat expired"
)

// Config wires a Reaper to its collaborators.
type Config struct {
	// UnitOfWork returns a fresh unit of work, so each recovery commits its
	// transition and the announcements it owes together and alone.
	UnitOfWork func() uow.UnitOfWork
	// Pods stops the pod of a lease that expired.
	Pods  ports.PodTerminator
	Clock ports.Clock
	// RetryBackoff is how long a recovered task waits before another worker may
	// claim it.
	RetryBackoff time.Duration
	// Tick is how often expired leases are looked for; default 10s.
	Tick   time.Duration
	Logger *slog.Logger
}

// Reaper takes back the tasks of workers whose leases have expired.
type Reaper struct {
	newUoW       func() uow.UnitOfWork
	pods         ports.PodTerminator
	clock        ports.Clock
	fanout       tasklifecycle.Fanout
	retryBackoff time.Duration
	tick         time.Duration
	logger       *slog.Logger
}

// NewReaper constructs the lease reaper.
func NewReaper(cfg Config) *Reaper {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Second
	}
	return &Reaper{
		newUoW:       cfg.UnitOfWork,
		pods:         cfg.Pods,
		clock:        cfg.Clock,
		retryBackoff: cfg.RetryBackoff,
		tick:         cfg.Tick,
		logger:       cfg.Logger,
	}
}

// Run reaps expired leases until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.tick)
	defer ticker.Stop()
	r.logger.Info("Starting worker lease reaper", "tick", r.tick)
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Worker lease reaper stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := r.ReapExpired(ctx); err != nil {
				r.logger.Error("Reaping expired worker leases failed", "error", err)
			}
		}
	}
}

// ReapExpired recovers every lease that has expired by now, one transaction
// each. Each recovery drives its task out of 'leased'/'running', so the set it
// works through strictly shrinks and the loop ends.
func (r *Reaper) ReapExpired(ctx context.Context) error {
	for {
		found, err := r.ReapOne(ctx)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
	}
}

// ReapOne recovers a single expired lease, reporting whether it found one.
//
// The pod is stopped first and inside the transaction, so a Kubernetes API the
// reaper cannot reach rolls the recovery back rather than committing it: the
// lease it failed to fence stays the authoritative one, no other worker can
// claim the task, and the next tick tries again. Committing the release of the
// slot without knowing the pod stopped is the one outcome that must not happen —
// the slot would start other work beside a dbt process still writing.
func (r *Reaper) ReapOne(ctx context.Context) (bool, error) {
	found := false
	err := r.withTx(ctx, func(repo repository.DeploymentRepository, outboxRepo outbox.Repository) error {
		now := r.clock.Now()
		dep, err := repo.GetExpiredLeaseForUpdate(ctx, now)
		if err != nil {
			return fmt.Errorf("get expired lease: %w", err)
		}
		if dep == nil {
			return nil
		}
		found = true
		return r.recover(ctx, repo, outboxRepo, dep, now)
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// recover takes one task back from the worker that stopped reporting on it. The
// execution being announced is read before any transition, because dropping the
// expired lease drops the identity and the start time that describe it.
func (r *Reaper) recover(
	ctx context.Context,
	repo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	dep *model.Deployment,
	now time.Time,
) error {
	lease := dep.ActiveLease()
	if lease == nil {
		// The lookup serves rows a worker holds, so one without a lease means the
		// row's status and its lease columns disagree. The reaper polls from its
		// own goroutine: reading the lease anyway would panic and take every
		// dispatch in the service down with it, so the row is reported and left
		// for an operator.
		return fmt.Errorf("deployment %s is %s with no lease to expire", dep.ID(), dep.Status())
	}
	exec := execution(lease, now)
	if err := r.stopPod(ctx, dep, lease); err != nil {
		return err
	}
	if err := dep.ExpireLease(lease.ID); err != nil {
		return fmt.Errorf("fence expired lease of deployment %s: %w", dep.ID(), err)
	}

	cmd := dep.Command()
	if cmd.TaskRetryCount+1 < cmd.TaskMaxRetries {
		if err := dep.MarkRetryPending(now, r.retryBackoff); err != nil {
			return fmt.Errorf("park expired deployment %s for retry: %w", dep.ID(), err)
		}
		if err := repo.Save(ctx, dep); err != nil {
			return fmt.Errorf("save expired deployment %s: %w", dep.ID(), err)
		}
		r.logger.Warn("Worker lease expired — the task will be offered again",
			"deployment_id", dep.ID(), "task_id", cmd.TaskID, "lease_id", lease.ID,
			"owner", lease.Owner, "attempt", lease.Attempt)
		return r.fanout.RetryableFailure(ctx, outboxRepo, dep, exec, expiredResult(true))
	}

	// The task's last allowed attempt is the one that went silent: nothing is
	// left to offer, so the node settles and the run advances past it.
	if err := dep.MarkFailed(now, expiredErrorMessage); err != nil {
		return fmt.Errorf("fail expired deployment %s: %w", dep.ID(), err)
	}
	if err := repo.Save(ctx, dep); err != nil {
		return fmt.Errorf("save expired deployment %s: %w", dep.ID(), err)
	}
	r.logger.Error("Worker lease expired with no attempts left — failing the task",
		"deployment_id", dep.ID(), "task_id", cmd.TaskID, "lease_id", lease.ID,
		"owner", lease.Owner, "attempt", lease.Attempt)
	return r.fanout.PermanentFailure(ctx, outboxRepo, dep, exec, expiredResult(false))
}

// expiredResult describes the failure an expired lease is announced as. retryable
// states whether the task has an attempt left, so the announced result agrees
// with the transition the recovery made: a task parked for requeue is reported
// retryable, and one that has exhausted its attempts is reported as the permanent
// failure it settled as.
func expiredResult(retryable bool) model.WorkerResult {
	return model.WorkerResult{
		Succeeded:    false,
		Retryable:    retryable,
		ErrorClass:   expiredErrorClass,
		ErrorMessage: expiredErrorMessage,
	}
}

// stopPod stops the pod that held the expired lease. A lease that never named
// its pod cannot be stopped by UID, and a delete by an empty name would fail
// every tick, leaving the task's slot held forever; the recovery goes ahead
// instead, which still fences every report the worker sends.
func (r *Reaper) stopPod(ctx context.Context, dep *model.Deployment, lease *model.Lease) error {
	if lease.PodName == "" || lease.PodUID == "" {
		r.logger.Warn("Reaped a lease whose worker named no pod — its pod cannot be stopped",
			"deployment_id", dep.ID(), "lease_id", lease.ID, "owner", lease.Owner)
		return nil
	}
	if err := r.pods.DeletePod(ctx, lease.PodName, lease.PodUID); err != nil {
		return fmt.Errorf("stop worker pod %s of expired deployment %s: %w",
			lease.PodName, dep.ID(), err)
	}
	return nil
}

// execution describes the run the expired lease was in the middle of: the lease
// that ran it, when its dbt process started, and when the executor gave up on
// it. A lease that expired before its worker reported a start times no start.
func execution(lease *model.Lease, now time.Time) tasklifecycle.Execution {
	completedAt := now
	exec := tasklifecycle.Execution{ID: lease.ID, CompletedAt: &completedAt}
	if lease.StartedAt != nil {
		startedAt := *lease.StartedAt
		exec.StartedAt = &startedAt
	}
	return exec
}

// withTx runs fn inside one transaction, committing the recovered task's new
// state and the announcements it owes together. A failure rolls both back.
func (r *Reaper) withTx(
	ctx context.Context,
	fn func(repository.DeploymentRepository, outbox.Repository) error,
) error {
	u := r.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err := u.Rollback(); err != nil {
			r.logger.Error("rollback failed", "error", err)
		}
	}()
	if err := fn(u.DeploymentsRepo(), u.OutboxRepo()); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
