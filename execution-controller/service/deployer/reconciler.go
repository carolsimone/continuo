package deployer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// ReconcilerConfig groups the reconciler's timings; zero values select the
// defaults.
type ReconcilerConfig struct {
	// Interval between passes; default 60s.
	Interval time.Duration
	// ReservedGrace is how long a deployment may stay reserved before its
	// launcher counts as gone and the reservation returns to pending; default
	// 2m. A live launcher creates the Job within seconds of the claim.
	ReservedGrace time.Duration
	// StartedGrace is how long a started deployment's Job may be missing, or
	// finished without its outcome recorded, before the reconciler acts;
	// default 2m.
	StartedGrace time.Duration
}

// Reconciler compares the deployments holding a slot with Kubernetes and
// repairs what a crash or a lost status check left behind. It never starts work
// and never reports a result itself: a stale reservation goes back to the
// queue, a missing Job gets a status check that reports it, and a Job that
// finished unobserved only gives its slot back.
type Reconciler struct {
	db           *sqlx.DB
	inventory    ports.JobInventory
	newAdmission AdmissionRepoFactory
	newRepo      RepoFactory
	logger       *slog.Logger
	cfg          ReconcilerConfig
	now          func() time.Time
}

// NewReconciler builds a Reconciler; zero fields of cfg select the defaults.
func NewReconciler(db *sqlx.DB, inventory ports.JobInventory, newAdmission AdmissionRepoFactory, newRepo RepoFactory, logger *slog.Logger, cfg ReconcilerConfig) *Reconciler {
	if cfg.Interval == 0 {
		cfg.Interval = time.Minute
	}
	if cfg.ReservedGrace == 0 {
		cfg.ReservedGrace = 2 * time.Minute
	}
	if cfg.StartedGrace == 0 {
		cfg.StartedGrace = 2 * time.Minute
	}
	return &Reconciler{db: db, inventory: inventory, newAdmission: newAdmission,
		newRepo: newRepo, logger: logger, cfg: cfg, now: time.Now}
}

// Run reconciles every Interval until ctx is done.
func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("Admission reconcile failed", "error", err)
			}
		}
	}
}

// ReconcileOnce runs one pass:
//   - a deployment reserved for longer than ReservedGrace goes back to pending,
//     and the claim re-admits it in fair order (a Job its lost launcher
//     created is found again by name at the next launch);
//   - a started deployment whose Job is missing gets a status check, which
//     reports it failed through the job-status handler;
//   - a started deployment whose Job finished more than StartedGrace ago
//     without a status check recording it releases its slot, outcome unset.
//
// The Job listing runs only when some deployment has been started for longer
// than StartedGrace. Exported for tests.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	adm := r.newAdmission(r.db)

	returned, err := adm.ReturnStaleReserved(ctx, r.cfg.ReservedGrace)
	if err != nil {
		return err
	}
	for _, id := range returned {
		r.logger.Warn("Deployment reserved without a Job — returned to the queue", "deployment_id", id)
	}

	started, err := adm.ListStarted(ctx, r.cfg.StartedGrace)
	if err != nil {
		return err
	}
	if len(started) == 0 {
		return nil
	}
	jobs, err := r.inventory.ListJobs(ctx)
	if err != nil {
		return fmt.Errorf("list jobs: %w", err)
	}
	now := r.now()
	var errs []error
	for _, dep := range started {
		job, ok := jobs[dep.JobName()]
		switch {
		case !ok:
			errs = append(errs, r.recheck(ctx, dep.ID()))
		case job.Finished && now.Sub(job.FinishedAt) >= r.cfg.StartedGrace:
			errs = append(errs, r.releaseSlot(ctx, dep.ID()))
		}
	}
	return errors.Join(errs...)
}

// recheck schedules a status check for a started deployment whose Job is gone
// and restarts its staleness clock, so the next pass waits StartedGrace before
// checking again.
func (r *Reconciler) recheck(ctx context.Context, id uuid.UUID) error {
	return r.inStarted(ctx, id, func(tx *sqlx.Tx, dep *model.Deployment) error {
		entry, err := checkTicket(dep, r.now(), dep.Status() == model.StatusRunning)
		if err != nil {
			return err
		}
		if err := outbox.NewPostgresRepository(tx, "execution_outbox", r.logger).Create(ctx, entry); err != nil {
			return fmt.Errorf("write check ticket: %w", err)
		}
		r.logger.Warn("Job of a started deployment is missing — checking its status",
			"deployment_id", dep.ID(), "job_name", dep.JobName())
		return r.newAdmission(tx).TouchStateChanged(ctx, dep.ID())
	})
}

// releaseSlot frees the slot of a started deployment whose Job finished
// without a status check recording its outcome.
func (r *Reconciler) releaseSlot(ctx context.Context, id uuid.UUID) error {
	return r.inStarted(ctx, id, func(tx *sqlx.Tx, dep *model.Deployment) error {
		if err := dep.ReleaseSlot(); err != nil {
			return err
		}
		r.logger.Warn("Job finished without its status being recorded — releasing its slot",
			"deployment_id", dep.ID(), "job_name", dep.JobName())
		return r.newRepo(tx).Save(ctx, dep)
	})
}

// inStarted runs fn on id in its own transaction while id is still started
// and no other transaction holds it.
func (r *Reconciler) inStarted(ctx context.Context, id uuid.UUID, fn func(*sqlx.Tx, *model.Deployment) error) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	dep, err := r.newAdmission(tx).GetStarted(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := fn(tx, dep); err != nil {
		return fmt.Errorf("reconcile deployment %s: %w", id, err)
	}
	return tx.Commit()
}
