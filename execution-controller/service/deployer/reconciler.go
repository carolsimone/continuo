package deployer

import (
	"context"
	"log/slog"
	"time"

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
}

// Reconciler returns to the queue the reservations a crashed launcher left
// behind. It never starts work and never reports a result: the observer
// follows every Job that was created.
type Reconciler struct {
	db           *sqlx.DB
	newAdmission AdmissionRepoFactory
	logger       *slog.Logger
	cfg          ReconcilerConfig
}

// NewReconciler builds a Reconciler; zero fields of cfg select the defaults.
func NewReconciler(db *sqlx.DB, newAdmission AdmissionRepoFactory, logger *slog.Logger, cfg ReconcilerConfig) *Reconciler {
	if cfg.Interval == 0 {
		cfg.Interval = time.Minute
	}
	if cfg.ReservedGrace == 0 {
		cfg.ReservedGrace = 2 * time.Minute
	}
	return &Reconciler{db: db, newAdmission: newAdmission, logger: logger, cfg: cfg}
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

// ReconcileOnce moves every deployment reserved for longer than ReservedGrace
// back to pending; the claim re-admits it in fair order, and a Job its lost
// launcher created is found again by name at the next launch. Exported for
// tests.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	returned, err := r.newAdmission(r.db).ReturnStaleReserved(ctx, r.cfg.ReservedGrace)
	if err != nil {
		return err
	}
	for _, id := range returned {
		r.logger.Warn("Deployment reserved without a Job — returned to the queue", "deployment_id", id)
	}
	return nil
}
