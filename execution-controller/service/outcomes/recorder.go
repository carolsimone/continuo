// Package outcomes records a candidate Job's terminal result on its deployments
// row and settles the release leg it belongs to, all on the caller's unit of
// work. The job-status handler calls it once a validation, seed-build or
// compile Job is terminal, so the observation, the outcome and the per-node
// and aggregate results commit together in one transaction.
package outcomes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
)

// NodeOutcome is one candidate Job's terminal result.
type NodeOutcome struct {
	Mode            model.Mode // ModeValidation, ModeSeedBuild or ModeCompile
	ReleaseID       string
	NodeID          string
	Outcome         string // "ok" or "failed"
	DBTLogURI       string
	RunResultsURI   string
	FailedContainer string // compile only
}

// Recorder attaches a candidate Job's terminal outcome to its deployments row
// and settles the release leg, all under the caller's unit of work.
type Recorder struct {
	logger *slog.Logger
	now    func() time.Time
}

// NewRecorder constructs a Recorder.
func NewRecorder(logger *slog.Logger) *Recorder {
	return &Recorder{logger: logger, now: time.Now}
}

// Record persists in.Outcome on the (release, node, mode) deployments row and
// settles the leg: unblocks or skips downstream nodes, emits the per-node
// result, and emits the leg's aggregate once every node has settled. It runs on
// u — the caller's already-open unit of work — and never begins, commits or
// rolls back a transaction itself, so the observation, the outcome and the
// settle all commit together with the caller's other writes.
//
// A row whose outcome is already recorded (dep.OutcomeAt() != nil) is left as
// is, so a re-observed terminal Job is a no-op rather than a double-record. A
// missing row (sql.ErrNoRows) is logged and ignored: the release it belonged to
// is no longer tracked here.
func (r *Recorder) Record(ctx context.Context, u uow.UnitOfWork, in NodeOutcome) error {
	dep, err := u.DeploymentsRepo().GetByReleaseNode(ctx, in.ReleaseID, in.NodeID, in.Mode)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			r.logger.Warn("candidate outcome: no matching deployment",
				"mode", in.Mode, "release_id", in.ReleaseID, "node_id", in.NodeID)
			return nil
		}
		return fmt.Errorf("get %s deployment: %w", in.Mode, err)
	}
	if dep.OutcomeAt() != nil {
		r.logger.Info("candidate outcome already recorded — treating as redelivery",
			"mode", in.Mode, "release_id", in.ReleaseID, "node_id", in.NodeID)
		return nil
	}

	now := r.now()
	if err := dep.RecordOutcome(in.Outcome, in.DBTLogURI, in.RunResultsURI, in.FailedContainer, now); err != nil {
		return fmt.Errorf("record %s outcome: %w", in.Mode, err)
	}
	if err := u.DeploymentsRepo().Save(ctx, dep); err != nil {
		return fmt.Errorf("save %s deployment: %w", in.Mode, err)
	}

	depRepo, outboxRepo, aggRepo := u.DeploymentsRepo(), u.OutboxRepo(), u.ValidationAggregateRepo()
	switch in.Mode {
	case model.ModeValidation:
		return validation.SettleNodeTerminal(ctx, depRepo, outboxRepo, aggRepo, validation.DedupNamespace, in.ReleaseID, in.NodeID, in.Outcome, now)
	case model.ModeSeedBuild:
		return validation.SettleSeedBuildNodeTerminal(ctx, depRepo, outboxRepo, aggRepo, in.ReleaseID, in.NodeID, in.Outcome, now)
	case model.ModeCompile:
		return validation.SettleCompileNodeTerminal(ctx, depRepo, outboxRepo, aggRepo, in.ReleaseID, in.NodeID, in.Outcome, now)
	default:
		return fmt.Errorf("record outcome: unsupported mode %q", in.Mode)
	}
}
