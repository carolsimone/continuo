package handlers

import (
	"context"
	"fmt"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// upgradeInterruptedDetail explains an upgrade_interrupted failure to an operator.
const upgradeInterruptedDetail = "the run was waiting on its parse result when release-controller moved candidate " +
	"topologies into artifacts; that result was published on a stream release-controller no longer reads. " +
	"Submit the release again."

// UpgradeLegacyTopologies settles the runs that hold an inline candidate
// topology or were left waiting on a parse result, so every run
// release-controller reads afterwards has a reference to its topology artifact:
//   - a run holding its candidate topology inline gets an artifact written from
//     that topology, so a run in seed_building or validating carries on and a
//     rejected release can still be verified. Its stored candidate SQL URIs are
//     dropped: they equal the ones derived per run.
//   - a run that was parsing when the migration ran and is parsing still waits
//     on a parse result nothing will deliver. It fails with
//     upgrade_interrupted the way any parse failure ends a run, and the queue
//     moves on.
//
// It runs under the release-queue lock, which it holds across its object-store
// writes so that concurrent pods serialize on the one-time step, before the
// stream consumers start. It is idempotent: a run that already has a reference
// is not listed again, and the artifact bytes are deterministic, so a crash
// between writing an artifact and recording its reference is repaired by the
// retry writing the same object. A failure to write an artifact rolls
// everything back and is returned, so the caller can retry it.
func UpgradeLegacyTopologies(ctx context.Context, d *Deps) error {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck
	if err := u.LockReleaseQueue(ctx); err != nil {
		return fmt.Errorf("lock release queue: %w", err)
	}

	legacy, err := u.LegacyTopologyRepo().ListRunsWithLegacyTopology(ctx)
	if err != nil {
		return fmt.Errorf("list runs with an inline topology: %w", err)
	}
	for _, l := range legacy {
		ref, err := d.Topologies.Write(ctx, l.RunID, l.Topology)
		if err != nil {
			return fmt.Errorf("write the topology artifact of run %s: %w", l.RunID, err)
		}
		if err := u.LegacyTopologyRepo().SetRunTopologyRef(ctx, l.RunID, ref); err != nil {
			return err
		}
	}

	parsing, err := u.LegacyTopologyRepo().ListParsingAtUpgrade(ctx)
	if err != nil {
		return fmt.Errorf("list runs parsing at upgrade: %w", err)
	}
	now := d.Clock.Now()
	var failed []*pipeline.Run
	for _, id := range parsing {
		r, err := u.RunRepo().Load(ctx, id)
		if err != nil {
			return fmt.Errorf("load run %s: %w", id, err)
		}
		if r != nil && r.Status() == pipeline.StatusParsing {
			if err := failInterruptedByUpgrade(ctx, d, u, r, now); err != nil {
				return err
			}
			failed = append(failed, r)
		}
		if err := u.LegacyTopologyRepo().ClearParsingAtUpgrade(ctx, id); err != nil {
			return err
		}
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	if len(legacy) > 0 || len(failed) > 0 {
		d.Logger.Info("settled runs holding inline topologies or waiting on a parse result",
			"artifacts_written", len(legacy), "runs_failed", len(failed))
	}
	for _, r := range failed {
		recordTerminalTelemetry(ctx, d, r, 0)
	}
	if len(failed) > 0 {
		if err := AdvanceQueue(ctx, d); err != nil {
			return fmt.Errorf("advance queue after the upgrade step: %w", err)
		}
	}
	return nil
}

// failInterruptedByUpgrade ends a run left parsing by the upgrade the way any
// parse failure ends it — a candidate is rejected and emits
// release.rejected:v1, a verification fails — with the non-healable reason
// upgrade_interrupted, so remediation opens no heal trigger for it.
func failInterruptedByUpgrade(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, now time.Time) error {
	reason := pkg_model.RejectReasonUpgradeInterrupted
	if err := r.Fail(string(reason), upgradeInterruptedDetail, nil, now); err != nil {
		return fmt.Errorf("fail run %s: %w", r.ID(), err)
	}
	payload, err := d.Rejections.Encode(ports.ReleaseRejection{
		Shape:         ports.RejectionShapeParse,
		ReleaseID:     r.ID(),
		Reason:        reason,
		ErrorDetail:   upgradeInterruptedDetail,
		FailingNodes:  []string{},
		PerNode:       []ports.RejectedNode{},
		Repo:          r.Repo(),
		CommitSHA:     r.CommitSHA(),
		CodeBundleURI: r.CodeBundleURI(),
	})
	if err != nil {
		return fmt.Errorf("encode rejection: %w", err)
	}
	if err := emitReleaseRejected(ctx, u, r, payload); err != nil {
		return err
	}
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save run %s: %w", r.ID(), err)
	}
	return enqueueRunFinished(ctx, u, r, now)
}
