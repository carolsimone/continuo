package handlers

import (
	"context"
	"fmt"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// rejectInvalidArtifactCheck ends a check that no leg can act on: a candidate ends
// rejected invalid_artifact and emits release.rejected:v1 with the parse body,
// a fix's release check ends failed with no release event, and both emit
// release_check.finished:v1. The detail names why the check cannot proceed.
// invalid_artifact is not healable, so remediation does not act on the
// rejection. It writes neither the live release nor a production pointer, and it
// commits u.
func rejectInvalidArtifactCheck(ctx context.Context, d *Deps, u uow.UnitOfWork, r *releasecheck.Check, detail string, now time.Time) error {
	reason := pkg_model.RejectReasonInvalidArtifact
	if err := r.Fail(string(pkg_model.RejectReasonInvalidArtifact), detail, []string{}, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	payload, err := d.Rejections.Encode(ports.ReleaseRejection{
		Shape:         ports.RejectionShapeParse,
		ReleaseID:     r.ID(),
		Reason:        reason,
		ErrorDetail:   detail,
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
	if err := u.ReleaseCheckRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := enqueueCheckFinished(ctx, u, r, now); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	recordTerminalTelemetry(ctx, d, r, 0)
	return nil
}
