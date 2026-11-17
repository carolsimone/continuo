package handlers

import (
	"context"
	"fmt"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// isStoredPythonRun reports whether r is an unfinished run of the kind python.
// Only a row an earlier continuo version accepted holds that kind: no parser
// reads its contract.yaml, and the service_prod row its promotion would write
// would name an artifact no release can read, so such a run never promotes or
// passes.
func isStoredPythonRun(r *pipeline.Run) bool {
	return r.ManifestKind() == release.ManifestKindPython && !r.Status().IsTerminal()
}

// rejectStoredPythonRun ends a stored kind-python run before any leg acts on
// it: a candidate ends rejected invalid_artifact and emits release.rejected:v1
// with the parse body, a verification ends failed with no release event, and
// both emit pipeline.run.finished:v1. The detail is the refusal
// ParseManifestKind gives for the kind. invalid_artifact is not healable, so
// remediation does not act on the rejection. It writes neither current_prod
// nor service_prod, and it commits u.
func rejectStoredPythonRun(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, now time.Time) error {
	d.Logger.Warn("rejecting a stored run of the kind python",
		"release_id", r.ID(), "service", r.ChangedService(), "status", string(r.Status()))
	detail := release.ErrPythonKindNotAccepted.Error()
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
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := enqueueRunFinished(ctx, u, r, now); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	recordTerminalTelemetry(ctx, d, r, 0)
	return nil
}
