package handlers

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// NodeValidationResultInput is the wire shape of the kind:"node" message on
// validation.result:v1: a single node's terminal outcome, projected into the
// release read model as it settles.
type NodeValidationResultInput struct {
	ReleaseID     string `json:"release_id"`
	Stage         string `json:"stage"`
	NodeID        string `json:"node_id"`
	Status        string `json:"status"`
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
}

// HandleNodeValidationResult upserts one node's outcome into the release's
// per_node_results read model so the UI can render results incrementally,
// before the kind:"complete" terminal message arrives. It loads the release
// FOR UPDATE so concurrent per-node upserts (and the terminal handler)
// serialize on the row. An unknown release is acked and dropped: a
// stale/duplicate for a pruned release has nothing to project.
func HandleNodeValidationResult(ctx context.Context, d *Deps, in NodeValidationResultInput) error {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	r, err := u.ReleaseRepo().Load(ctx, in.ReleaseID)
	if err != nil {
		return fmt.Errorf("load release: %w", err)
	}
	if r == nil {
		d.Logger.Warn("per-node validation result for unknown release; dropping",
			"release_id", in.ReleaseID, "node_id", in.NodeID)
		return nil
	}

	r.UpsertStageResult(in.Stage, release.NodeValidationResult{
		NodeID:        in.NodeID,
		Status:        in.Status,
		DBTLogURI:     in.DBTLogURI,
		RunResultsURI: in.RunResultsURI,
	})
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
