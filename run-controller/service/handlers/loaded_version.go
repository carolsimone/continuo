package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// recordsBuiltVersions reports whether a succeeded task of r loaded its node's
// version of r's release into the warehouse: run-controller chose r's tasks from that
// release, and r's operation writes the table. A test only reads it.
func recordsBuiltVersions(r *run.Run) bool {
	return r.PlannedAt() != nil && r.ReleaseID() != "" && r.Operation() != model.OperationTest
}

// recordLoadedVersion records the version a succeeded task of a run that names
// a release loaded into the warehouse: its node's content hash in that
// release, the image the task ran, and the run and task. A task whose row is
// gone, or whose node the release lacks or holds without a content hash,
// records nothing.
func recordLoadedVersion(ctx context.Context, u uow.UnitOfWork, tenantID string, logger *slog.Logger, r *run.Run, taskID uuid.UUID) error {
	task, err := u.TaskCollection(tenantID).GetByID(ctx, taskID)
	if errors.Is(err, run.ErrTaskNotFound) {
		logger.Warn("task status: task row not found; no loaded version recorded",
			"task_id", taskID, "run_id", r.RunID(), "release_id", r.ReleaseID())
		return nil
	}
	if err != nil {
		return fmt.Errorf("read task %s: %w", taskID, err)
	}
	key := topology.KeyOf(task.SchemaName, task.TableName)
	hash, found, err := u.Topology().NodeContentHash(ctx, tenantID, r.ReleaseID(), key)
	if err != nil {
		return fmt.Errorf("read the content hash of %s in release %s: %w", key, r.ReleaseID(), err)
	}
	if !found || hash == "" {
		logger.Info("task status: node has no content hash in the run's release; no loaded version recorded",
			"task_id", taskID, "node_key", key, "release_id", r.ReleaseID(), "in_release", found)
		return nil
	}
	if err := u.Materializations().RecordBuilt(ctx, tenantID, seeds.Built{
		Key:         key,
		ContentHash: hash,
		ImageTag:    task.ImageTag,
		ReleaseID:   r.ReleaseID(),
		RunID:       r.RunID(),
		TaskID:      taskID,
		At:          u.Clock().Now(),
		Source:      seeds.SourceBuild,
	}); err != nil {
		return fmt.Errorf("record the loaded version of %s: %w", key, err)
	}
	logger.Info("task status: loaded version recorded",
		"task_id", taskID, "node_key", key, "release_id", r.ReleaseID(), "content_hash", hash)
	return nil
}
