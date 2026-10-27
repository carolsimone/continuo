package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// ErrSourceRunNotTerminal is returned by TriggerSingleNodeRunHandler when the
// stale-mode source run has not yet reached a terminal state.
var ErrSourceRunNotTerminal = errors.New("source run is not in a terminal state")

// TriggerSingleNodeRunHandler is the use case behind the TriggerSingleNodeRun
// gRPC method. It creates a single-task Run, stores its task and dispatches it
// in one transaction: on the live release, or, in stale mode (MetadataSource =
// snapshot_of_run), on the source run's release, after validating that the
// source run is terminal and carries a task at the requested node.
type TriggerSingleNodeRunHandler struct {
	planner runstart.Planner
	logger  *slog.Logger
}

// NewTriggerSingleNodeRunHandler constructs the handler; planner plans its
// runs.
func NewTriggerSingleNodeRunHandler(planner runstart.Planner, logger *slog.Logger) *TriggerSingleNodeRunHandler {
	return &TriggerSingleNodeRunHandler{planner: planner, logger: logger}
}

// TriggerSingleNodeRunInput holds the parameters for Handle.
type TriggerSingleNodeRunInput struct {
	Target         run.NodeID
	MetadataSource run.MetadataSource
	Operation      model.Operation
	SourceRunID    *uuid.UUID
	Initiator      identity.Identity
}

// Handle synthesises a single-task Run. In stale mode (MetadataSource =
// snapshot_of_run) it requires the source run to be terminal and to carry a
// task at the requested node. A selection the planner refuses (the node is not
// in the release or another service owns it, a test of a node without data
// tests, a full refresh of a type that cannot be rebuilt, a source that names
// no release) is returned as is, and nothing is written.
func (h *TriggerSingleNodeRunHandler) Handle(ctx context.Context, u uow.UnitOfWork, in TriggerSingleNodeRunInput) (uuid.UUID, string, error) {
	var src *run.Run
	if in.MetadataSource == run.MetadataSourceSnapshotOfRun {
		if in.SourceRunID == nil {
			return uuid.Nil, "", fmt.Errorf("source_run_id required when metadata_source=snapshot_of_run")
		}
		var err error
		src, err = u.Run().GetRun(ctx, *in.SourceRunID)
		if err != nil {
			return uuid.Nil, "", fmt.Errorf("get source: %w", err)
		}
		if !src.IsTerminal() {
			return uuid.Nil, "", ErrSourceRunNotTerminal
		}
		ok, err := src.HasTaskAt(ctx, u.TaskCollection(h.planner.TenantID), in.Target)
		if err != nil {
			return uuid.Nil, "", err
		}
		if !ok {
			return uuid.Nil, "", run.ErrTaskNotFound
		}
	}

	if err := u.Begin(ctx); err != nil {
		return uuid.Nil, "", fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()

	sel, err := h.planner.SingleNode(ctx, u, in.Target, in.Operation, src)
	if err != nil {
		return uuid.Nil, "", err
	}
	id := uuid.New()
	scheduleName := "single-node-run-" + strings.ReplaceAll(id.String(), "-", "")[:8]
	newRun, err := run.NewSingleNodeRun(h.planner.TenantID, scheduleName, in.Operation, in.SourceRunID, in.Initiator.UserID, sel.Stamp, u.Clock().Now())
	if err != nil {
		return uuid.Nil, "", err
	}
	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		return uuid.Nil, "", fmt.Errorf("save: %w", err)
	}
	if err := h.planner.Start(ctx, u, newRun, sel, uuid.Nil); err != nil {
		return uuid.Nil, "", err
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, "", fmt.Errorf("commit: %w", err)
	}
	committed = true

	h.logger.Info("Single-node run triggered", "new_run_id", newRun.RunID(), "schedule_name", scheduleName, "target", in.Target,
		"release_id", sel.Stamp.ReleaseID)
	return newRun.RunID(), newRun.ScheduleName(), nil
}
