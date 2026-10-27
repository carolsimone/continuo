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
// in the caller's transaction: on the live release, or, in stale mode
// (MetadataSource = snapshot_of_run), on the source run's release, after
// validating that the source run is terminal and carries a task at the
// requested node.
type TriggerSingleNodeRunHandler struct {
	planner runstart.Planner
	logger  *slog.Logger
}

// NewTriggerSingleNodeRunHandler constructs the handler; planner plans its
// runs.
func NewTriggerSingleNodeRunHandler(planner runstart.Planner, logger *slog.Logger) *TriggerSingleNodeRunHandler {
	return &TriggerSingleNodeRunHandler{planner: planner, logger: logger}
}

// TriggerSingleNodeRunInput holds the parameters for HandleInTransaction.
// TenantID is the tenant the run is created for; its source run, in stale
// mode, is one of that tenant's runs.
type TriggerSingleNodeRunInput struct {
	TenantID       string
	Target         run.NodeID
	MetadataSource run.MetadataSource
	Operation      model.Operation
	SourceRunID    *uuid.UUID
	Initiator      identity.Identity
}

// HandleInTransaction synthesises the single-task Run runID inside u's open
// transaction, which the caller commits or rolls back, and returns the run's
// schedule name. In stale mode (MetadataSource = snapshot_of_run) it requires
// the source run to be terminal and to carry a task at the requested node. A
// selection the planner refuses (the node is not in the release or another
// service owns it, a test of a node without data tests, a full refresh of a
// type that cannot be rebuilt, a source that names no release) is returned as
// is, and the caller rolls back.
func (h *TriggerSingleNodeRunHandler) HandleInTransaction(ctx context.Context, u uow.UnitOfWork, runID uuid.UUID, in TriggerSingleNodeRunInput) (string, error) {
	src, err := h.source(ctx, u, in)
	if err != nil {
		return "", err
	}
	return h.write(ctx, u, runID, in, src)
}

// source returns the source run of a stale-mode request, after checking it
// is terminal and carries a task at the requested node; a request on the live
// release has none.
func (h *TriggerSingleNodeRunHandler) source(ctx context.Context, u uow.UnitOfWork, in TriggerSingleNodeRunInput) (*run.Run, error) {
	if in.MetadataSource != run.MetadataSourceSnapshotOfRun {
		return nil, nil
	}
	if in.SourceRunID == nil {
		return nil, fmt.Errorf("source_run_id required when metadata_source=snapshot_of_run")
	}
	src, err := u.Run().GetRun(ctx, in.TenantID, *in.SourceRunID)
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}
	if !src.IsTerminal() {
		return nil, ErrSourceRunNotTerminal
	}
	ok, err := src.HasTaskAt(ctx, u.TaskCollection(src.TenantID()), in.Target)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, run.ErrTaskNotFound
	}
	return src, nil
}

// write creates the single-task Run runID, named single-node-run- and the
// first eight hex digits of its id, stores its task and dispatches it, inside
// u's open transaction. It returns the run's schedule name.
func (h *TriggerSingleNodeRunHandler) write(ctx context.Context, u uow.UnitOfWork, runID uuid.UUID, in TriggerSingleNodeRunInput, src *run.Run) (string, error) {
	sel, err := h.planner.SingleNode(ctx, u, in.TenantID, in.Target, in.Operation, src)
	if err != nil {
		return "", err
	}
	scheduleName := "single-node-run-" + strings.ReplaceAll(runID.String(), "-", "")[:8]
	newRun, err := run.NewSingleNodeRun(in.TenantID, runID, scheduleName, in.Operation, in.SourceRunID, in.Initiator.UserID, sel.Stamp, u.Clock().Now())
	if err != nil {
		return "", err
	}
	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		return "", fmt.Errorf("save: %w", err)
	}
	if err := h.planner.Start(ctx, u, newRun, sel, uuid.Nil); err != nil {
		return "", err
	}
	u.AfterCommit(func() {
		h.logger.Info("Single-node run triggered", "tenant_id", in.TenantID, "new_run_id", runID, "schedule_name", scheduleName, "target", in.Target,
			"release_id", sel.Stamp.ReleaseID)
	})
	return scheduleName, nil
}
