package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// ErrSourceRunNotTerminal is returned by TriggerSingleNodeRunHandler when the
// stale-mode source run has not yet reached a terminal state.
var ErrSourceRunNotTerminal = errors.New("source run is not in a terminal state")

// TriggerSingleNodeRunHandler is the use case behind the TriggerSingleNodeRun
// gRPC method. It writes a new single-task Run atomically with its outbox
// event. In stale mode (MetadataSource = snapshot_of_run) it validates that
// the source run is terminal and carries a task at the requested node.
type TriggerSingleNodeRunHandler struct {
	logger *slog.Logger
}

// NewTriggerSingleNodeRunHandler constructs the handler.
func NewTriggerSingleNodeRunHandler(logger *slog.Logger) *TriggerSingleNodeRunHandler {
	return &TriggerSingleNodeRunHandler{logger: logger}
}

// TriggerSingleNodeRunInput holds the parameters for Handle.
type TriggerSingleNodeRunInput struct {
	Target         run.NodeID
	MetadataSource run.MetadataSource
	SourceRunID    *uuid.UUID
}

// Handle synthesises a single-task Run. In stale mode (MetadataSource =
// snapshot_of_run) it requires the source run to be terminal and to carry a
// task at the requested node.
func (h *TriggerSingleNodeRunHandler) Handle(ctx context.Context, u uow.UnitOfWork, in TriggerSingleNodeRunInput) (uuid.UUID, string, error) {
	if in.MetadataSource == run.MetadataSourceSnapshotOfRun {
		if in.SourceRunID == nil {
			return uuid.Nil, "", fmt.Errorf("source_run_id required when metadata_source=snapshot_of_run")
		}
		src, err := u.Run().GetRun(ctx, *in.SourceRunID)
		if err != nil {
			return uuid.Nil, "", fmt.Errorf("get source: %w", err)
		}
		if !src.IsTerminal() {
			return uuid.Nil, "", ErrSourceRunNotTerminal
		}
		ok, err := src.HasTaskAt(ctx, u.TaskCollection(), in.Target)
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

	id := uuid.New()
	scheduleName := "single-node-run-" + strings.ReplaceAll(id.String(), "-", "")[:8]
	newRun, evt, err := run.NewSingleNodeRun(scheduleName, in.Target, in.MetadataSource, in.SourceRunID, u.Clock().Now())
	if err != nil {
		return uuid.Nil, "", err
	}
	if err := u.Run().SaveRun(ctx, u.Tx(), newRun); err != nil {
		return uuid.Nil, "", fmt.Errorf("save: %w", err)
	}
	if err := u.Outbox().Append(ctx, u.Tx(), []run.DomainEvent{evt}, uuid.Nil); err != nil {
		return uuid.Nil, "", fmt.Errorf("outbox: %w", err)
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, "", fmt.Errorf("commit: %w", err)
	}
	committed = true

	h.logger.Info("Single-node run triggered", "new_run_id", newRun.ScheduleID(), "schedule_name", scheduleName, "target", in.Target)
	return newRun.ScheduleID(), newRun.ScheduleName(), nil
}
