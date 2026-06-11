package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/policy"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// triggerDerivedConfig captures everything that distinguishes a rerun trigger
// from a rebase trigger. The use-case body (eligibility → policy → atomic
// SaveRun+outbox) is identical; only the source-eligibility predicate, the run
// Kind, and the log label differ, so they are supplied per kind.
type triggerDerivedConfig struct {
	// kind is the derived-run kind minted by NewDerivedRun (KindRerun |
	// KindRebase).
	kind run.Kind
	// eligible runs the source aggregate's kind-specific eligibility check
	// (CanBeRerunSource / CanBeRebaseSource); it returns the kind-specific
	// "nothing to do" sentinel so the gRPC adapter can distinguish the paths.
	eligible func(ctx context.Context, src *run.Run, tasks run.TaskCollection) error
	// logLabel is the human label used in the success log line.
	logLabel string
}

// TriggerDerivedRunHandler is the use case behind both the TriggerRerun and
// TriggerRebase gRPC methods. It enforces eligibility on the source Run via the
// aggregate and writes a new derived Run atomically with its outbox event. The
// only per-trigger variation lives in the triggerDerivedConfig it is
// constructed with.
type TriggerDerivedRunHandler struct {
	policy policy.SchedulePolicy
	logger *slog.Logger
	cfg    triggerDerivedConfig
}

// NewTriggerRerunHandler constructs the handler wired for the rerun trigger.
func NewTriggerRerunHandler(logger *slog.Logger) *TriggerDerivedRunHandler {
	return &TriggerDerivedRunHandler{
		logger: logger,
		cfg: triggerDerivedConfig{
			kind: run.KindRerun,
			eligible: func(ctx context.Context, src *run.Run, tasks run.TaskCollection) error {
				return src.CanBeRerunSource(ctx, tasks)
			},
			logLabel: "Rerun triggered",
		},
	}
}

// NewTriggerRebaseHandler constructs the handler wired for the rebase trigger.
func NewTriggerRebaseHandler(logger *slog.Logger) *TriggerDerivedRunHandler {
	return &TriggerDerivedRunHandler{
		logger: logger,
		cfg: triggerDerivedConfig{
			kind: run.KindRebase,
			eligible: func(ctx context.Context, src *run.Run, tasks run.TaskCollection) error {
				return src.CanBeRebaseSource(ctx, tasks)
			},
			logLabel: "Rebase triggered",
		},
	}
}

// Handle synthesises a fresh derived Run from a source. Returns
// (newRunID, scheduleName).
func (h *TriggerDerivedRunHandler) Handle(ctx context.Context, u uow.UnitOfWork, sourceID uuid.UUID) (uuid.UUID, string, error) {
	src, err := u.Run().GetRun(ctx, sourceID)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("get source: %w", err)
	}
	if err := h.cfg.eligible(ctx, src, u.TaskCollection()); err != nil {
		return uuid.Nil, "", err
	}
	if err := h.policy.IsScheduleAvailable(ctx, u.Run(), src.ScheduleName()); err != nil {
		return uuid.Nil, "", err
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

	newRun, evt, err := run.NewDerivedRun(src.ScheduleName(), h.cfg.kind, sourceID, u.Clock().Now())
	if err != nil {
		return uuid.Nil, "", err
	}
	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		return uuid.Nil, "", fmt.Errorf("save: %w", err)
	}
	if err := u.Outbox().Append(ctx, []run.DomainEvent{evt}, uuid.Nil); err != nil {
		return uuid.Nil, "", fmt.Errorf("outbox: %w", err)
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, "", fmt.Errorf("commit: %w", err)
	}
	committed = true

	h.logger.Info(h.cfg.logLabel, "new_run_id", newRun.ScheduleID(), "schedule_name", newRun.ScheduleName(), "source_run_id", sourceID)
	return newRun.ScheduleID(), newRun.ScheduleName(), nil
}
