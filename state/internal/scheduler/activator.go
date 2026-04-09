package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
)

// ScheduleActivator defines the interface for activating schedules.
type ScheduleActivator interface {
	ActivateSchedule(ctx context.Context, scheduleName string) (uuid.UUID, error)
	PrepareActivation(ctx context.Context, scheduleName string) (*model.SchedulerTracker, error)
}

// scheduleActivator implements ScheduleActivator.
// ActivateSchedule is used in tests and standalone contexts.
// Production code uses ScheduleActivationService, which calls PrepareActivation directly.
type scheduleActivator struct {
	repo   postgres.SchedulerTrackerRepository
	logger *slog.Logger
}

// NewScheduleActivator creates a new scheduleActivator.
func NewScheduleActivator(
	repo postgres.SchedulerTrackerRepository,
	logger *slog.Logger,
) ScheduleActivator {
	return &scheduleActivator{
		repo:   repo,
		logger: logger,
	}
}

// PrepareActivation checks eligibility and returns a ready-to-insert SchedulerTracker,
// or nil if a PENDING or RUNNING run already exists for this schedule name.
func (a *scheduleActivator) PrepareActivation(ctx context.Context, scheduleName string) (*model.SchedulerTracker, error) {
	hasActive, err := a.repo.HasActiveSchedule(ctx, scheduleName)
	if err != nil {
		return nil, fmt.Errorf("failed to check for active schedule: %w", err)
	}
	if hasActive {
		a.logger.Info("Schedule activation skipped - active run exists",
			"schedule_name", scheduleName,
			"reason", "status is RUNNING or PENDING",
		)
		return nil, nil
	}
	return &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         scheduleName,
		Status:               model.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}, nil
}

// ActivateSchedule checks eligibility, creates the tracker row, and returns the
// new schedule ID. It does not publish to Redis — use ScheduleActivationService
// for the production outbox path.
func (a *scheduleActivator) ActivateSchedule(ctx context.Context, scheduleName string) (uuid.UUID, error) {
	a.logger.Info("Attempting to activate schedule", "schedule_name", scheduleName)

	tracker, err := a.PrepareActivation(ctx, scheduleName)
	if err != nil {
		return uuid.Nil, err
	}
	if tracker == nil {
		return uuid.Nil, nil
	}

	if err := a.repo.Create(ctx, tracker); err != nil {
		a.logger.Error("Failed to create scheduler_tracker",
			"schedule_id", tracker.ScheduleID,
			"schedule_name", scheduleName,
			"error", err,
		)
		return uuid.Nil, fmt.Errorf("failed to create scheduler_tracker: %w", err)
	}

	a.logger.Info("Created scheduler_tracker record",
		"schedule_id", tracker.ScheduleID,
		"schedule_name", scheduleName,
		"status", tracker.Status,
	)
	return tracker.ScheduleID, nil
}
