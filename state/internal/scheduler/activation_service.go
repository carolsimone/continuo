package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// ScheduleActivationService wraps a ScheduleActivator and owns the transactional
// outbox write that atomically ties scheduler_tracker creation to publishing
// scheduler.started:v1. It implements ScheduleActivator so it can be wired
// wherever a ScheduleActivator is expected.
type ScheduleActivationService struct {
	db            *sqlx.DB
	activator     ScheduleActivator
	catalogRepo   postgres.ScheduleCatalogRepository
	schedulerRepo postgres.SchedulerTrackerRepository
	outboxRepo    postgres.OutboxRepository
	streamName    string
	logger        *slog.Logger
}

// NewScheduleActivationService creates a new ScheduleActivationService.
func NewScheduleActivationService(
	db *sqlx.DB,
	activator ScheduleActivator,
	catalogRepo postgres.ScheduleCatalogRepository,
	schedulerRepo postgres.SchedulerTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	streamName string,
	logger *slog.Logger,
) *ScheduleActivationService {
	return &ScheduleActivationService{
		db:            db,
		activator:     activator,
		catalogRepo:   catalogRepo,
		schedulerRepo: schedulerRepo,
		outboxRepo:    outboxRepo,
		streamName:    streamName,
		logger:        logger,
	}
}

// ActivateSchedule checks if a schedule is already active and, if not, atomically
// creates a scheduler_tracker row and an outbox entry within a single transaction.
// The background OutboxProcessor picks up the entry and publishes to Redis.
func (s *ScheduleActivationService) ActivateSchedule(ctx context.Context, scheduleName string) (uuid.UUID, error) {
	s.logger.Info("Attempting to activate schedule", "schedule_name", scheduleName)

	tracker, err := s.activator.PrepareActivation(ctx, scheduleName)
	if err != nil {
		return uuid.Nil, err
	}
	if tracker == nil {
		s.logger.Info("Schedule activation skipped - active run exists", "schedule_name", scheduleName)
		return uuid.Nil, nil
	}

	manifestVersions, err := s.catalogRepo.GetManifestVersions(ctx, scheduleName)
	if err != nil {
		s.logger.Warn("Could not read manifest_versions from catalog, proceeding with empty",
			"schedule_name", scheduleName, "error", err)
		manifestVersions = map[string]string{}
	}
	versionsJSON, err := json.Marshal(manifestVersions)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to marshal manifest_versions: %w", err)
	}
	tracker.ManifestVersions = manifestVersions
	tracker.ManifestVersionsRaw = versionsJSON

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := s.schedulerRepo.CreateTx(ctx, tx, tracker); err != nil {
		return uuid.Nil, fmt.Errorf("failed to create scheduler_tracker: %w", err)
	}

	payload, err := json.Marshal(map[string]interface{}{
		"runner_id":         tracker.ScheduleID.String(),
		"schedule_name":     tracker.ScheduleName,
		"manifest_versions": manifestVersions,
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	if err := s.outboxRepo.Create(ctx, tx, &postgres.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   tracker.ScheduleID,
		EventType:     "scheduler_started",
		Payload:       payload,
		StreamName:    s.streamName,
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     time.Now(),
	}); err != nil {
		return uuid.Nil, fmt.Errorf("failed to write outbox entry: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return uuid.Nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.logger.Info("Schedule activated successfully",
		"schedule_id", tracker.ScheduleID,
		"schedule_name", scheduleName,
	)
	return tracker.ScheduleID, nil
}

// PrepareActivation delegates to the wrapped activator, satisfying ScheduleActivator.
func (s *ScheduleActivationService) PrepareActivation(ctx context.Context, scheduleName string) (*model.SchedulerTracker, error) {
	return s.activator.PrepareActivation(ctx, scheduleName)
}
