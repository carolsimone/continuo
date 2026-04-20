package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// schedulesLoadedPayload is the JSON payload of a schedules.loaded:v1 message.
type schedulesLoadedPayload struct {
	EventID          string            `json:"event_id"`
	ScheduleNames    []string          `json:"schedule_names"`
	ManifestVersions map[string]string `json:"manifest_versions"`
}

// ScheduleCatalogHandler reconciles schedule_catalog from a single schedules.loaded:v1 payload.
// Returns (true, nil) if the message was processed (or was a known duplicate).
// Returns (false, err) if a transient error prevents processing — caller should NOT ACK.
type ScheduleCatalogHandler struct {
	catalogRepo postgres.ScheduleCatalogRepository
	db          *sqlx.DB
	logger      *slog.Logger
}

// NewScheduleCatalogHandler creates a ScheduleCatalogHandler.
func NewScheduleCatalogHandler(
	catalogRepo postgres.ScheduleCatalogRepository,
	db *sqlx.DB,
	logger *slog.Logger,
) *ScheduleCatalogHandler {
	return &ScheduleCatalogHandler{catalogRepo: catalogRepo, db: db, logger: logger}
}

// Handle processes a raw payload string from a schedules.loaded:v1 Redis message.
// Returns (shouldACK bool, err error).
// shouldACK=true means ACK regardless of err (permanent/unprocessable message).
// shouldACK=false + err means transient failure — do NOT ACK.
func (h *ScheduleCatalogHandler) Handle(ctx context.Context, payloadStr string) (shouldACK bool, err error) {
	if payloadStr == "" {
		h.logger.Error("Missing payload field — discarding")
		return true, nil // permanent data error; ACK to drain
	}

	var p schedulesLoadedPayload
	if err := json.Unmarshal([]byte(payloadStr), &p); err != nil {
		h.logger.Error("Invalid JSON payload — discarding", "error", err)
		return true, nil
	}

	if p.EventID == "" {
		h.logger.Error("Missing event_id — discarding")
		return true, nil
	}

	eventID, err := uuid.Parse(p.EventID)
	if err != nil {
		h.logger.Error("Invalid event_id UUID — discarding", "error", err)
		return true, nil
	}

	// Deduplication check — transient DB error: don't ACK
	var already bool
	if err := h.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)`,
		eventID,
	).Scan(&already); err != nil {
		return false, fmt.Errorf("dedup check: %w", err)
	}
	if already {
		h.logger.Info("Duplicate event — skipping", "event_id", eventID)
		return true, nil
	}

	// Reconcile catalog — transient errors: don't ACK
	if err := h.catalogRepo.UpsertAll(ctx, p.ScheduleNames, p.ManifestVersions); err != nil {
		return false, fmt.Errorf("upsert schedule names: %w", err)
	}
	if err := h.catalogRepo.SoftDeleteAbsent(ctx, p.ScheduleNames); err != nil {
		return false, fmt.Errorf("soft-delete absent schedules: %w", err)
	}

	// Record processed event — transient DB error: don't ACK so we can retry
	if _, err := h.db.ExecContext(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	); err != nil {
		return false, fmt.Errorf("record processed event: %w", err)
	}

	h.logger.Info("Schedule catalog reconciled",
		"event_id", eventID,
		"schedule_count", len(p.ScheduleNames),
	)
	return true, nil
}
