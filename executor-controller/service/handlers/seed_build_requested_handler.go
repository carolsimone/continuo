// executor-controller/service/handlers/seed_build_requested_handler.go
package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// SeedBuildRequestedHandler processes seed.build.requested:v1 events by
// enqueuing one pending seed-build Deployment per seed in the request. Seeds
// are dbt roots with no intra-service upstreams, so every deployment starts
// pending (immediately dispatchable). The handler is pure orchestration — it
// never parses JSON, manages the transaction, or runs dedup (dedup is
// binding-level, keyed per-release on a deterministic outbox_entry_id).
type SeedBuildRequestedHandler struct {
	logger *slog.Logger
}

// NewSeedBuildRequestedHandler constructs the handler.
func NewSeedBuildRequestedHandler(logger *slog.Logger) *SeedBuildRequestedHandler {
	return &SeedBuildRequestedHandler{logger: logger}
}

// Handle enqueues one seed-build deployment row per seed. msgProcID is the
// binding-layer dedup row's UUID (from message_processing); it is stored as
// message_processing_id on every enqueued row for provenance.
func (h *SeedBuildRequestedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.SeedBuildRequested,
	msgProcID uuid.UUID,
) error {
	now := time.Now()
	for _, s := range evt.Seeds {
		cmd := command.ValidationDeployTask{
			ReleaseID:       evt.ReleaseID,
			NodeID:          s.NodeID,
			ServiceName:     s.ServiceName,
			SchemaName:      s.SchemaName,
			TableName:       s.TableName,
			NodeType:        string(s.NodeType),
			ImageTag:        s.ImageTag,
			JobName:         BuildValidationJobName(evt.ReleaseID, s.NodeID),
			CandidateSchema: evt.CandidateSchema,
			// A shadow release verifying a proposed fix carries the overlay of
			// proposed source files; the seed Job lays it over the checked-in
			// project so `dbt seed` loads the proposed CSV.
			SourceOverlayURI: evt.SourceOverlayURI,
			// Seed-build tasks have no SQL URI, upstreams, validation op, or prod schema.
		}
		var procID *uuid.UUID
		if msgProcID != uuid.Nil {
			id := msgProcID
			procID = &id
		}
		if err := u.DeploymentsRepo().Add(ctx, model.NewSeedBuildDeployment(cmd, procID, now)); err != nil {
			return fmt.Errorf("add seed-build deployment %s: %w", s.NodeID, err)
		}
	}
	h.logger.Info("enqueued seed-build deployments",
		"release_id", evt.ReleaseID, "seed_count", len(evt.Seeds))
	return nil
}
