// executor-controller/service/handlers/compile_requested_handler.go
package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/artifacts"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// CompileRequestedHandler processes compile.requested:v1 events by enqueuing
// exactly one pending compile Deployment for the named service. The compile
// node has no intra-service upstreams, so it always starts pending
// (immediately dispatchable). The handler is pure orchestration — it never
// parses JSON, manages the transaction, or runs dedup (dedup is
// binding-level, keyed per-release on a deterministic outbox_entry_id).
type CompileRequestedHandler struct {
	logger *slog.Logger
}

// NewCompileRequestedHandler constructs the handler.
func NewCompileRequestedHandler(logger *slog.Logger) *CompileRequestedHandler {
	return &CompileRequestedHandler{logger: logger}
}

// Handle enqueues one compile deployment row. msgProcID is the binding-layer
// dedup row's UUID (from message_processing); it is stored as
// message_processing_id on the enqueued row for provenance.
func (h *CompileRequestedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.CompileRequested,
	msgProcID uuid.UUID,
) error {
	now := time.Now()
	cmd := command.ValidationDeployTask{
		ReleaseID:        evt.ReleaseID,
		NodeID:           evt.Service,
		ServiceName:      evt.Service,
		ImageTag:         evt.ImageTag,
		JobName:          BuildValidationJobName(evt.ReleaseID, evt.Service),
		ManifestS3URI:    artifacts.ManifestURI(evt.Bucket, evt.Service, evt.ReleaseID),
		SourceOverlayURI: evt.SourceOverlayURI,
		// Compile tasks have no candidate SQL URI, upstreams, validation op, or prod schema.
	}
	// The parse-export leg needs the candidate schema at parse time; an older
	// compile.requested without candidate_schema disables the leg entirely.
	if evt.CandidateSchema != "" {
		cmd.CandidateSchema = evt.CandidateSchema
		cmd.ParseProdS3URI = artifacts.ParseCacheProdURI(evt.Bucket, evt.Service, evt.ImageTag)
		cmd.ParseCandidateS3URI = artifacts.ParseCacheCandidateURI(evt.Bucket, evt.Service, evt.ReleaseID)
	}
	var procID *uuid.UUID
	if msgProcID != uuid.Nil {
		id := msgProcID
		procID = &id
	}
	if err := u.DeploymentsRepo().Add(ctx, model.NewCompileDeployment(cmd, procID, now)); err != nil {
		return fmt.Errorf("add compile deployment for service %s: %w", evt.Service, err)
	}
	h.logger.Info("enqueued compile deployment",
		"release_id", evt.ReleaseID, "service", evt.Service)
	return nil
}
