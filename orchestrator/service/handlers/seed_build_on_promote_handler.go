package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// seedBuildOnPromoteNamespace seeds deterministic TaskID and ScheduleID UUIDs
// for query.model:v1 entries emitted on the promote-seed path.
//
// IMMUTABLE: changing this value re-keys every TaskID/ScheduleID derived from a
// node's unique_id and breaks executor-side dedup for in-flight redeliveries.
var seedBuildOnPromoteNamespace = uuid.MustParse("3e7b1a2f-8c4d-4e9a-b5f0-1d2c3a4e5f60")

// promoteSeedScheduleName is the synthetic schedule name stamped on every
// promote-seed query.model emission. The executor receives it as ScheduleName
// and passes it through to the k8s Job; it does not need to match a real
// schedule in the Neo4j topology.
const promoteSeedScheduleName = "promote-seed"

// SeedBuildOnPromoteHandler consumes release.promoted:v1 events (independent
// consumer group: orchestrator-release-promoted-seed-build) and, for each node
// with Changed == true && NodeType == "dbt-seed", emits a query.model:v1
// outbox row so the executor builds the seed into the prod schema.
//
// This handler is intentionally light: no Neo4j interaction, no topology state
// update. It exists solely to trigger prod seed builds on promotion, isolated
// from the topology-swap handler so failures here do not block the promote path.
type SeedBuildOnPromoteHandler struct {
	uow    uow.UnitOfWork
	logger *slog.Logger
}

// NewSeedBuildOnPromoteHandler creates a SeedBuildOnPromoteHandler.
func NewSeedBuildOnPromoteHandler(u uow.UnitOfWork, logger *slog.Logger) *SeedBuildOnPromoteHandler {
	return &SeedBuildOnPromoteHandler{uow: u, logger: logger}
}

// Handle processes a release.promoted:v1 message and emits one query.model:v1
// outbox row per changed dbt-seed node. Unchanged seeds and non-seed nodes are
// skipped. The handler is idempotent via message-processing dedup keyed on
// (messageID, release.promoted:v1 / seed-build consumer group namespace).
func (h *SeedBuildOnPromoteHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in domainModel.PromoteReleaseInput,
) error {
	h.logger.Info("Processing release.promoted:v1 (seed-build path)",
		"message_id", messageID,
		"release_id", in.ReleaseID,
		"node_count", len(in.Topology),
	)

	payload, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	// Dedup keyed on the seed-build consumer's own stream constant so that
	// this consumer's dedup rows are scoped independently from the topology-swap
	// consumer's rows on the same release.promoted:v1 stream.
	msgProcessingID, shouldSkip, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, streams.OrchestratorReleasePromotedSeedBuild, payload, outboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Synthetic ScheduleID: deterministic per release_id so that idempotent
	// redeliveries produce the same ScheduleID and the executor's dedup layer
	// catches duplicate submissions for the same promote event.
	scheduleID := uuid.NewSHA1(seedBuildOnPromoteNamespace, []byte("schedule:"+in.ReleaseID))

	dispatchedCount := 0
	for _, node := range in.Topology {
		if !node.Changed || node.NodeType != "dbt-seed" {
			continue
		}

		// Deterministic TaskID per (release_id, unique_id) so retries carry the
		// same task identity. uuid.NewSHA1 is collision-resistant for this scope.
		taskID := uuid.NewSHA1(seedBuildOnPromoteNamespace, []byte("task:"+in.ReleaseID+":"+node.UniqueID))

		jobName, err := pkgDomain.ComputeJobName(node.ServiceName, node.SchemaName, node.TableName, scheduleID.String())
		if err != nil {
			return fmt.Errorf("compute job_name for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		nodeEvt := domain.NodeReadyForExecution{
			ScheduleID:   scheduleID.String(),
			ScheduleName: promoteSeedScheduleName,
			ServiceName:  node.ServiceName,
			SchemaName:   node.SchemaName,
			TableName:    node.TableName,
			TaskID:       taskID.String(),
			JobName:      jobName,
			NodeType:     node.NodeType,
			ImageTag:     node.ImageTag,
		}

		evtPayload, err := json.Marshal(nodeEvt)
		if err != nil {
			return fmt.Errorf("marshal NodeReadyForExecution for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		outboxEntry := &pkgoutbox.Entry{
			ID:                  uuid.New(),
			MessageProcessingID: &msgProcessingID,
			AggregateType:       "orchestrator",
			AggregateID:         scheduleID,
			EventType:           "node_ready_for_execution",
			Payload:             evtPayload,
			StreamName:          streams.QueryModelV1,
			Status:              "pending",
			MaxRetries:          pkgoutbox.DefaultMaxRetries,
		}
		if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
			return fmt.Errorf("write query.model outbox entry for %s.%s: %w", node.SchemaName, node.TableName, err)
		}
		dispatchedCount++

		h.logger.Debug("Emitted promote-seed query.model entry",
			"unique_id", node.UniqueID,
			"table", node.TableName,
			"task_id", taskID,
		)
	}

	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}

	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	h.logger.Info("Promote-seed dispatch finished",
		"release_id", in.ReleaseID,
		"dispatched_count", dispatchedCount,
	)

	return nil
}
