// executor-controller/service/handlers/validation_requested_handler.go
package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// ValidationRequestedHandler processes validation.requested:v1 events by
// enqueuing one pending validation Deployment per node in the request. A single
// inbound message carries every node for a release; the handler is pure
// orchestration — it never parses JSON, manages the transaction, or runs dedup
// (dedup is binding-level, keyed per-release on a deterministic outbox_entry_id).
type ValidationRequestedHandler struct {
	logger *slog.Logger
}

// NewValidationRequestedHandler constructs the handler.
func NewValidationRequestedHandler(logger *slog.Logger) *ValidationRequestedHandler {
	return &ValidationRequestedHandler{logger: logger}
}

// Handle enqueues one validation deployment row per node. msgProcID is the
// binding-layer dedup row's UUID (from message_processing); it is stored as
// message_processing_id on every enqueued row for provenance.
func (h *ValidationRequestedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.ValidationRequested,
	msgProcID uuid.UUID,
) error {
	now := time.Now()
	for _, n := range evt.Nodes {
		jobName := BuildValidationJobName(evt.ReleaseID, n.NodeID)
		cmd := command.ValidationDeployTask{
			ReleaseID:       evt.ReleaseID,
			NodeID:          n.NodeID,
			ServiceName:     n.ServiceName,
			SchemaName:      n.SchemaName,
			TableName:       n.TableName,
			NodeType:        string(n.NodeType),
			ImageTag:        n.ImageTag,
			JobName:         jobName,
			CandidateSchema: evt.CandidateSchema,
			DeferStateURI:   evt.DeferStateURI,
		}
		if err := createValidationDeployment(ctx, u, cmd, msgProcID, now); err != nil {
			return fmt.Errorf("enqueue validation node %s: %w", n.NodeID, err)
		}
	}
	h.logger.Info("enqueued validation deployments",
		"release_id", evt.ReleaseID, "node_count", len(evt.Nodes))
	return nil
}

// createValidationDeployment writes one pending validation Deployment aggregate
// to the executor_deployments command queue through the UoW deployments repo.
// msgProcID is stored for provenance; pass uuid.Nil when no inbound trigger
// applies.
func createValidationDeployment(
	ctx context.Context,
	u uow.UnitOfWork,
	cmd command.ValidationDeployTask,
	msgProcID uuid.UUID,
	now time.Time,
) error {
	var procID *uuid.UUID
	if msgProcID != uuid.Nil {
		id := msgProcID
		procID = &id
	}
	if err := u.DeploymentsRepo().Add(ctx, model.NewValidationDeployment(cmd, procID, now)); err != nil {
		return fmt.Errorf("add validation deployment: %w", err)
	}
	return nil
}

// nonK8sChar matches any character that is not a lowercase RFC1123 label
// character. dbt unique_ids and release IDs are lowercased before this runs,
// so the class only needs lowercase alphanumerics and the hyphen.
var nonK8sChar = regexp.MustCompile(`[^a-z0-9-]`)

// jobNameMaxLen is the RFC1123 label / K8s Job name limit.
const jobNameMaxLen = 63

// BuildValidationJobName derives a deterministic, RFC1123-valid (<=63 char) K8s
// Job name from (release_id, node_id):
//
//	validate-<release_sanitized>-<node_sanitized>-<8hex>
//
// release_id and node_id (a dbt unique_id such as "model.shop.orders") are
// lowercased, then every non [a-z0-9-] character is replaced with '-'. An
// 8-hex-char tail of sha256(release_id\x00node_id) keeps the name unique and
// stable. The 8-hex tail (plus its leading '-') is reserved before truncation,
// so the human-readable segment is shortened rather than the hash — two nodes
// whose names diverge only past the limit still produce distinct Job names. The
// result is trimmed of leading/trailing '-' so it is always a valid RFC1123
// label; the hash tail is pure hex, so it never introduces a trailing '-', and
// the "validate" literal anchors a valid label even when both inputs sanitize
// away to nothing.
func BuildValidationJobName(releaseID, nodeID string) string {
	sanitize := func(s string) string {
		return nonK8sChar.ReplaceAllString(strings.ToLower(s), "-")
	}
	hash := sha256.Sum256([]byte(releaseID + "\x00" + nodeID))
	tail := hex.EncodeToString(hash[:4]) // 8 hex chars

	prefix := "validate-" + sanitize(releaseID) + "-" + sanitize(nodeID)
	// Reserve room for "-" + tail so truncation never eats the hash.
	if budget := jobNameMaxLen - len(tail) - 1; len(prefix) > budget {
		prefix = prefix[:budget]
	}
	name := strings.Trim(prefix, "-") + "-" + tail
	return strings.Trim(name, "-")
}
