package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/codebundle"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// unmatchedSampleSize bounds how many node ids a warning names, so an
// estate-wide mismatch logs a usable sample instead of thousands of ids.
const unmatchedSampleSize = 5

// ReleasePromotedVersionsHandler consumes release.promoted:v1 on its own
// consumer group and records the release's code versions in the graph.
//
// It is deliberately separate from the topology-swap handler: promotion must
// never gain a dependency on object storage being available, and nothing on the
// run path reads versions, so this group can trail the swap and retry freely.
// Which nodes actually get a version is decided against the graph, not against
// the event's `changed` flags — that is what lets any later release converge a
// graph that missed a write.
type ReleasePromotedVersionsHandler struct {
	uow      uow.UnitOfWork
	bundles  ports.CodeBundleReader
	versions repository.CodeVersionRepository
	logger   *slog.Logger
}

// NewReleasePromotedVersionsHandler creates a ReleasePromotedVersionsHandler.
func NewReleasePromotedVersionsHandler(
	u uow.UnitOfWork,
	bundles ports.CodeBundleReader,
	versions repository.CodeVersionRepository,
	logger *slog.Logger,
) *ReleasePromotedVersionsHandler {
	return &ReleasePromotedVersionsHandler{uow: u, bundles: bundles, versions: versions, logger: logger}
}

// Handle processes one release.promoted:v1 message on the versions group.
func (h *ReleasePromotedVersionsHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in domainModel.PromoteReleaseInput,
) error {
	h.logger.Info("Processing release.promoted:v1 (versions path)",
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

	// Dedup scope rule (shared): three consumer groups read release.promoted:v1,
	// so each scopes its dedup by its own CONSUMER-GROUP name and their
	// message_processing rows stay independent under the (outbox_entry_id,
	// stream_name) unique index.
	msgProcessingID, shouldSkip, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, streams.OrchestratorReleasePromotedVersions, payload, outboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	if in.CodeBundleURI == "" {
		// A release promoted before topology-controller began writing bundles.
		// No retry can produce one, so record the message as handled.
		h.logger.Warn("release.promoted carries no code_bundle_uri — no versions to ingest",
			"release_id", in.ReleaseID)
		return h.complete(ctx, msgProcessingID)
	}

	bundle, err := h.bundles.Fetch(ctx, in.CodeBundleURI)
	if err != nil {
		if errors.Is(err, ports.ErrBundleMalformed) || errors.Is(err, ports.ErrBundleTooLarge) {
			// Re-reading the same bytes cannot fix them: acknowledge, drop, and
			// make the failure loud.
			h.logger.Error("code bundle is unreadable — dropping the message",
				"release_id", in.ReleaseID, "uri", in.CodeBundleURI, "error", err)
			return fmt.Errorf("%w: code bundle %s: %v", pkgevents.ErrPermanent, in.CodeBundleURI, err)
		}
		// Absent or unreachable: the object may still land, so retry.
		return fmt.Errorf("fetch code bundle %s: %w", in.CodeBundleURI, err)
	}

	if err := bundleMatchesEvent(bundle, in); err != nil {
		// The URI resolved to a document that is not this release's. Writing it
		// would stamp this release's provenance onto another release's code and
		// could point :CURRENT at a hash that disagrees with :Table.content_hash.
		h.logger.Error("code bundle does not match the promoted release — dropping the message",
			"release_id", in.ReleaseID, "uri", in.CodeBundleURI, "error", err)
		return fmt.Errorf("%w: code bundle %s: %v", pkgevents.ErrPermanent, in.CodeBundleURI, err)
	}

	res, err := h.versions.WriteVersions(ctx, planVersionWrite(in, bundle, h.logger))
	if err != nil {
		return fmt.Errorf("write code versions for release %s: %w", in.ReleaseID, err)
	}

	if len(res.UnmatchedNodeIDs) > 0 {
		sample := res.UnmatchedNodeIDs
		if len(sample) > unmatchedSampleSize {
			sample = sample[:unmatchedSampleSize]
		}
		switch {
		case res.GraphAhead:
			// The topology has already applied a newer promotion, so these nodes
			// were retired and deleted in between. Retrying would only burn the
			// delivery budget and end with the message dropped, losing their
			// history; the repository has recorded it unattached instead.
			h.logger.Warn("topology has moved past this release — recorded its history unattached",
				"release_id", in.ReleaseID,
				"graph_release_id", res.GraphReleaseID,
				"unattached_count", len(res.UnmatchedNodeIDs),
				"sample", sample)
		case res.GraphReleaseID != in.ReleaseID:
			// The topology-swap group reads the same message and has not applied
			// this release yet, so these nodes have no :Table to attach a version
			// to. Retrying is how this group trails the swap.
			return fmt.Errorf("topology swap for release %s has not landed (graph is at %q); "+
				"%d bundle nodes have no :Table (e.g. %v)",
				in.ReleaseID, res.GraphReleaseID, len(res.UnmatchedNodeIDs), sample)
		default:
			h.logger.Warn("code bundle names nodes absent from the promoted topology",
				"release_id", in.ReleaseID,
				"unmatched_count", len(res.UnmatchedNodeIDs),
				"sample", sample)
		}
	}

	h.logger.Info("Code version ingestion finished",
		"release_id", in.ReleaseID,
		"bundle_nodes", len(bundle.Nodes),
		"node_versions_created", res.NodeVersionsCreated,
		"unit_versions_created", res.UnitVersionsCreated,
		"current_pointers_moved", res.CurrentPointersMoved,
		"rejections_resolved", res.RejectionsResolved,
	)
	return h.complete(ctx, msgProcessingID)
}

// bundleMatchesEvent checks that the fetched document really describes the
// release the event promoted. The bundle and the promoted topology come from the
// same parse, so they must agree; a disagreement means the URI resolved to
// another release's object and the code must not be recorded under this
// release's provenance.
func bundleMatchesEvent(b codebundle.Bundle, in domainModel.PromoteReleaseInput) error {
	if b.ReleaseID != in.ReleaseID {
		return fmt.Errorf("bundle belongs to release %q, event promoted %q", b.ReleaseID, in.ReleaseID)
	}
	for _, n := range in.Topology {
		bn, ok := b.Nodes[n.UniqueID]
		if !ok || n.ContentHash == "" {
			// A node absent from the bundle is reported by the write path as
			// unmatched; an event node with no hash predates the field. Neither is
			// evidence that the document is the wrong one.
			continue
		}
		if bn.ContentHash != n.ContentHash {
			return fmt.Errorf("node %s: bundle content_hash %q disagrees with the promoted topology's %q",
				n.UniqueID, bn.ContentHash, n.ContentHash)
		}
	}
	return nil
}

// complete marks the dedup row processed and commits.
func (h *ReleasePromotedVersionsHandler) complete(ctx context.Context, msgProcessingID uuid.UUID) error {
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
