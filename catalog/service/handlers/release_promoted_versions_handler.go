package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/codebundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
)

// ReleasePromotedVersionsHandler consumes release.promoted:v2 on its own
// consumer group and records the release's code versions in the tenant's
// graph.
//
// It is separate from the topology-swap handler, so promotion never waits on
// the code bundle, and it does not wait for the swap either: a node the swap
// has not written yet is created inactive from the release's artifact, and
// the swap makes it live. Which nodes get a version is decided against the
// graph, not against the event's changed_node_ids, which is what lets any
// later release converge a graph that missed a write; changed_node_ids only
// qualifies provenance.
type ReleasePromotedVersionsHandler struct {
	uow       uow.UnitOfWork
	bundles   ports.CodeBundleReader
	artifacts ports.TopologyArtifactReader
	logger    *slog.Logger
}

// NewReleasePromotedVersionsHandler creates a ReleasePromotedVersionsHandler.
func NewReleasePromotedVersionsHandler(
	u uow.UnitOfWork,
	bundles ports.CodeBundleReader,
	artifacts ports.TopologyArtifactReader,
	logger *slog.Logger,
) *ReleasePromotedVersionsHandler {
	return &ReleasePromotedVersionsHandler{uow: u, bundles: bundles, artifacts: artifacts, logger: logger}
}

// Handle processes one promotion of in.TenantID on the versions group. Steps:
//  1. Look the group's marker up, by messageID or by outboxEntryID, which
//     catches a re-XADD of the same upstream outbox row under a fresh Redis
//     message ID. A processed message is acknowledged without reading object
//     storage.
//  2. Read the code bundle and the topology artifact, outside any
//     transaction. A malformed or oversized bundle, a corrupt artifact, or a
//     bundle or artifact of another release is permanent; a missing object or
//     an outage is retried. A promotion with no bundle records only its
//     marker.
//  3. Write the versions in batches of VersionsBatchSize nodes, one write
//     transaction each; the marker commits with the last batch. A failure
//     ends the message with no marker, and its redelivery writes every batch
//     again, each MERGE finding what an earlier delivery wrote.
func (h *ReleasePromotedVersionsHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in domainModel.PromoteReleaseInput,
) error {
	h.logger.Info("Processing release.promoted:v2 (versions group)",
		"message_id", messageID,
		"tenant_id", in.TenantID,
		"release_id", in.ReleaseID,
		"promotion_seq", in.PromotionSeq,
	)
	marker := uow.Marker{
		TenantID:      in.TenantID,
		Group:         streams.CatalogReleaseVersions,
		MessageID:     messageID,
		OutboxEntryID: outboxEntryID,
	}

	processed, err := h.uow.Processed(ctx, marker)
	if err != nil {
		return fmt.Errorf("look up the processed-message marker: %w", err)
	}
	if processed {
		h.logger.Info("release.promoted already processed by the versions group; acknowledged",
			"message_id", messageID, "tenant_id", in.TenantID, "release_id", in.ReleaseID)
		return nil
	}

	if in.CodeBundleURI == "" {
		// An announcement carries no bundle, and no retry can produce one, so
		// record the message as handled.
		h.logger.Warn("release.promoted carries no code_bundle_uri — no versions to ingest",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "promotion_seq", in.PromotionSeq)
		return h.recordMarker(ctx, marker)
	}

	bundle, err := h.bundles.Fetch(ctx, in.CodeBundleURI)
	if err != nil {
		if errors.Is(err, ports.ErrBundleMalformed) || errors.Is(err, ports.ErrBundleTooLarge) {
			// Re-reading the same bytes cannot fix them: the permanent error
			// dead-letters the message, and the log line makes the failure loud.
			h.logger.Error("code bundle is unreadable — dead-lettering the message",
				"tenant_id", in.TenantID, "release_id", in.ReleaseID, "uri", in.CodeBundleURI, "error", err)
			return fmt.Errorf("%w: code bundle %s: %v", pkgevents.ErrPermanent, in.CodeBundleURI, err)
		}
		// Absent or unreachable: the object may still land, so retry.
		return fmt.Errorf("fetch code bundle %s: %w", in.CodeBundleURI, err)
	}

	doc, err := h.artifacts.Load(ctx, in.TopologyURI, in.TopologySHA256)
	if err != nil {
		if errors.Is(err, ports.ErrTopologyArtifactCorrupt) {
			h.logger.Error("topology artifact does not match the promotion — dead-lettering the message",
				"tenant_id", in.TenantID,
				"release_id", in.ReleaseID,
				"promotion_seq", in.PromotionSeq,
				"uri", in.TopologyURI,
				"expected_sha256", in.TopologySHA256,
				"error", err)
			return fmt.Errorf("%w: topology artifact %s: %v", pkgevents.ErrPermanent, in.TopologyURI, err)
		}
		return fmt.Errorf("load topology artifact %s: %w", in.TopologyURI, err)
	}

	if err := bundleMatchesTopology(bundle, in.TenantID, in.ReleaseID, doc); err != nil {
		// A document that is not this release's must not be written: doing so
		// would stamp this release's provenance onto another release's code and
		// could point :CURRENT at a hash that disagrees with :Node.content_hash.
		h.logger.Error("code bundle does not match the promoted release — dead-lettering the message",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "uri", in.CodeBundleURI, "error", err)
		return fmt.Errorf("%w: code bundle %s: %v", pkgevents.ErrPermanent, in.CodeBundleURI, err)
	}

	batches := versionBatches(planVersionWrite(in, bundle, promotedNodes(doc), h.logger), VersionsBatchSize)
	if len(batches) == 0 {
		return h.recordMarker(ctx, marker)
	}

	var total codeversion.WriteResult
	for i, batch := range batches {
		final := i == len(batches)-1
		var (
			res   codeversion.WriteResult
			wrote bool
		)
		err := h.uow.Write(ctx, marker, final, func(ctx context.Context, tx uow.Tx) error {
			var err error
			res, err = tx.CodeVersions().WriteBatch(ctx, in.TenantID, batch)
			wrote = true
			return err
		})
		if err != nil {
			// The batch and, on the last one, the marker roll back together, so
			// the message replays.
			return fmt.Errorf("write code versions for release %s (batch %d of %d): %w",
				in.ReleaseID, i+1, len(batches), err)
		}
		if !wrote {
			h.logger.Info("release.promoted processed by another delivery of the versions group meanwhile; acknowledged",
				"message_id", messageID, "tenant_id", in.TenantID, "release_id", in.ReleaseID)
			return nil
		}
		total = addWriteResults(total, res)
		h.logger.Debug("code-version batch written",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "batch", i+1, "batches", len(batches))
	}

	h.logger.Info("Code version ingestion finished",
		"tenant_id", in.TenantID,
		"release_id", in.ReleaseID,
		"promotion_seq", in.PromotionSeq,
		"bundle_nodes", len(bundle.Nodes),
		"batches", len(batches),
		"nodes_created", total.NodesCreated,
		"node_versions_created", total.NodeVersionsCreated,
		"unit_versions_created", total.UnitVersionsCreated,
		"current_pointers_moved", total.CurrentPointersMoved,
		"rejections_resolved", total.RejectionsResolved,
	)
	return nil
}

// recordMarker records the message as processed and writes nothing else.
func (h *ReleasePromotedVersionsHandler) recordMarker(ctx context.Context, marker uow.Marker) error {
	if err := h.uow.Write(ctx, marker, true, func(context.Context, uow.Tx) error { return nil }); err != nil {
		return fmt.Errorf("record the processed-message marker: %w", err)
	}
	return nil
}

// addWriteResults sums what two batches wrote.
func addWriteResults(a, b codeversion.WriteResult) codeversion.WriteResult {
	return codeversion.WriteResult{
		NodesCreated:         a.NodesCreated + b.NodesCreated,
		NodeVersionsCreated:  a.NodeVersionsCreated + b.NodeVersionsCreated,
		UnitVersionsCreated:  a.UnitVersionsCreated + b.UnitVersionsCreated,
		CurrentPointersMoved: a.CurrentPointersMoved + b.CurrentPointersMoved,
		RejectionsResolved:   a.RejectionsResolved + b.RejectionsResolved,
	}
}

// bundleMatchesTopology checks that the fetched bundle and the topology artifact
// both describe the release the event promoted, and the artifact its tenant.
// They come from the same parse, so they must agree node by node; a
// disagreement means a URI resolved to another release's object and the code
// must not be recorded under this release's provenance.
func bundleMatchesTopology(b codebundle.Bundle, tenantID, releaseID string, doc topologyartifact.Document) error {
	if b.ReleaseID != releaseID {
		return fmt.Errorf("bundle belongs to release %q, event promoted %q", b.ReleaseID, releaseID)
	}
	if doc.ReleaseID != releaseID {
		return fmt.Errorf("topology artifact belongs to release %q, event promoted %q", doc.ReleaseID, releaseID)
	}
	if doc.TenantID != tenantID {
		return fmt.Errorf("topology artifact belongs to tenant %q, event promoted for %q", doc.TenantID, tenantID)
	}
	for _, n := range doc.Nodes {
		bn, ok := b.Nodes[n.UniqueID]
		if !ok || n.ContentHash == "" {
			// A node absent from the bundle (a dbt-test, for one) has no version
			// to write, and a node with no hash cannot be compared. Neither is
			// evidence that the document is the wrong one.
			continue
		}
		if bn.ContentHash != n.ContentHash {
			return fmt.Errorf("node %s: bundle content_hash %q disagrees with the topology artifact's %q",
				n.UniqueID, bn.ContentHash, n.ContentHash)
		}
	}
	return nil
}
