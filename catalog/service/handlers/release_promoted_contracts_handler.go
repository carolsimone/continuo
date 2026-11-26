package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// LiveContractsRewriter rewrites a tenant's live contracts folder after a
// promotion's contract versions are recorded. An error leaves the message
// unfinished, so its redelivery rewrites the folder again.
type LiveContractsRewriter interface {
	// Rewrite returns the fingerprint of the publishing settings the
	// folder's files carry once it returns, or "" when it wrote nothing and
	// cannot tell.
	Rewrite(ctx context.Context, in domainModel.PromoteReleaseInput) (string, error)
}

// ReleasePromotedContractsHandler consumes release.promoted:v1 on its own
// consumer group and records the release's contract versions in the tenant's
// graph: one stored contract per (node, contract hash), a promotion edge for
// each node whose contract the promotion changes, and the node's pointer to
// its current contract. Then it rewrites the tenant's live contracts folder.
//
// It is separate from the topology swap and the code versions, so a contract
// bundle that cannot be read, or object storage that refuses the live folder,
// never holds either of them up.
type ReleasePromotedContractsHandler struct {
	uow           uow.UnitOfWork
	bundles       ports.ContractBundleReader
	live          LiveContractsRewriter
	metrics       ports.ContractMetrics
	inflightRenew time.Duration
	logger        *slog.Logger
}

// NewReleasePromotedContractsHandler creates a ReleasePromotedContractsHandler.
// inflightRenew is how often a running live rewrite renews the message's
// in-flight mark, and how long one renewal may take; it must be well below
// half of uow.InFlightTTL.
func NewReleasePromotedContractsHandler(
	u uow.UnitOfWork,
	bundles ports.ContractBundleReader,
	live LiveContractsRewriter,
	metrics ports.ContractMetrics,
	inflightRenew time.Duration,
	logger *slog.Logger,
) *ReleasePromotedContractsHandler {
	return &ReleasePromotedContractsHandler{
		uow: u, bundles: bundles, live: live, metrics: metrics, inflightRenew: inflightRenew, logger: logger,
	}
}

// Handle processes one promotion of in.TenantID on the contracts group. Steps:
//  1. Look the group's marker up, by messageID or by outboxEntryID. A
//     processed message is acknowledged without reading object storage.
//  2. A promotion with no contract bundle records only its marker.
//  3. While an export or a restore holds the tenant's lease it returns
//     uow.ErrWritesPaused without reading object storage. Otherwise it reads
//     the bundle and verifies its SHA-256, outside any transaction. A bundle
//     the reader refuses as invalid, or one of another tenant or release, is
//     permanent; a missing bundle or an outage is retried.
//  4. Transaction A reads the nodes' contract states, plans the promotion
//     (contractversion.Plan) and writes the plan, and marks the message in
//     flight, so no export starts before transaction B. The versions counter
//     moves once it has committed.
//  5. Rewrite the tenant's live contracts folder. While the rewrite runs, a
//     write that is not final renews the message's in-flight mark every
//     inflightRenew, so the mark outlives a rewrite of any length.
//  6. Transaction B records the live folder's watermark (the promotion's
//     seq and release, kept when a larger seq is stored) and, when the
//     rewrite reports the settings the folder's files carry, their
//     fingerprint; it records the marker and clears the in-flight mark.
//
// A failure after transaction A ends the message with no marker. Its
// redelivery plans against what transaction A wrote, which records no change
// twice and creates no version twice, and rewrites the folder again.
func (h *ReleasePromotedContractsHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in domainModel.PromoteReleaseInput,
) error {
	h.logger.Info("Processing release.promoted:v1 (contracts group)",
		"message_id", messageID,
		"tenant_id", in.TenantID,
		"release_id", in.ReleaseID,
		"promotion_seq", in.PromotionSeq,
	)
	marker := uow.Marker{
		TenantID:      in.TenantID,
		Group:         streams.CatalogReleaseContracts,
		MessageID:     messageID,
		OutboxEntryID: outboxEntryID,
	}

	processed, err := h.uow.Processed(ctx, marker)
	if err != nil {
		return fmt.Errorf("look up the processed-message marker: %w", err)
	}
	if processed {
		h.logger.Info("release.promoted already processed by the contracts group; acknowledged",
			"message_id", messageID, "tenant_id", in.TenantID, "release_id", in.ReleaseID)
		return nil
	}

	if in.ContractBundleURI == "" {
		// An announced topology carries no contract bundle, and no retry can
		// produce one, so record the message as handled.
		h.logger.Info("release.promoted carries no contract_bundle_uri — no contracts to record",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "promotion_seq", in.PromotionSeq)
		if err := h.uow.Write(ctx, marker, true, func(context.Context, uow.Tx) error { return nil }); err != nil {
			return fmt.Errorf("record the processed-message marker: %w", err)
		}
		return nil
	}

	// While the tenant's writes are paused the first write would be refused and
	// the message redelivered, so skip the download it would repeat.
	paused, err := h.uow.WritesPaused(ctx, in.TenantID)
	if err != nil {
		return fmt.Errorf("check whether the tenant's writes are paused: %w", err)
	}
	if paused {
		return fmt.Errorf("tenant %q: %w", in.TenantID, uow.ErrWritesPaused)
	}

	ingestStarted := time.Now()
	bundle, err := h.fetch(ctx, in)
	if err != nil {
		return err
	}

	promotion := contractversion.Promotion{
		TenantID:          in.TenantID,
		ReleaseID:         in.ReleaseID,
		ContinuoContracts: bundle.ContinuoContracts,
		OntologyVersion:   bundle.OntologyVersion,
		PromotionSeq:      in.PromotionSeq,
		PromotedAt:        in.PromotedAt,
	}
	uniqueIDs := make([]string, 0, len(bundle.Contracts))
	for _, c := range bundle.Contracts {
		uniqueIDs = append(uniqueIDs, c.UniqueID)
	}
	var (
		plan  contractversion.WritePlan
		res   contractversion.WriteResult
		wrote bool
	)
	err = h.uow.Write(ctx, marker, false, func(ctx context.Context, tx uow.Tx) error {
		states, err := tx.Contracts().ReadStates(ctx, in.TenantID, uniqueIDs)
		if err != nil {
			return err
		}
		plan = contractversion.Plan(promotion, bundle.Contracts, states)
		res, err = tx.Contracts().Write(ctx, in.TenantID, promotion, plan)
		wrote = true
		return err
	})
	if err != nil {
		return fmt.Errorf("write contract versions for release %s: %w", in.ReleaseID, err)
	}
	if !wrote {
		h.logger.Info("release.promoted processed by another delivery of the contracts group meanwhile; acknowledged",
			"message_id", messageID, "tenant_id", in.TenantID, "release_id", in.ReleaseID)
		return nil
	}
	h.metrics.VersionsWritten(res.VersionsCreated)
	h.metrics.ObserveStep(ports.ContractStepIngest, time.Since(ingestStarted))
	h.logger.Info("Contract versions recorded",
		"tenant_id", in.TenantID,
		"release_id", in.ReleaseID,
		"promotion_seq", in.PromotionSeq,
		"contracts", len(bundle.Contracts),
		"versions_created", res.VersionsCreated,
		"edges_created", res.EdgesCreated,
		"pointers_moved", res.PointersMoved,
		"unchanged", plan.Unchanged,
		"superseded", plan.Superseded,
	)

	rewriteStarted := time.Now()
	settings, err := h.rewrite(ctx, marker, in)
	if err != nil {
		return fmt.Errorf("rewrite the live contracts folder for release %s: %w", in.ReleaseID, err)
	}
	h.metrics.ObserveStep(ports.ContractStepLiveRewrite, time.Since(rewriteStarted))

	if err := h.uow.Write(ctx, marker, true, func(ctx context.Context, tx uow.Tx) error {
		return tx.Contracts().RecordLiveFolder(ctx, in.TenantID, in.PromotionSeq, in.ReleaseID, settings)
	}); err != nil {
		return fmt.Errorf("finish release %s on the contracts group: %w", in.ReleaseID, err)
	}
	return nil
}

// rewrite runs the live rewrite while renewing the message's in-flight mark,
// each renewal bounded by inflightRenew. A renewal that fails or runs out of
// time is logged and the rewrite goes on: once the mark lapses an export may
// start before transaction B, which then backs off, and the redelivery finds
// transaction A's writes and the rewrite done.
func (h *ReleasePromotedContractsHandler) rewrite(ctx context.Context, marker uow.Marker, in domainModel.PromoteReleaseInput) (string, error) {
	stop := keepAlive(ctx, h.inflightRenew, h.inflightRenew, func(ctx context.Context) error {
		return h.uow.Write(ctx, marker, false, func(context.Context, uow.Tx) error { return nil })
	}, func(err error) {
		h.logger.Warn("could not renew the in-flight mark during the live rewrite",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "error", err)
	})
	defer stop()
	return h.live.Rewrite(ctx, in)
}

// fetch reads the promotion's contract bundle and checks that it is the
// promoted tenant's and release's. A bundle the reader refuses as invalid, or
// one of another tenant or release, is permanent; any other error is returned
// as it is, so a missing bundle and an outage are retried.
func (h *ReleasePromotedContractsHandler) fetch(ctx context.Context, in domainModel.PromoteReleaseInput) (ports.ContractBundle, error) {
	bundle, err := h.bundles.Fetch(ctx, in.ContractBundleURI, in.ContractBundleSHA256)
	if err != nil {
		if errors.Is(err, ports.ErrContractBundleInvalid) {
			// Re-reading the same bytes cannot fix them: the permanent error
			// dead-letters the message, and the log line makes the failure loud.
			h.logger.Error("contract bundle is unreadable — dead-lettering the message",
				"tenant_id", in.TenantID, "release_id", in.ReleaseID, "uri", in.ContractBundleURI, "error", err)
			return ports.ContractBundle{}, fmt.Errorf("%w: contract bundle %s: %v", pkgevents.ErrPermanent, in.ContractBundleURI, err)
		}
		// Absent or unreachable: the object may still land, so retry.
		return ports.ContractBundle{}, fmt.Errorf("fetch contract bundle %s: %w", in.ContractBundleURI, err)
	}
	if bundle.TenantID != in.TenantID {
		// Recording it would put another tenant's contracts in this tenant's
		// graph.
		h.logger.Error("contract bundle does not belong to the promotion's tenant — dead-lettering the message",
			"tenant_id", in.TenantID, "bundle_tenant_id", bundle.TenantID, "release_id", in.ReleaseID,
			"uri", in.ContractBundleURI)
		return ports.ContractBundle{}, fmt.Errorf("%w: contract bundle %s belongs to tenant %q, event promoted for %q",
			pkgevents.ErrPermanent, in.ContractBundleURI, bundle.TenantID, in.TenantID)
	}
	if bundle.ReleaseID != in.ReleaseID {
		// Recording it would stamp this promotion onto another release's
		// contracts.
		h.logger.Error("contract bundle does not belong to the promoted release — dead-lettering the message",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "bundle_release_id", bundle.ReleaseID,
			"uri", in.ContractBundleURI)
		return ports.ContractBundle{}, fmt.Errorf("%w: contract bundle %s belongs to release %q, event promoted %q",
			pkgevents.ErrPermanent, in.ContractBundleURI, bundle.ReleaseID, in.ReleaseID)
	}
	return bundle, nil
}
