package redis

import (
	"context"
	"fmt"
	"log/slog"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// parsedManifestInput maps a decoded manifest.loaded.candidate entry to the
// handler's domain-typed input. A failure_kind this build does not declare is
// passed through as is; the handler maps it to internal_error and names it in
// the detail, so a release from a newer producer still rejects and the queue
// still advances.
func parsedManifestInput(p pkgevents.ManifestLoadedCandidate, logger *slog.Logger) handlers.HandleParsedManifestInput {
	kind := pkg_model.ParseFailureKind(p.FailureKind)
	if p.Status == pkgevents.ManifestStatusFailed && !kind.IsValid() {
		logger.Error("manifest.loaded.candidate carries an undeclared failure_kind; rejecting as internal_error",
			"release_id", p.ReleaseID, "failure_kind", p.FailureKind)
	}
	failed := make([]handlers.ParsedFailedNode, 0, len(p.FailedNodes))
	for _, n := range p.FailedNodes {
		failed = append(failed, handlers.ParsedFailedNode{
			NodeID: n.NodeID, Kind: pkg_model.ParseFailureKind(n.Kind), Service: n.Service,
			FilePath: n.FilePath, NodeType: n.NodeType, Detail: n.Detail,
		})
	}
	return handlers.HandleParsedManifestInput{
		ReleaseID:     p.ReleaseID,
		Status:        p.Status,
		TopologyRef:   release.TopologyRef{URI: p.TopologyURI, SHA256: p.TopologySHA256, NodeCount: p.NodeCount},
		CodeBundleURI: p.CodeBundleURI,
		FailureKind:   kind,
		Detail:        p.Detail,
		FailedNodes:   failed,
	}
}

// NewManifestLoadedCandidateConsumer constructs a StreamConsumer that reads
// manifest.loaded.candidate:v2 and dispatches each entry to
// handlers.HandleParsedManifest. The consumer group is created idempotently by
// StreamConsumer.Start; call Start(ctx) in a goroutine to begin consuming.
func NewManifestLoadedCandidateConsumer(
	rc *goredis.Client,
	deps *handlers.Deps,
	logger *slog.Logger,
) *pkgredis.StreamConsumer {
	handler := newManifestLoadedCandidateHandler(deps, logger)
	return pkgredis.NewStreamConsumer(
		rc,
		streams.ManifestLoadedCandidateV2,
		streams.ReleaseControllerManifestLoadedCandidateV2,
		handler,
		logger,
	)
}

// newManifestLoadedCandidateHandler returns a MessageHandler that decodes each
// manifest.loaded.candidate:v2 entry (envelope fields plus payload), calls
// handlers.HandleParsedManifest, and on success advances the release queue.
// Advancing after a failed parse is essential: no kind:"complete" terminal
// message on validation.result:v1 will arrive for a rejected release, so
// without this call every queued candidate would stay in StatusReceived
// indefinitely. An entry that does not decode is a permanent failure, as is a
// corrupt topology artifact met by the handler; the consumer dead-letters both.
func newManifestLoadedCandidateHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		_, p, err := pkgevents.DecodeManifestLoadedCandidate(stringFields(msg.Values))
		if err != nil {
			return fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, streams.ManifestLoadedCandidateV2, err)
		}
		if err := handlers.HandleParsedManifest(ctx, deps, parsedManifestInput(p, logger)); err != nil {
			return permanentOnCorruptTopology(err)
		}
		// Advance the queue after every parse result. On the success path the
		// release is now in Validating (active), so this is a no-op. On the
		// failure path the release was just Rejected, so this unblocks the next
		// queued candidate.
		if err := handlers.AdvanceQueue(ctx, deps); err != nil {
			logger.Error("advance queue after manifest.loaded.candidate", "error", err)
			// Non-fatal: the next incoming message will trigger another advance.
		}
		return nil
	}
}
