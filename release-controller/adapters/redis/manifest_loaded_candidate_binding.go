package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// NewManifestLoadedCandidateConsumer constructs a StreamConsumer that reads
// manifest.loaded.candidate:v1 and dispatches each message to
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
		streams.ManifestLoadedCandidateV1,
		streams.ReleaseControllerManifestLoadedCandidate,
		handler,
		logger,
	)
}

// newManifestLoadedCandidateHandler returns a MessageHandler that decodes the
// "payload" field of each manifest.loaded.candidate:v1 message, calls
// handlers.HandleParsedManifest, and on success advances the release queue.
// Advancing after a failed parse is essential: no validation.completed:v1 will
// arrive for a rejected release, so without this call every queued candidate
// would stay in StatusReceived indefinitely.
func newManifestLoadedCandidateHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		var in handlers.HandleParsedManifestInput
		if err := decodePayload(msg, &in); err != nil {
			logger.Error("manifest.loaded.candidate:v1 decode failure — discarding",
				"message_id", msg.ID, "error", err)
			return nil // permanent: ACK by returning nil so it is not left in the PEL
		}
		if err := handlers.HandleParsedManifest(ctx, deps, in); err != nil {
			return err
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

// decodePayload extracts the "payload" field from a Redis stream message and
// unmarshals it into dst.
func decodePayload(msg goredis.XMessage, dst any) error {
	raw, ok := msg.Values["payload"].(string)
	if !ok {
		return fmt.Errorf("missing or non-string payload field")
	}
	if err := json.Unmarshal([]byte(raw), dst); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	return nil
}
