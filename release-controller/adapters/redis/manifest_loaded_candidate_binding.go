package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/adapters/serialization"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// parsedManifestDTO is the JSON shape of a manifest.loaded.candidate:v1 payload.
// It carries the json tags for the parse result so the handler input stays a
// domain-typed struct; the topology decodes through the shared Node DTO.
type parsedManifestDTO struct {
	ReleaseID     string                    `json:"release_id"`
	Status        string                    `json:"status"`
	Topology      serialization.TopologyDTO `json:"topology,omitempty"`
	CodeBundleURI string                    `json:"code_bundle_uri,omitempty"`
	FailureKind   string                    `json:"failure_kind,omitempty"`
	Detail        string                    `json:"detail,omitempty"`
	FailedNodes   []parsedFailedNodeDTO     `json:"failed_nodes,omitempty"`
}

// parsedFailedNodeDTO is one failed_nodes entry of a failed parse.
type parsedFailedNodeDTO struct {
	NodeID   string `json:"node_id"`
	Kind     string `json:"kind"`
	Service  string `json:"service"`
	FilePath string `json:"file_path"`
	NodeType string `json:"node_type"`
	Detail   string `json:"detail"`
}

// toInput maps the decoded wire DTO to the handler's domain-typed input. A
// failure_kind this build does not declare is passed through as is; the
// handler maps it to internal_error and names it in the detail, so a release
// from a newer producer still rejects and the queue still advances.
func (d parsedManifestDTO) toInput(logger *slog.Logger) handlers.HandleParsedManifestInput {
	kind := pkg_model.ParseFailureKind(d.FailureKind)
	if d.Status == "failed" && !kind.IsValid() {
		logger.Error("manifest.loaded.candidate:v1 carries an undeclared failure_kind; rejecting as internal_error",
			"release_id", d.ReleaseID, "failure_kind", d.FailureKind)
	}
	failed := make([]handlers.ParsedFailedNode, 0, len(d.FailedNodes))
	for _, n := range d.FailedNodes {
		failed = append(failed, handlers.ParsedFailedNode{
			NodeID: n.NodeID, Kind: pkg_model.ParseFailureKind(n.Kind), Service: n.Service,
			FilePath: n.FilePath, NodeType: n.NodeType, Detail: n.Detail,
		})
	}
	return handlers.HandleParsedManifestInput{
		ReleaseID:     d.ReleaseID,
		Status:        d.Status,
		Topology:      d.Topology.ToDomain(),
		CodeBundleURI: d.CodeBundleURI,
		FailureKind:   kind,
		Detail:        d.Detail,
		FailedNodes:   failed,
	}
}

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
// Advancing after a failed parse is essential: no kind:"complete" terminal
// message on validation.result:v1 will arrive for a rejected release, so
// without this call every queued candidate would stay in StatusReceived
// indefinitely.
func newManifestLoadedCandidateHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		var dto parsedManifestDTO
		if err := decodePayload(msg, &dto); err != nil {
			logger.Error("manifest.loaded.candidate:v1 decode failure — discarding",
				"message_id", msg.ID, "error", err)
			return nil // permanent: ACK by returning nil so it is not left in the PEL
		}
		if err := handlers.HandleParsedManifest(ctx, deps, dto.toInput(logger)); err != nil {
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
