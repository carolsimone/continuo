package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation-agent/service/handlers"
)

// requestedPayload mirrors the remediation.requested:v1 wire shape produced by
// the remediation classifier. Only the fields the agent needs are decoded.
type requestedPayload struct {
	EventID   string `json:"event_id"`
	Source    string `json:"source"`
	ReleaseID string `json:"release_id"`
	NodeID    string `json:"node_id"`
	// RelationID is the contested physical relation for a duplicate_table
	// trigger, distinct from NodeID (the target claimant's own unique_id) —
	// the two differ whenever the target carries an alias. Empty for every
	// other source.
	RelationID     string `json:"relation_id"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	// Reason is the classifier's finer-grained reason within Category; with
	// Category it forms the fallback precedent-lookup key when the exact
	// signature has no recorded matches.
	Reason string `json:"reason"`
	// ErrorExcerpt is the classifier's key error line for this failure (capped
	// at 4 KiB).
	ErrorExcerpt         string `json:"error_excerpt"`
	DBTLogURI            string `json:"dbt_log_uri"`
	CandidateArtifactURI string `json:"candidate_artifact_uri"`
	// CodeBundleURI locates the release's code-bundle document, which carries
	// every parsed node's raw source. Empty only for a compile-stage
	// rejection, which precedes the parse that produces the bundle; every
	// post-parse rejection (duplicate_table included) carries it.
	CodeBundleURI string `json:"code_bundle_uri"`
	FilePath      string `json:"file_path"`
	// Service is the owning dbt service for the failing node. Set for
	// validation, seed_build, and duplicate_table failures where the source
	// location is threaded from the candidate topology.
	Service string `json:"service"`
	// NodeType is the failing node's kind, set on validation and
	// duplicate-relation failures so the fixer can skip a python node without a
	// topology lookup.
	NodeType string `json:"node_type"`
	// OtherService and OtherFilePath locate the competing node that also
	// produces the contested relation (RelationID), set on a duplicate-relation
	// failure.
	OtherService  string `json:"other_service"`
	OtherFilePath string `json:"other_file_path"`
	Repo          string `json:"repo"`
	CommitSHA     string `json:"commit_sha"`
}

// TriggerFromPayload decodes a remediation.requested:v1 payload into a
// handlers.Trigger. The dedup identity fields (MessageID, OutboxEntryID) are
// left unset: they belong to the Redis message that delivered the payload
// rather than to the payload itself, so a caller replaying stored bytes
// supplies an identity of its own. RawPayload is set to raw so a replayed
// trigger carries the same bytes the original did. Returns an error if the
// JSON is malformed.
func TriggerFromPayload(raw []byte) (handlers.Trigger, error) {
	var p requestedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return handlers.Trigger{}, fmt.Errorf("unmarshal remediation.requested payload: %w", err)
	}
	return handlers.Trigger{
		Source:               p.Source,
		ReleaseID:            p.ReleaseID,
		NodeID:               p.NodeID,
		RelationID:           p.RelationID,
		Category:             p.Category,
		ErrorSignature:       p.ErrorSignature,
		Reason:               p.Reason,
		ErrorExcerpt:         p.ErrorExcerpt,
		DBTLogURI:            p.DBTLogURI,
		CandidateArtifactURI: p.CandidateArtifactURI,
		CodeBundleURI:        p.CodeBundleURI,
		FilePath:             p.FilePath,
		Service:              p.Service,
		NodeType:             p.NodeType,
		OtherService:         p.OtherService,
		OtherFilePath:        p.OtherFilePath,
		Repo:                 p.Repo,
		CommitSHA:            p.CommitSHA,
		RawPayload:           raw,
	}, nil
}

// triggerFromRequested decodes a remediation.requested:v1 payload into a
// handlers.Trigger and stamps it with the dedup identity of the Redis message
// that delivered it.
func triggerFromRequested(msg goredis.XMessage, raw []byte) (handlers.Trigger, error) {
	t, err := TriggerFromPayload(raw)
	if err != nil {
		return handlers.Trigger{}, err
	}
	t.MessageID = msg.ID
	t.OutboxEntryID = messageprocessing.ExtractOutboxEntryID(msg.Values)
	return t, nil
}

// NewRemediationRequestedConsumer constructs a StreamConsumer that reads
// remediation.requested:v1 and proposes a fix per trigger via handlers.ProposeFix.
// The consumer group is created idempotently by StreamConsumer.Start; call
// Start(ctx) in a goroutine to begin consuming.
func NewRemediationRequestedConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	handler := func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			logger.Error("remediation.requested:v1 missing payload — discarding", "message_id", msg.ID)
			return nil // permanent: ACK by returning nil so the message is not left in the PEL
		}
		trigger, err := triggerFromRequested(msg, []byte(raw))
		if err != nil {
			logger.Error("remediation.requested:v1 decode failure — discarding", "message_id", msg.ID, "error", err)
			return nil // permanent: malformed payload cannot be retried
		}
		if err := handlers.ProposeFix(ctx, deps, trigger); err != nil {
			return err // transient: do not ACK; allow redelivery via PEL sweep
		}
		return nil
	}
	return pkgredis.NewStreamConsumer(
		rc,
		streams.RemediationRequestedV1,
		streams.RemediationAgentRemediationRequested,
		handler,
		logger,
	)
}
