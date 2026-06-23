package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation-agent/service/handlers"
)

// requestedPayload mirrors the remediation.requested:v1 wire shape produced by
// the remediation classifier. Only the fields the agent needs are decoded.
type requestedPayload struct {
	EventID         string `json:"event_id"`
	Source          string `json:"source"`
	ReleaseID       string `json:"release_id"`
	NodeID          string `json:"node_id"`
	Category        string `json:"category"`
	ErrorSignature  string `json:"error_signature"`
	DBTLogURI       string `json:"dbt_log_uri"`
	CandidateSQLURI string `json:"candidate_sql_uri"`
	Repo            string `json:"repo"`
	CommitSHA       string `json:"commit_sha"`
}

// triggerFromRequested decodes a remediation.requested:v1 payload into a
// handlers.Trigger. Returns an error if the JSON is malformed.
func triggerFromRequested(raw []byte) (handlers.Trigger, error) {
	var p requestedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return handlers.Trigger{}, fmt.Errorf("unmarshal remediation.requested payload: %w", err)
	}
	return handlers.Trigger{
		Source:          p.Source,
		ReleaseID:       p.ReleaseID,
		NodeID:          p.NodeID,
		Category:        p.Category,
		ErrorSignature:  p.ErrorSignature,
		DBTLogURI:       p.DBTLogURI,
		CandidateSQLURI: p.CandidateSQLURI,
		Repo:            p.Repo,
		CommitSHA:       p.CommitSHA,
	}, nil
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
		trigger, err := triggerFromRequested([]byte(raw))
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
