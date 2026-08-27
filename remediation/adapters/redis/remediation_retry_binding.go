package redis

import (
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/service/handlers"
)

// NewRemediationRetryConsumer consumes remediation.retry_requested:v1 — a
// rejected release's stored rejection replayed with a remediation_round — and
// classifies it exactly as the original rejection was, so a "try again" walks
// the same path as the rejection itself, one round later.
func NewRemediationRetryConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return classifyRejectionMessages(rc, streams.RemediationRetryRequestedV1, streams.RemediationRetryRequested, deps, logger)
}
