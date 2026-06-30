// executor-controller/adapters/redis/seed_build_requested_binding.go
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// seedBuildDedupNamespace derives a deterministic outbox_entry_id keying
// seed.build.requested:v1 dedup on the release rather than (message_id,
// stream_name). DO NOT CHANGE — a different value would re-dedup every
// previously processed release as fresh.
var seedBuildDedupNamespace = uuid.MustParse("d5a9f2b1-3c4e-5f6a-8b9c-1d2e3f4a5b6c")

// seedBuildDedupKey maps a release_id to the deterministic UUID used as the
// outbox_entry_id override in message_processing. One seed.build.requested:v1
// message carries every seed for a release, so dedup is per-release: a
// redelivery (even with a fresh Redis message_id) collides on this key and is
// skipped, so the release's seeds are enqueued exactly once.
func seedBuildDedupKey(releaseID string) uuid.UUID {
	return uuid.NewSHA1(seedBuildDedupNamespace, []byte("seed-build-requested:"+releaseID))
}

// NewSeedBuildRequestedBinding returns a pkg/redis.MessageHandler that parses
// each seed.build.requested:v1 message, runs per-release dedup, and invokes
// SeedBuildRequestedHandler inside a single Unit-of-Work transaction. The
// wire-stable stream name is sourced from pkg/streams
// (streams.SeedBuildRequestedV1); no stream literal lives in this file.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and drop),
// while handler/repository failures propagate as-is (NACK and let the message
// stay pending for retry). On a duplicate the transaction is committed (empty
// txn) and nil is returned so the consumer ACKs.
//
// Before enqueueing seeds, the binding creates the release's candidate schema in
// the dbt warehouse exactly once via schemaCreator. Seeds build into this
// schema; creating it here ensures it exists before any seed job races
// `CREATE SCHEMA` on pg_namespace. Schema creation runs outside the UoW
// transaction; a failure returns before any deployment row is enqueued and the
// message is retried.
func NewSeedBuildRequestedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.SeedBuildRequestedHandler,
	schemaCreator ports.CandidateSchemaCreator,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseSeedBuildRequested(msg)
		if err != nil {
			logger.Error("seed.build.requested: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}

		payload, mErr := json.Marshal(msg.Values)
		if mErr != nil {
			payload = []byte("{}")
		}

		u := uowFactory()
		if err := u.Begin(ctx); err != nil {
			return fmt.Errorf("begin uow: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rbErr := u.Rollback(); rbErr != nil {
					logger.Error("seed.build.requested: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		dedupKey := seedBuildDedupKey(evt.ReleaseID)
		msgProcID, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, streams.SeedBuildRequestedV1, payload,
			&dedupKey,
		)
		if err != nil {
			return err
		}
		if dup {
			if err := u.Commit(); err != nil {
				return fmt.Errorf("commit dedup tx: %w", err)
			}
			committed = true
			return nil
		}

		// Create the candidate schema once, race-safely, before any seed is
		// enqueued. A non-empty candidate_schema is always present on a real
		// seed-build request; if absent (a malformed/legacy message) skip the
		// pre-create.
		if evt.CandidateSchema != "" {
			if err := schemaCreator.EnsureCandidateSchema(ctx, evt.CandidateSchema); err != nil {
				logger.Error("seed.build.requested: candidate schema pre-create failed",
					"message_id", msg.ID, "release_id", evt.ReleaseID,
					"candidate_schema", evt.CandidateSchema, "error", err)
				return fmt.Errorf("ensure candidate schema %s: %w", evt.CandidateSchema, err)
			}
		}

		if err := handler.Handle(ctx, u, evt, msgProcID); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error("seed.build.requested: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("seed.build.requested: transient handler error",
					"message_id", msg.ID, "error", err)
			}
			return err
		}
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		committed = true
		return nil
	}
}
