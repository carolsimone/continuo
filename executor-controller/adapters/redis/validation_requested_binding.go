// executor-controller/adapters/redis/validation_requested_binding.go
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

// validationDedupNamespace derives a deterministic outbox_entry_id keying
// validation.requested:v1 dedup on the release rather than (message_id,
// stream_name). DO NOT CHANGE — a different value would re-dedup every
// previously processed release as fresh.
var validationDedupNamespace = uuid.MustParse("c4f8e1a0-2b3d-4e5f-9a8b-7c6d5e4f3a2b")

// validationDedupKey maps a release_id to the deterministic UUID used as the
// outbox_entry_id override in message_processing. One validation.requested:v1
// message carries every node for a release, so dedup is per-release: a
// redelivery (even with a fresh Redis message_id) collides on this key and is
// skipped, so the release's nodes are enqueued exactly once.
func validationDedupKey(releaseID string) uuid.UUID {
	return uuid.NewSHA1(validationDedupNamespace, []byte("validation-requested:"+releaseID))
}

// NewValidationRequestedBinding returns a pkg/redis.MessageHandler that parses
// each validation.requested:v1 message, runs per-release dedup, and invokes
// ValidationRequestedHandler inside a single Unit-of-Work transaction. The
// wire-stable stream name is sourced from pkg/streams
// (streams.ValidationRequestedV1); no stream literal lives in this file.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and drop),
// while handler/repository failures propagate as-is (NACK and let the message
// stay pending for retry). On a duplicate the transaction is committed (empty
// txn) and nil is returned so the consumer ACKs.
//
// Dedup keys on a deterministic release-derived outbox_entry_id (see
// validationDedupKey) rather than the Redis message_id, so a redelivered
// message for an already-processed release enqueues no extra nodes. Per-node
// uniqueness is additionally enforced at the DB level by the partial unique
// index uq_executor_deployments_validation_release_node.
//
// Before enqueuing nodes, the binding creates the release's candidate schema in
// the dbt warehouse exactly once via schemaCreator. Doing it here — once per
// release, ahead of any job — means every validation job (including seeds, which
// run `dbt seed --empty` and create the schema through dbt's own non-race-safe
// path) finds the schema already present, so no two parallel root jobs race
// `CREATE SCHEMA` on pg_namespace. Schema creation runs against the warehouse
// outside the executor's Unit-of-Work transaction; a failure returns before any
// deployment row is enqueued and the message is retried.
func NewValidationRequestedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.ValidationRequestedHandler,
	schemaCreator ports.CandidateSchemaCreator,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseValidationRequested(msg)
		if err != nil {
			logger.Error("validation.requested: parse failure", "message_id", msg.ID, "error", err)
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
					logger.Error("validation.requested: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		dedupKey := validationDedupKey(evt.ReleaseID)
		msgProcID, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, streams.ValidationRequestedV1, payload,
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

		// Create the candidate schema once, race-safely, before any node is
		// enqueued. A non-empty candidate_schema is always present on a real
		// validation request; if absent (a malformed/legacy message) skip the
		// pre-create and let the per-node paths fall back to their own creation.
		if evt.CandidateSchema != "" {
			if err := schemaCreator.EnsureCandidateSchema(ctx, evt.CandidateSchema); err != nil {
				logger.Error("validation.requested: candidate schema pre-create failed",
					"message_id", msg.ID, "release_id", evt.ReleaseID,
					"candidate_schema", evt.CandidateSchema, "error", err)
				return fmt.Errorf("ensure candidate schema %s: %w", evt.CandidateSchema, err)
			}
		}

		if err := handler.Handle(ctx, u, evt, msgProcID); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error("validation.requested: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("validation.requested: transient handler error",
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
