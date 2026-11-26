package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
)

// releaseOutboxPublisher implements pkgoutbox.Publisher by XADDing each outbox
// entry to the Redis stream stored in entry.StreamName.
type releaseOutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

var _ pkgoutbox.Publisher = (*releaseOutboxPublisher)(nil)

// Publish writes the outbox entry to its designated Redis stream, with the
// fields Render decides for the row.
func (p *releaseOutboxPublisher) Publish(ctx context.Context, entry *pkgoutbox.Entry) error {
	values, err := p.Render(entry)
	if err != nil {
		return err
	}
	args := p.xaddArgs(entry, values)
	if _, err := p.redis.XAdd(ctx, args).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, pkgredis.MissingStreamError(args.Stream, err))
	}
	return nil
}

// xaddArgs builds the XADD for entry from its rendered fields plus
// outbox_entry_id. It sets no length cap: the dead-letter-controller's trim loop
// bounds every stream.
//
// The stream is the prefixed key of entry.StreamName, and NOMKSTREAM keeps XADD
// from creating it: while the stream is missing the XADD fails and the outbox
// retries the row.
func (p *releaseOutboxPublisher) xaddArgs(entry *pkgoutbox.Entry, values map[string]any) *goredis.XAddArgs {
	values["outbox_entry_id"] = entry.ID.String()
	return &goredis.XAddArgs{
		Stream:     pkgredis.Key(entry.StreamName),
		NoMkStream: true,
		Values:     values,
	}
}

var _ pkgoutbox.Renderer = (*releaseOutboxPublisher)(nil)

// envelopeProducer names release-controller as the producer of the events it
// publishes with an envelope.
const envelopeProducer = "release-controller"

// Render returns the field map Publish XADDs for entry, without
// outbox_entry_id: a dead-letter row's scalar fields, a release.promoted:v1
// row's envelope fields, a remediation.retry_requested:v1 row's envelope
// fields, or the row's JSON body as the single "payload" field. Every call
// returns a fresh map.
func (p *releaseOutboxPublisher) Render(entry *pkgoutbox.Entry) (map[string]any, error) {
	switch entry.EventType {
	case pkgoutbox.DeadLetterEventType:
		values, err := pkgoutbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		delete(values, "outbox_entry_id")
		return values, nil
	case ports.ReleasePromotedEventType:
		var promoted pkgevents.ReleasePromoted
		if err := json.Unmarshal(entry.Payload, &promoted); err != nil {
			return nil, fmt.Errorf("%w: release.promoted payload: %v", pkgevents.ErrPermanent, err)
		}
		return pkgevents.ReleasePromotedFields(pkgevents.DefaultTenantID, envelopeProducer, entry.CreatedAt, promoted)
	}
	if entry.StreamName == streams.RemediationRetryRequestedV1 {
		return renderRemediationRetryRequested(entry)
	}
	return map[string]any{"payload": string(entry.Payload)}, nil
}

// renderRemediationRetryRequested renders a row on remediation.retry_requested:v1:
// the row stores the typed retry under its tenant (pkgoutbox.WrapTenant), and
// the envelope names the row as the event, the tenant it stores and the time it
// was written. The row is recognised by its stream rather than its event type:
// the stream's version is what fixes the payload's contract. Every failure is
// permanent, since the stored payload is release-controller's own and decodes
// the same way on every attempt.
func renderRemediationRetryRequested(entry *pkgoutbox.Entry) (map[string]any, error) {
	tenantID, payload, err := pkgoutbox.UnwrapTenant(entry.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", pkgevents.ErrPermanent, streams.RemediationRetryRequestedV1, err)
	}
	var retry pkgevents.RemediationRetryRequested
	if err := json.Unmarshal(payload, &retry); err != nil {
		return nil, fmt.Errorf("%w: unmarshal %s: %v", pkgevents.ErrPermanent, streams.RemediationRetryRequestedV1, err)
	}
	values, err := pkgevents.RemediationRetryRequestedFields(entry.ID.String(), tenantID, envelopeProducer, entry.CreatedAt, retry)
	if err != nil {
		return nil, fmt.Errorf("%w: render %s: %v", pkgevents.ErrPermanent, streams.RemediationRetryRequestedV1, err)
	}
	return values, nil
}

// outboxHeartbeatStale is the liveness budget for the outbox processor's Run
// loop. An idle loop turns at least once per pkgoutbox.FallbackTick (5s), so 60s
// is comfortably above it: a wedged (not exited) processor trips within a
// minute, while an idle-but-live one never does.
const outboxHeartbeatStale = 60 * time.Second

// StartOutboxPublisher constructs a pkgoutbox.Processor backed by postgres.OutboxTable,
// starts its relay loop in a goroutine and returns the processor. The loop
// drains the table on each signal from waker and every pkgoutbox.FallbackTick,
// until ctx is cancelled; obs receives each batch's publish and failure counts.
// It is registered with liveReg both as a worker
// (RegisterWorker/WorkerExited detects a full goroutine EXIT) and with a
// heartbeat probe (processor.Healthy detects a wedged-but-not-exited loop) —
// the outbox analogue of a consumer's liveness backstop.
func StartOutboxPublisher(ctx context.Context, db *sqlx.DB, rc *goredis.Client, waker pkgoutbox.Waker, obs pkgoutbox.Observer, liveReg *liveness.Registry, logger *slog.Logger) *pkgoutbox.Processor {
	publisher := &releaseOutboxPublisher{redis: rc, logger: logger}
	processor := pkgoutbox.NewProcessor(
		db,
		postgres.OutboxTable,
		publisher,
		nil, // no terminal-failure hook needed for simple event publishing
		logger,
		pkgoutbox.ProcessorConfig{
			Tick:      pkgoutbox.FallbackTick,
			BatchSize: 64,
			Waker:     waker,
			Observer:  obs,
		},
	)
	liveReg.RegisterWorker("outbox_publisher")
	liveReg.AddWorkerProbe("outbox_publisher_heartbeat", 10*time.Second, func(context.Context) error {
		return processor.Healthy(outboxHeartbeatStale)
	})
	go func() {
		err := processor.Run(ctx)
		// Treat as a clean stop when the parent ctx is done OR the error unwraps
		// to context.Canceled — a driver-level cancellation on graceful shutdown
		// (e.g. "pq: canceling statement due to user request") does not unwrap to
		// context.Canceled, so checking ctx.Err() too avoids a spurious unhealthy
		// flip and error log during drain.
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			err = nil
		}
		liveReg.WorkerExited("outbox_publisher", err)
		if err != nil {
			logger.Error("outbox publisher stopped unexpectedly", "error", err)
		}
	}()
	return processor
}
