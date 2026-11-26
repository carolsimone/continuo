package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/adapters/postgres"
)

// producer is the envelope's producer of every entry the component renders.
const producer = "remediation"

// remediationOutboxPublisher implements pkgoutbox.Publisher by XADDing each
// outbox entry to the Redis stream stored in entry.StreamName.
type remediationOutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

var _ pkgoutbox.Publisher = (*remediationOutboxPublisher)(nil)

// Publish writes the outbox entry to its designated Redis stream. The wire
// format is a "payload" field containing the JSON body, a "tenant_id" field and
// the entry's "outbox_entry_id".
func (p *remediationOutboxPublisher) Publish(ctx context.Context, entry *pkgoutbox.Entry) error {
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
func (p *remediationOutboxPublisher) xaddArgs(entry *pkgoutbox.Entry, values map[string]any) *goredis.XAddArgs {
	values["outbox_entry_id"] = entry.ID.String()
	return &goredis.XAddArgs{
		Stream:     pkgredis.Key(entry.StreamName),
		NoMkStream: true,
		Values:     values,
	}
}

var _ pkgoutbox.Renderer = (*remediationOutboxPublisher)(nil)

// Render returns the field map Publish XADDs for entry, without
// outbox_entry_id. A dead-letter row's scalar fields come from
// outbox.DeadLetterValues. A heal.triggered:v1 row stores its payload
// under its tenant (outbox.WrapTenant) and renders under an envelope that names
// the row as the event, the tenant it stores and the time it was written. Any
// other row's JSON body is the "payload" field beside the default tenant as
// "tenant_id". The tenant is stamped here, not in xaddArgs, because the fields
// an outbox dead letter stores come from Render, so a redrive republishes the
// tenant too. Every call returns a fresh map.
func (p *remediationOutboxPublisher) Render(entry *pkgoutbox.Entry) (map[string]any, error) {
	if entry.EventType == pkgoutbox.DeadLetterEventType {
		values, err := pkgoutbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		delete(values, "outbox_entry_id")
		return values, nil
	}
	if entry.StreamName == streams.HealTriggeredV1 {
		return renderHealTriggered(entry)
	}
	return map[string]any{"payload": string(entry.Payload), "tenant_id": pkgevents.DefaultTenantID}, nil
}

// renderHealTriggered renders a heal.triggered:v1 row. The row's
// stored payload is our own, so a row that is not stored under a tenant or does
// not hold a trigger is permanent: no retry makes it renderable.
func renderHealTriggered(entry *pkgoutbox.Entry) (map[string]any, error) {
	tenantID, payload, err := pkgoutbox.UnwrapTenant(entry.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", pkgevents.ErrPermanent, streams.HealTriggeredV1, err)
	}
	var trigger pkgevents.HealTriggered
	if err := json.Unmarshal(payload, &trigger); err != nil {
		return nil, fmt.Errorf("%w: unmarshal %s: %v", pkgevents.ErrPermanent, streams.HealTriggeredV1, err)
	}
	values, err := pkgevents.HealTriggeredFields(entry.ID.String(), tenantID, producer, entry.CreatedAt, trigger)
	if err != nil {
		return nil, fmt.Errorf("%w: render %s: %v", pkgevents.ErrPermanent, streams.HealTriggeredV1, err)
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
// heartbeat probe (processor.Healthy detects a wedged-but-not-exited loop).
func StartOutboxPublisher(ctx context.Context, db *sqlx.DB, rc *goredis.Client, waker pkgoutbox.Waker, obs pkgoutbox.Observer, liveReg *liveness.Registry, logger *slog.Logger) *pkgoutbox.Processor {
	publisher := &remediationOutboxPublisher{redis: rc, logger: logger}
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
		// Clean stop when the parent ctx is done OR the error unwraps to
		// context.Canceled — a driver-level cancellation on graceful shutdown
		// (e.g. "pq: canceling statement due to user request") does not unwrap to
		// context.Canceled, so checking ctx.Err() too avoids a spurious unhealthy
		// flip and error log during drain.
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			err = nil
		}
		liveReg.WorkerExited("outbox_publisher", err)
		if err != nil {
			logger.Error("remediation outbox publisher stopped unexpectedly", "error", err)
		}
	}()
	return processor
}
