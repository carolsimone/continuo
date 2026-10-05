package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/agent-remediation/adapters/postgres"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
)

// agentRemediationOutboxPublisher implements pkgoutbox.Publisher by XADDing each
// outbox entry to the Redis stream stored in entry.StreamName.
type agentRemediationOutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

var _ pkgoutbox.Publisher = (*agentRemediationOutboxPublisher)(nil)

// Publish writes the outbox entry payload to its designated Redis stream.
// The wire format uses a single "payload" field containing the JSON body,
// consistent with how other service event consumers decode messages.
func (p *agentRemediationOutboxPublisher) Publish(ctx context.Context, entry *pkgoutbox.Entry) error {
	values, err := p.Render(entry)
	if err != nil {
		return err
	}
	if _, err := p.redis.XAdd(ctx, p.xaddArgs(entry, values)).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}

// xaddArgs builds the XADD for entry from its rendered fields plus
// outbox_entry_id. It sets no length cap: the dead-letter-controller's trim loop
// bounds every stream.
func (p *agentRemediationOutboxPublisher) xaddArgs(entry *pkgoutbox.Entry, values map[string]any) *goredis.XAddArgs {
	values["outbox_entry_id"] = entry.ID.String()
	return &goredis.XAddArgs{
		Stream: entry.StreamName,
		Values: values,
	}
}

var _ pkgoutbox.Renderer = (*agentRemediationOutboxPublisher)(nil)

// Render returns the field map Publish XADDs for entry, without
// outbox_entry_id: a dead-letter row's scalar fields, or the row's JSON body as
// the single "payload" field. Every call returns a fresh map.
func (p *agentRemediationOutboxPublisher) Render(entry *pkgoutbox.Entry) (map[string]any, error) {
	if entry.EventType == pkgoutbox.DeadLetterEventType {
		values, err := pkgoutbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		delete(values, "outbox_entry_id")
		return values, nil
	}
	return map[string]any{"payload": string(entry.Payload)}, nil
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
	publisher := &agentRemediationOutboxPublisher{redis: rc, logger: logger}
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
			logger.Error("agent-remediation outbox publisher stopped unexpectedly", "error", err)
		}
	}()
	return processor
}
