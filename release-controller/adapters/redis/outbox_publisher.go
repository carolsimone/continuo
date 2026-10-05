package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
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

// Publish writes the outbox entry payload to its designated Redis stream.
// The wire format is a single "payload" field containing the JSON body, which
// is consistent with how release-controller event consumers decode messages.
func (p *releaseOutboxPublisher) Publish(ctx context.Context, entry *pkgoutbox.Entry) error {
	if entry.EventType == pkgoutbox.DeadLetterEventType {
		// Dead-letter rows publish generically: their payload is already a flat
		// scalar map (pkgoutbox.DeadLetterPayload), expanded via DeadLetterValues
		// rather than nested under a single "payload" field like other events.
		values, err := pkgoutbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		if _, err := p.redis.XAdd(ctx, &goredis.XAddArgs{
			Stream: entry.StreamName,
			MaxLen: 10000,
			Approx: true,
			Values: values,
		}).Result(); err != nil {
			return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
		}
		return nil
	}
	_, err := p.redis.XAdd(ctx, &goredis.XAddArgs{
		Stream: entry.StreamName,
		MaxLen: 10000,
		Approx: true,
		Values: map[string]any{
			"outbox_entry_id": entry.ID.String(),
			"payload":         string(entry.Payload),
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
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
