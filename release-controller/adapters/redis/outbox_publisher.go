package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/liveness"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
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

// StartOutboxPublisher constructs a pkgoutbox.Processor backed by
// release_controller_outbox and starts its poll loop in a goroutine. The loop
// runs until ctx is cancelled. Errors are logged; the caller does not need to
// handle the returned error (the goroutine blocks until ctx.Done). It is
// registered with liveReg (RegisterWorker before launch, WorkerExited on
// return) so a genuine unhandled exit — not the processor's own retry loop,
// which already survives transient Redis/Postgres errors — flips /ready.
func StartOutboxPublisher(ctx context.Context, db *sqlx.DB, rc *goredis.Client, liveReg *liveness.Registry, logger *slog.Logger) {
	publisher := &releaseOutboxPublisher{redis: rc, logger: logger}
	processor := pkgoutbox.NewProcessor(
		db,
		"release_controller_outbox",
		publisher,
		nil, // no terminal-failure hook needed for simple event publishing
		logger,
		pkgoutbox.ProcessorConfig{
			Tick:      0,   // default 1s poll interval
			BatchSize: 64,
		},
	)
	liveReg.RegisterWorker("outbox_publisher")
	go func() {
		err := processor.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		liveReg.WorkerExited("outbox_publisher", err)
		if err != nil {
			logger.Error("outbox publisher stopped unexpectedly", "error", err)
		}
	}()
}
