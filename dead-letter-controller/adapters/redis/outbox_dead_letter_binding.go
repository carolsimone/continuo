package redis

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// OutboxDeadLetterBinding stores each outbox.dead_letter:v1 entry. It is
// redrivable when the entry carries the fields the row would have published.
// An entry it cannot decode is stored as unreadable and acknowledged; a store
// failure is returned so the entry stays pending (see ConsumerDeadLetterBinding).
type OutboxDeadLetterBinding struct {
	rec   Recorder
	clock ports.Clock
}

func NewOutboxDeadLetterBinding(rec Recorder, clock ports.Clock) *OutboxDeadLetterBinding {
	return &OutboxDeadLetterBinding{rec: rec, clock: clock}
}

func (b *OutboxDeadLetterBinding) Handle(ctx context.Context, msg goredis.XMessage) error {
	now := b.clock.Now()
	p, original, err := outbox.DecodeDeadLetterFields(stringFields(msg.Values))
	if err != nil {
		return b.rec.Record(ctx, unreadable(deadletter.SourceOutbox, streams.OutboxDeadLetterV1,
			streams.DeadLetterControllerOutboxDeadLetters, msg, err, now))
	}
	originalAt := idTimeOr(msg.ID, now)
	if t, err := time.Parse(outbox.OriginalCreatedAtLayout, p.OriginalCreatedAt); err == nil {
		originalAt = t.UTC()
	}
	return b.rec.Record(ctx, deadletter.DeadLetter{
		ID: uuid.New(), DedupKey: deadletter.OutboxKey(p.OutboxTable, p.FailedOutboxID), Source: deadletter.SourceOutbox,
		FailureKind: p.FailureKind, Stream: p.OriginalStream, Producer: p.OutboxTable, Error: p.Error,
		DeliveryCount: int64(p.Attempts), Fields: original, Redrivable: len(original) > 0,
		OriginalEventType: p.OriginalEventType, FailedOutboxID: p.FailedOutboxID, OriginalAt: originalAt,
		RecordedAt: now, Status: deadletter.StatusOpen,
	})
}
