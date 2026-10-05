package redis

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ConsumerDeadLetterBinding stores each consumer.dead_letter:v1 entry.
//
// Two outcomes are kept apart. An entry it cannot decode is stored as an
// unreadable, non-redrivable dead letter and Handle returns nil, so the entry is
// acknowledged: returning a decode failure would redeliver it forever. A failure
// to store, whether of a readable or an unreadable entry, is returned, so the
// entry stays pending and is stored once the store recovers (the service's own
// consumers run without dead-lettering, so nothing else can claim the entry).
type ConsumerDeadLetterBinding struct {
	rec   Recorder
	clock ports.Clock
}

func NewConsumerDeadLetterBinding(rec Recorder, clock ports.Clock) *ConsumerDeadLetterBinding {
	return &ConsumerDeadLetterBinding{rec: rec, clock: clock}
}

func (b *ConsumerDeadLetterBinding) Handle(ctx context.Context, msg goredis.XMessage) error {
	now := b.clock.Now()
	env, p, err := events.DecodeConsumerDeadLetter(stringFields(msg.Values))
	if err == nil && !p.FailureKind.IsValid() {
		err = fmt.Errorf("failure_kind %q is not a dead_letter_kind", p.FailureKind)
	}
	if err != nil {
		return b.rec.Record(ctx, unreadable(deadletter.SourceConsumer, streams.ConsumerDeadLetterV1,
			streams.DeadLetterControllerConsumerDeadLetters, msg, err, now))
	}
	return b.rec.Record(ctx, deadletter.DeadLetter{
		ID: uuid.New(), DedupKey: deadletter.ConsumerKey(p.OriginalStream, p.OriginalGroup, p.OriginalMessageID),
		Source: deadletter.SourceConsumer, FailureKind: p.FailureKind, Stream: p.OriginalStream, Group: p.OriginalGroup,
		OriginalMessageID: p.OriginalMessageID, Producer: env.Producer, Error: p.Error, DeliveryCount: p.DeliveryCount,
		Fields: p.Fields, Redrivable: len(p.Fields) > 0, OriginalAt: idTimeOr(p.OriginalMessageID, env.OccurredAt),
		RecordedAt: now, Status: deadletter.StatusOpen,
	})
}
