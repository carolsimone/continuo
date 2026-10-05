// Package redis holds dead-letter-controller's Redis adapters: the consumers of
// the two dead-letter streams and the publisher of redrives.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// Recorder stores one dead letter. A returned error means the dead letter is not
// stored.
type Recorder interface {
	Record(ctx context.Context, dl deadletter.DeadLetter) error
}

// stringFields copies a message's fields as strings.
func stringFields(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			out[k] = s
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}

// idTimeOr returns the time of stream id id, or fallback when id is not a stream id.
func idTimeOr(id string, fallback time.Time) time.Time {
	if t, ok := events.StreamIDTime(id); ok {
		return t
	}
	return fallback
}

// unreadable is the stored form of a dead-letter stream entry that could not be
// decoded: its raw fields, kept for inspection and never redriven.
func unreadable(source deadletter.Source, stream, group string, msg goredis.XMessage, cause error, now time.Time) deadletter.DeadLetter {
	return deadletter.DeadLetter{
		ID: uuid.New(), DedupKey: deadletter.UnreadableKey(stream, msg.ID), Source: source,
		FailureKind: model.DeadLetterKindPermanent, Stream: stream, Group: group, OriginalMessageID: msg.ID,
		Error: "unreadable dead letter: " + cause.Error(), Fields: stringFields(msg.Values), Redrivable: false,
		OriginalAt: idTimeOr(msg.ID, now), RecordedAt: now, Status: deadletter.StatusOpen,
	}
}
