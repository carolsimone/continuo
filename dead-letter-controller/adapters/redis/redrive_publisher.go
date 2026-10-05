package redis

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/dead-letter-controller/serialization"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// RedrivePublisher publishes dead-letter-controller's outbox: a redrive row
// XADDs the stored fields to the original stream with redriven_from naming the
// dead letter and, for one group, redrive_group. A consumer or quarantine
// redrive carries the row's id as outbox_entry_id, so it is reprocessed; an
// outbox-source redrive preserves the original event's outbox_entry_id, so
// consumers that already processed it deduplicate it. A dead-letter row of this
// outbox publishes like any other service's.
type RedrivePublisher struct{ redis *goredis.Client }

var (
	_ pkgoutbox.Publisher = (*RedrivePublisher)(nil)
	_ pkgoutbox.Renderer  = (*RedrivePublisher)(nil)
)

func NewRedrivePublisher(rc *goredis.Client) *RedrivePublisher { return &RedrivePublisher{redis: rc} }

// Render returns the fields Publish adds to the stream, without outbox_entry_id.
// It returns a new map on every call.
func (p *RedrivePublisher) Render(entry *pkgoutbox.Entry) (map[string]any, error) {
	switch entry.EventType {
	case pkgoutbox.DeadLetterEventType:
		values, err := pkgoutbox.DeadLetterValues(entry)
		if err != nil {
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		delete(values, "outbox_entry_id")
		return values, nil
	case serialization.EventTypeRedrive:
		r, err := serialization.DecodeRedrive(entry.Payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}
		values := make(map[string]any, len(r.Fields)+2)
		for k, v := range r.Fields {
			values[k] = v
		}
		delete(values, "outbox_entry_id")
		values[pkgredis.RedrivenFromField] = r.RedrivenFrom
		if r.RedriveGroup != "" {
			values[pkgredis.RedriveGroupField] = r.RedriveGroup
		}
		return values, nil
	default:
		return nil, fmt.Errorf("%w: unknown event_type %q", pkgevents.ErrPermanent, entry.EventType)
	}
}

// stampedOutboxEntryID is the outbox_entry_id Publish stamps on entry: the
// original event's id for an outbox-source redrive, the entry's own id
// otherwise, so consumers deduplicate a republish of the same row.
func stampedOutboxEntryID(entry *pkgoutbox.Entry) (string, error) {
	if entry.EventType == serialization.EventTypeRedrive {
		r, err := serialization.DecodeRedrive(entry.Payload)
		if err != nil {
			return "", fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}
		if r.OutboxEntryID != "" {
			return r.OutboxEntryID, nil
		}
	}
	return entry.ID.String(), nil
}

// Publish XADDs the rendered fields to the entry's stream, stamping
// outbox_entry_id: the original event's id for an outbox-source redrive (so
// consumers that already processed it deduplicate it), the entry's id otherwise.
func (p *RedrivePublisher) Publish(ctx context.Context, entry *pkgoutbox.Entry) error {
	values, err := p.Render(entry)
	if err != nil {
		return err
	}
	id, err := stampedOutboxEntryID(entry)
	if err != nil {
		return err
	}
	values["outbox_entry_id"] = id
	if err := p.redis.XAdd(ctx, &goredis.XAddArgs{Stream: entry.StreamName, Values: values}).Err(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}
