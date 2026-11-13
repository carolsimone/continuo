package conformance

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// outboxRepository is the in-memory outbox.Repository. The runner and the
// proposal driver only write entries; the relay's methods return
// errors.ErrUnsupported.
type outboxRepository struct {
	gate gate
}

var _ outbox.Repository = (*outboxRepository)(nil)

// Create stores a copy of entry. Like the Postgres repository it gives the
// entry an id when it has none, stamps its creation time, defaults its status
// to pending, and requires the message-processing row it names to exist.
func (r *outboxRepository) Create(_ context.Context, entry *outbox.Entry) error {
	return r.gate(func(d *data) error {
		if entry.ID == uuid.Nil {
			entry.ID = uuid.New()
		}
		for _, e := range d.outbox {
			if e.ID == entry.ID {
				return fmt.Errorf("create outbox entry %s: already exists", entry.ID)
			}
		}
		if entry.MessageProcessingID != nil && d.findMessageByID(*entry.MessageProcessingID) == nil {
			return fmt.Errorf("create outbox entry %s: message_processing %s does not exist", entry.ID, *entry.MessageProcessingID)
		}
		entry.CreatedAt = time.Now()
		if entry.Status == "" {
			entry.Status = "pending"
		}
		stored := cloneOutboxEntry(*entry)
		d.outbox = append(d.outbox, &stored)
		return nil
	})
}

func (r *outboxRepository) GetPendingBatch(context.Context, int) ([]*outbox.Entry, error) {
	return nil, errors.ErrUnsupported
}

func (r *outboxRepository) MarkProcessed(context.Context, uuid.UUID) error {
	return errors.ErrUnsupported
}

func (r *outboxRepository) MarkProcessedBatch(context.Context, []uuid.UUID) error {
	return errors.ErrUnsupported
}

func (r *outboxRepository) MarkFailed(context.Context, uuid.UUID, string) error {
	return errors.ErrUnsupported
}

func (r *outboxRepository) IncrementRetry(context.Context, uuid.UUID) error {
	return errors.ErrUnsupported
}

// messageRepository is the in-memory messageprocessing.Repository.
type messageRepository struct {
	gate gate
}

var _ messageprocessing.Repository = (*messageRepository)(nil)

func (d *data) findMessageByID(id uuid.UUID) *messageprocessing.MessageProcessing {
	for _, m := range d.messages {
		if m.ID == id {
			return m
		}
	}
	return nil
}

// conflicts reports whether msg collides with m on either unique key of the
// table: (message_id, stream_name), or (outbox_entry_id, stream_name) when both
// carry an outbox entry id.
func conflicts(m, msg *messageprocessing.MessageProcessing) bool {
	if m.MessageID == msg.MessageID && m.StreamName == msg.StreamName {
		return true
	}
	return m.OutboxEntryID != nil && msg.OutboxEntryID != nil &&
		*m.OutboxEntryID == *msg.OutboxEntryID && m.StreamName == msg.StreamName
}

// InsertIfNotExists inserts msg unless a row collides with it on either dedup
// axis, and then returns the colliding row's id with inserted false. As the
// Postgres repository does when it looks the row up after a conflict, it
// prefers the row with the same message id and stream, and otherwise takes a
// row with the same outbox entry id on any stream.
func (r *messageRepository) InsertIfNotExists(_ context.Context, msg *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
	var (
		id       uuid.UUID
		inserted bool
	)
	err := r.gate(func(d *data) error {
		var collided bool
		for _, m := range d.messages {
			if conflicts(m, msg) {
				collided = true
				break
			}
		}
		if !collided {
			now := time.Now()
			stored := cloneMessage(*msg)
			stored.ID = uuid.New()
			stored.CreatedAt, stored.UpdatedAt = now, now
			d.messages = append(d.messages, &stored)
			id, inserted = stored.ID, true
			return nil
		}
		for _, m := range d.messages {
			if m.MessageID == msg.MessageID && m.StreamName == msg.StreamName {
				id = m.ID
				return nil
			}
		}
		for _, m := range d.messages {
			if m.OutboxEntryID != nil && msg.OutboxEntryID != nil && *m.OutboxEntryID == *msg.OutboxEntryID {
				id = m.ID
				return nil
			}
		}
		return errors.New("get existing message after conflict: not found")
	})
	return id, inserted, err
}

// AlreadyProcessed reports whether a row exists on either dedup axis within the
// consumer's stream.
func (r *messageRepository) AlreadyProcessed(_ context.Context, messageID, streamName string, outboxEntryID *uuid.UUID) (bool, error) {
	var found bool
	err := r.gate(func(d *data) error {
		for _, m := range d.messages {
			if m.StreamName != streamName {
				continue
			}
			if m.MessageID == messageID || (m.OutboxEntryID != nil && outboxEntryID != nil && *m.OutboxEntryID == *outboxEntryID) {
				found = true
				return nil
			}
		}
		return nil
	})
	return found, err
}

func (r *messageRepository) GetByMessageIDAndStream(_ context.Context, messageID, streamName string) (*messageprocessing.MessageProcessing, error) {
	var out *messageprocessing.MessageProcessing
	err := r.gate(func(d *data) error {
		for _, m := range d.messages {
			if m.MessageID == messageID && m.StreamName == streamName {
				c := cloneMessage(*m)
				out = &c
				return nil
			}
		}
		return fmt.Errorf("message not found: %s on stream %s", messageID, streamName)
	})
	return out, err
}

func (r *messageRepository) GetByID(_ context.Context, id uuid.UUID) (*messageprocessing.MessageProcessing, error) {
	var out *messageprocessing.MessageProcessing
	err := r.gate(func(d *data) error {
		m := d.findMessageByID(id)
		if m == nil {
			return fmt.Errorf("message_processing row not found: %s", id)
		}
		c := cloneMessage(*m)
		out = &c
		return nil
	})
	return out, err
}

func (r *messageRepository) UpdateState(_ context.Context, id uuid.UUID, state string) error {
	return r.gate(func(d *data) error {
		m := d.findMessageByID(id)
		if m == nil {
			return fmt.Errorf("no message_processing row with id %s", id)
		}
		m.State = state
		m.UpdatedAt = time.Now()
		return nil
	})
}

func (r *messageRepository) DeleteOlderThan(context.Context, time.Duration, int) (int64, error) {
	return 0, errors.ErrUnsupported
}
