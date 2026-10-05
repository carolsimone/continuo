// Package deadletter models a stored dead letter: a stream message, outbox row
// or trimmed stream entry that a consumer group never finished, kept so an
// operator can inspect it and redrive it within the replay horizon.
package deadletter

import (
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// Source is where a dead letter came from.
type Source string

const (
	// SourceConsumer: a stream consumer gave up on a message.
	SourceConsumer Source = "consumer"
	// SourceOutbox: an outbox relay gave up publishing a row.
	SourceOutbox Source = "outbox"
	// SourceQuarantine: the retention cap trimmed an entry a group still needed.
	SourceQuarantine Source = "quarantine"
)

// IsValid reports whether s is a known source.
func (s Source) IsValid() bool {
	return s == SourceConsumer || s == SourceOutbox || s == SourceQuarantine
}

// Status is whether a dead letter is waiting for an operator or was redriven.
type Status string

const (
	StatusOpen     Status = "open"
	StatusRedriven Status = "redriven"
)

// IsValid reports whether s is a known status.
func (s Status) IsValid() bool { return s == StatusOpen || s == StatusRedriven }

var (
	ErrNotFound       = errors.New("dead letter not found")
	ErrExpired        = errors.New("dead letter is past the replay horizon")
	ErrNotRedrivable  = errors.New("dead letter has no fields to redrive")
	ErrReasonRequired = errors.New("a redrive reason is required")
	ErrNoIDs          = errors.New("at least one dead letter id is required")
	ErrInvalidFilter  = errors.New("invalid dead letter filter")
)

// Redrive records who redrove a dead letter, why, and when.
type Redrive struct {
	Actor  string
	Reason string
	At     time.Time
}

// DeadLetter is one stored dead letter. Fields are the stream fields a redrive
// publishes; Redrivable is false when they are not a publishable message (an
// unreadable dead letter, or an outbox row that could not be rendered). Group
// is empty for an outbox dead letter, whose event was never published to any
// group. OriginalAt is when the original message was produced.
type DeadLetter struct {
	ID                uuid.UUID
	DedupKey          string
	Source            Source
	FailureKind       model.DeadLetterKind
	Stream            string
	Group             string
	OriginalMessageID string
	Producer          string
	Error             string
	DeliveryCount     int64
	Fields            map[string]string
	Redrivable        bool
	OriginalEventType string
	FailedOutboxID    string
	OriginalAt        time.Time
	RecordedAt        time.Time
	Status            Status
	Redrive           *Redrive
}

// ExpiresAt is when the dead letter leaves the replay horizon.
func (d DeadLetter) ExpiresAt() time.Time { return d.OriginalAt.Add(model.ReplayHorizon) }

// CheckRedrive returns why d cannot be redriven at now, or nil. An already
// redriven dead letter returns nil: redriving it again is a no-op.
func (d DeadLetter) CheckRedrive(now time.Time) error {
	if d.Status == StatusRedriven {
		return nil
	}
	if !d.Redrivable {
		return ErrNotRedrivable
	}
	if !now.Before(d.ExpiresAt()) {
		return fmt.Errorf("%w (original message from %s)", ErrExpired, d.OriginalAt.UTC().Format(time.RFC3339))
	}
	return nil
}

// MarkRedriven records the redrive and reports whether the status changed. A
// dead letter that is already redriven keeps its first redrive record.
func (d *DeadLetter) MarkRedriven(actor, reason string, now time.Time) bool {
	if d.Status == StatusRedriven {
		return false
	}
	d.Status = StatusRedriven
	d.Redrive = &Redrive{Actor: actor, Reason: reason, At: now}
	return true
}

// TargetGroup is the consumer group a redrive is addressed to, or "" when every
// group of the stream should receive it (an outbox event that was never
// published).
func (d DeadLetter) TargetGroup() string {
	if d.Source == SourceOutbox {
		return ""
	}
	return d.Group
}

// Filter selects dead letters to list. Empty Source and Stream match all;
// Status defaults to open; Limit defaults to 50 and is capped at 500.
type Filter struct {
	Source Source
	Stream string
	Status Status
	Limit  int
}

const (
	defaultListLimit = 50
	maxListLimit     = 500
)

// Normalize applies the defaults and validates the filter.
func (f Filter) Normalize() (Filter, error) {
	if f.Source != "" && !f.Source.IsValid() {
		return Filter{}, fmt.Errorf("%w: source %q (want consumer, outbox or quarantine)", ErrInvalidFilter, f.Source)
	}
	if f.Status == "" {
		f.Status = StatusOpen
	}
	if !f.Status.IsValid() {
		return Filter{}, fmt.Errorf("%w: status %q (want open or redriven)", ErrInvalidFilter, f.Status)
	}
	switch {
	case f.Limit <= 0:
		f.Limit = defaultListLimit
	case f.Limit > maxListLimit:
		f.Limit = maxListLimit
	}
	return f, nil
}

// BacklogRow counts the open dead letters of one source, stream and kind.
type BacklogRow struct {
	Source           Source
	Stream           string
	Kind             model.DeadLetterKind
	Open             int64
	OldestRecordedAt time.Time
}
