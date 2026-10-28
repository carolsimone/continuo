// Package schedule hosts the Schedule aggregate: the cron a schedule name runs
// on, in its own timezone, with the instant of its next fire. The aggregate
// reads a cron only through Expression, so it knows nothing of cron syntax,
// timezone databases or the libraries that parse them.
package schedule

import "time"

// maxCountedSkips bounds how many missed slots Due counts, and how many it
// walks one by one for an expression that is not an Interval. Past it the walk
// gives way to a search that costs a few dozen calls of Expression.Next however
// many slots were missed.
const maxCountedSkips = 1000

// Expression is a cron read in a timezone: the instants it fires at. Unless it
// is an Interval, those instants are the same whatever instant Next is asked
// from, so the latest of them before an instant can be searched for.
type Expression interface {
	// Next returns the first instant the cron fires at strictly after `after`,
	// or the zero time when it fires no more. Fires are whole seconds.
	Next(after time.Time) time.Time
}

// Interval is an Expression that fires every Period of elapsed time: for every
// instant s that Next returns, Next(s) is s + Period(). Its fire after an
// instant that is not one of its fires counts from that instant, so its fires
// lie on no grid of their own, and Due counts an Interval's slots from the
// stored next fire rather than search for them.
type Interval interface {
	Expression
	// Period is the elapsed time from one fire to the next.
	Period() time.Duration
}

// Schedule is the aggregate root for one row of the schedules table: the cron
// of a schedule name in a tenant, and when it fires next. Its identity is
// (tenant, name). Only an operator's edit (New, Replace, ReplaceUnreadable)
// changes its version, author and edit time; a fire moves the next fire alone.
type Schedule struct {
	tenantID    string
	name        string
	cron        string
	timezone    string
	description string
	paused      bool
	nextFireAt  *time.Time // nil while paused, or when the cron fires no more
	version     int64
	updatedBy   string
	updatedAt   time.Time
	expr        Expression
}

// New returns the schedule an operator creates at `now`: version 1, edited by
// `by`, with its next fire counted from `now`, and none while it is paused.
// expr is the parsed form of cron in timezone. A schedule with no tenant, no
// name or no expr is refused with ErrTenantRequired, ErrNameRequired or
// ErrExpressionRequired.
func New(tenantID, name, cron, timezone, description string, paused bool, expr Expression, by string, now time.Time) (*Schedule, error) {
	if err := validateIdentity(tenantID, name, expr); err != nil {
		return nil, err
	}
	s := &Schedule{tenantID: tenantID, name: name}
	s.edit(cron, timezone, description, paused, expr, by, now)
	return s, nil
}

// validateIdentity refuses a schedule that has no tenant, no name or no
// expression.
func validateIdentity(tenantID, name string, expr Expression) error {
	switch {
	case tenantID == "":
		return ErrTenantRequired
	case name == "":
		return ErrNameRequired
	case expr == nil:
		return ErrExpressionRequired
	}
	return nil
}

// ReplaceUnreadable returns the schedule an operator's edit at `now` by `by`
// makes of a stored row whose cron or timezone no longer parses, so cannot be
// rebuilt: the edit's cron, timezone, description and paused flag, with expr
// the parsed form of cron in timezone, at the version after storedVersion, the
// row's. Of the stored row only the identity and the version are kept. The
// next fire counts from now, and there is none while the schedule is paused. A
// schedule with no tenant, no name or no expr is refused as New refuses it.
func ReplaceUnreadable(
	tenantID, name string,
	storedVersion int64,
	cron, timezone, description string,
	paused bool,
	expr Expression,
	by string,
	now time.Time,
) (*Schedule, error) {
	if err := validateIdentity(tenantID, name, expr); err != nil {
		return nil, err
	}
	s := &Schedule{tenantID: tenantID, name: name, version: storedVersion}
	s.edit(cron, timezone, description, paused, expr, by, now)
	return s, nil
}

// Rehydrate restores a schedule from its stored fields. nextFireAt is nil when
// the row has none; a paused schedule has none whatever the row says. expr is
// the parsed form of cron in timezone, which the caller builds from the stored
// text.
func Rehydrate(
	tenantID, name, cron, timezone, description string,
	paused bool,
	nextFireAt *time.Time,
	version int64,
	updatedBy string,
	updatedAt time.Time,
	expr Expression,
) *Schedule {
	s := &Schedule{
		tenantID:    tenantID,
		name:        name,
		cron:        cron,
		timezone:    timezone,
		description: description,
		paused:      paused,
		version:     version,
		updatedBy:   updatedBy,
		updatedAt:   updatedAt,
		expr:        expr,
	}
	if !paused && nextFireAt != nil {
		next := *nextFireAt
		s.nextFireAt = &next
	}
	return s
}

// TenantID is the tenant the schedule belongs to.
func (s *Schedule) TenantID() string { return s.tenantID }

// Name is the schedule name the cron runs.
func (s *Schedule) Name() string { return s.name }

// Cron is the cron expression as the operator wrote it.
func (s *Schedule) Cron() string { return s.cron }

// Timezone is the IANA zone the cron is read in.
func (s *Schedule) Timezone() string { return s.timezone }

// Description says what the schedule is for; empty when it has none.
func (s *Schedule) Description() string { return s.description }

// Paused reports whether the operator paused the schedule.
func (s *Schedule) Paused() bool { return s.paused }

// Version counts the operator's edits, from 1.
func (s *Schedule) Version() int64 { return s.version }

// UpdatedBy is who made the latest edit.
func (s *Schedule) UpdatedBy() string { return s.updatedBy }

// UpdatedAt is when the latest edit was made.
func (s *Schedule) UpdatedAt() time.Time { return s.updatedAt }

// NextFireAt returns the instant of the next fire, and false when there is
// none: the schedule is paused, or its cron fires no more.
func (s *Schedule) NextFireAt() (time.Time, bool) {
	if s.nextFireAt == nil {
		return time.Time{}, false
	}
	return *s.nextFireAt, true
}

// Due reports the slot a pass at `now` fires: the latest slot at or before
// now, and how many earlier slots that fire leaves unfired. ok is false when
// the next fire is after now or there is none. The slots are those a walk with
// Expression.Next from the stored next fire reaches.
//
// Due counts the skipped slots up to 1000: a schedule that missed more (a
// per-minute cron after a long outage) reports 1000, a lower bound. The slots
// of an Interval with a Period above zero are counted from the stored next
// fire, in a few calls of Expression.Next however many were missed, so the slot
// that fires lies on the interval. Any other expression's slots are walked one
// by one up to the 1000th, and past it the latest is found by halving the span
// instead, so the cost of a pass stays about a thousand calls of
// Expression.Next however long the outage.
func (s *Schedule) Due(now time.Time) (slot time.Time, skipped int, ok bool) {
	if s.nextFireAt == nil || s.nextFireAt.After(now) {
		return time.Time{}, 0, false
	}
	if every, isInterval := s.expr.(Interval); isInterval && every.Period() > 0 {
		slot, skipped = latestIntervalSlot(every, *s.nextFireAt, now)
		return slot, skipped, true
	}
	slot = *s.nextFireAt
	for skipped < maxCountedSkips {
		next := s.expr.Next(slot)
		if !next.After(slot) || next.After(now) {
			return slot, skipped, true
		}
		slot = next
		skipped++
	}
	return s.latestSlot(slot, now), skipped, true
}

// latestIntervalSlot returns the latest slot at or before `now` of every, whose
// slots start at first, a slot at or before now, and how many slots come before
// it, up to maxCountedSkips. After first come the second slot, Next(first), and
// the instants a whole number of periods after the second. A span longer than a
// time.Duration holds, about 292 years, saturates; the slot it reaches is still
// one of the slots, and the loop steps on from there.
func latestIntervalSlot(every Interval, first, now time.Time) (time.Time, int) {
	second := every.Next(first)
	if !second.After(first) || second.After(now) {
		return first, 0
	}
	period := every.Period()
	slot, skipped := second, int64(1)
	for {
		steps := int64(now.Sub(slot) / period)
		if steps <= 0 {
			return slot, int(min(skipped, maxCountedSkips))
		}
		slot = slot.Add(time.Duration(steps) * period)
		skipped += steps
	}
}

// latestSlot returns the latest slot at or before `now`, given a slot `from`
// at or before it, by halving the span. It keeps a slot `lo` and a bound `hi`
// with no slot in (hi, now]: when a slot lies in the later half of (lo, hi] it
// becomes the new lo, and when none does, hi drops to the middle. Both ways the
// span at least halves, and the search ends when no slot follows lo before hi.
func (s *Schedule) latestSlot(from, now time.Time) time.Time {
	lo, hi := from, now
	for {
		next := s.expr.Next(lo)
		if !next.After(lo) || next.After(hi) {
			return lo
		}
		mid := lo.Add(hi.Sub(lo) / 2)
		if later := s.expr.Next(mid); later.After(mid) && !later.After(hi) {
			lo = later
		} else {
			hi = mid
		}
	}
}

// Fired records that the slot fired: the next fire becomes the first slot
// after it. The version, author and edit time stay as the operator left them.
// A paused schedule has no next fire and keeps none.
func (s *Schedule) Fired(slot time.Time) {
	if s.paused {
		return
	}
	s.setNextFireAfter(slot)
}

// Replace applies an operator's edit made at `now` by `by`: the new cron,
// timezone, description and paused flag, with expr the parsed form of cron in
// timezone. The next fire counts from now, and there is none while the
// schedule is paused. The version goes up by one.
func (s *Schedule) Replace(cron, timezone, description string, paused bool, expr Expression, by string, now time.Time) {
	s.edit(cron, timezone, description, paused, expr, by, now)
}

func (s *Schedule) edit(cron, timezone, description string, paused bool, expr Expression, by string, now time.Time) {
	s.cron = cron
	s.timezone = timezone
	s.description = description
	s.paused = paused
	s.expr = expr
	s.updatedBy = by
	s.updatedAt = now
	s.version++
	s.nextFireAt = nil
	if !paused {
		s.setNextFireAfter(now)
	}
}

func (s *Schedule) setNextFireAfter(after time.Time) {
	next := s.expr.Next(after)
	if next.IsZero() {
		s.nextFireAt = nil
		return
	}
	s.nextFireAt = &next
}
