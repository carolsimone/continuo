package cron

import (
	"time"

	robfig "github.com/robfig/cron/v3"

	"github.com/carolsimone/continuo/state/domain/schedule"
)

const (
	secondsPerDay = 24 * 60 * 60

	// everyHour is robfig's hour field when every hour of the day is allowed:
	// one bit per hour.
	everyHour = 1<<24 - 1
)

// newExpression returns the expression of spec in loc.
//
// A cron that fires in every hour of the day (`0 * * * *`, `*/15 * * * *`,
// `@hourly`) and an `@every` interval recur on elapsed time. They follow the
// zone's clock as robfig's schedule does: the repeated hour of a clock change
// fires twice, the skipped hour not at all, and no fire is more than one
// interval of elapsed time after the last.
//
// Every other cron names the hours it fires in, so it fires at wall-clock times,
// and robfig's schedule does not give such a cron one fire per local day across
// a clock change: it fires twice on the day the clocks go back, at the same
// local time on both passes of the repeated hour, and not at all on the day
// they go forward, when its time does not exist. A wall-clock expression fires
// once on both days: at the first of the two instants a repeated time names,
// and, for a time that does not exist, at the first instant after the gap.
func newExpression(spec robfig.Schedule, loc *time.Location) schedule.Expression {
	if fields, ok := spec.(*robfig.SpecSchedule); ok && fields.Hour&everyHour != everyHour {
		return wallClockExpression{spec: fields, loc: loc}
	}
	return elapsedExpression{spec: spec, loc: loc}
}

// elapsedExpression fires as robfig's schedule does, on the clock of its zone.
type elapsedExpression struct {
	spec robfig.Schedule
	loc  *time.Location
}

// Next returns the first fire after `after`, in UTC.
func (e elapsedExpression) Next(after time.Time) time.Time {
	return e.spec.Next(after.In(e.loc)).UTC()
}

// wallClockExpression fires at the local times its cron names. It matches the
// cron against the zone's wall clock read as a UTC clock, which has no clock
// changes, and then places each local time on the timeline.
type wallClockExpression struct {
	spec *robfig.SpecSchedule
	loc  *time.Location
}

// Next returns the first fire after `after`, in UTC. A local time that does not
// exist fires at the instant the clocks go forward over it, and one that exists
// twice fires on its first pass.
func (e wallClockExpression) Next(after time.Time) time.Time {
	// Placing local times is monotonic, so every local time up to the clock
	// reading of `after` is placed at or before it, and the search starts there.
	// Local times of a repeated hour that follow the reading are placed on its
	// first pass, which may be before `after` too, so the search goes on until a
	// placed instant is after it.
	cursor := e.wallClock(after)
	for {
		wall := e.spec.Next(cursor)
		if wall.IsZero() {
			return time.Time{}
		}
		if at := e.place(wall); at.After(after) {
			return at
		}
		cursor = wall
	}
}

// wallClock returns the clock reading of the instant in the zone, as a UTC time
// with the same date and time of day.
func (e wallClockExpression) wallClock(instant time.Time) time.Time {
	local := instant.In(e.loc)
	return time.Date(local.Year(), local.Month(), local.Day(),
		local.Hour(), local.Minute(), local.Second(), local.Nanosecond(), time.UTC)
}

// place returns the instant of a local time, given as a UTC time with the same
// date and time of day. It is the time's only instant, the earlier of its two
// when the clocks went back over it, and the instant the clocks went forward
// over it when it does not exist.
func (e wallClockExpression) place(wall time.Time) time.Time {
	reading := wall.Unix()
	// The offset in force a day before and a day after bounds the offsets the
	// time can have: clock changes are weeks apart.
	_, before := time.Unix(reading-secondsPerDay, 0).In(e.loc).Zone()
	_, after := time.Unix(reading+secondsPerDay, 0).In(e.loc).Zone()

	var first int64
	found := false
	for _, offset := range [...]int{before, after} {
		instant := reading - int64(offset)
		if _, got := time.Unix(instant, 0).In(e.loc).Zone(); got != offset {
			continue
		}
		if !found || instant < first {
			first, found = instant, true
		}
	}
	if found {
		return time.Unix(first, 0).UTC()
	}
	// No offset puts the time on the clock: the clocks went forward over it.
	// The instant the offset before it would give is already past the change,
	// and the zone period it falls in starts at the change.
	start, _ := time.Unix(reading-int64(before), 0).In(e.loc).ZoneBounds()
	return start.UTC()
}
