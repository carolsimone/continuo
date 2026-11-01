// Package croninfra reads the backup schedule with robfig/cron.
package croninfra

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/robfig/cron/v3"
)

// minInterval is the shortest @every interval a schedule may name: each
// export pauses the tenant's writes while it runs.
const minInterval = time.Minute

// neverFiresProbe is the instant a schedule must fire after to be taken as
// firing at all; the schedules that fail it name a date no calendar has.
var neverFiresProbe = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// BackupScheduleParser reads a backup schedule: five cron fields or an @
// descriptor that robfig/cron's standard parser reads, in UTC. It refuses a
// CRON_TZ= or TZ= prefix, an @every under a minute and a schedule no date
// satisfies.
type BackupScheduleParser struct{}

var _ ports.BackupScheduleParser = BackupScheduleParser{}

// Parse returns the schedule spec names, or why spec cannot be a backup
// schedule. Blanks around spec are ignored.
func (BackupScheduleParser) Parse(spec string) (ports.BackupSchedule, error) {
	text := strings.TrimSpace(spec)
	if text == "" {
		return nil, errors.New("the schedule is empty")
	}
	if strings.HasPrefix(text, "CRON_TZ=") || strings.HasPrefix(text, "TZ=") {
		return nil, errors.New("the schedule runs in UTC and must not start with CRON_TZ= or TZ=")
	}
	schedule, err := cron.ParseStandard(text)
	if err != nil {
		return nil, err
	}
	if every, ok := schedule.(cron.ConstantDelaySchedule); ok && every.Delay < minInterval {
		return nil, fmt.Errorf("an @every interval is at least %s", minInterval)
	}
	if schedule.Next(neverFiresProbe).IsZero() {
		return nil, errors.New("no date satisfies the schedule")
	}
	return utcSchedule{schedule}, nil
}

// utcSchedule reads every time as UTC: robfig/cron matches the fields in the
// zone of the time it is given, so the time is converted first.
type utcSchedule struct{ schedule cron.Schedule }

func (s utcSchedule) Next(after time.Time) time.Time {
	return s.schedule.Next(after.UTC())
}
