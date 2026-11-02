package ports

import "time"

// BackupSchedule is the times a tenant's graph is exported, in UTC.
type BackupSchedule interface {
	// Next returns the first slot after the given time, in UTC, or the zero
	// time when the schedule names no later slot.
	Next(after time.Time) time.Time
}

// BackupScheduleParser reads the text of a backup schedule, the value of
// catalog.backup.schedule.
type BackupScheduleParser interface {
	// Parse returns the schedule spec names, or the reason it cannot be a
	// backup schedule. Blanks around spec are ignored.
	Parse(spec string) (BackupSchedule, error)
}
