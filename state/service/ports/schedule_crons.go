package ports

// ScheduleCron is the cron configuration of one schedule.
type ScheduleCron struct {
	// Expression is the cron expression the schedule fires on.
	Expression string
	// Description says what the schedule is for; empty when it has none.
	Description string
}

// ScheduleCrons gives the cron configuration the install declares for its
// schedules.
type ScheduleCrons interface {
	// Timezone is the IANA timezone every cron expression is read in; empty
	// when the install declares none.
	Timezone() string
	// CronOf returns the cron configuration of the named schedule, and false
	// when the install declares none for it.
	CronOf(scheduleName string) (ScheduleCron, bool)
}
