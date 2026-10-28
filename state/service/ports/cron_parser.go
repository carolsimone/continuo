package ports

import "github.com/carolsimone/continuo/state/domain/schedule"

// CronParser reads the cron an operator writes. Both methods fail only because
// their input is refused, and the error says why in words fit to show the
// operator.
type CronParser interface {
	// Parse reads cron in the IANA timezone and returns the expression that
	// says when it fires. It refuses a cron that carries its own zone (a
	// `CRON_TZ=` or `TZ=` prefix), a zone it does not know, and a cron that
	// never fires.
	Parse(cron, timezone string) (schedule.Expression, error)
	// Describe returns the cron in English words, such as "At 01:00".
	Describe(cron string) (string, error)
}
