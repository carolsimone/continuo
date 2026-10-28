package schedule

import "errors"

// Errors a schedule's callers translate for their own audience.
var (
	// ErrStaleVersion is returned by ScheduleRepository.Save when its caller
	// names a version other than the stored row's: another edit saved first,
	// and nothing is written.
	ErrStaleVersion = errors.New("schedule was changed by another edit")

	// ErrUnknownName is returned when a command would give a schedule name its
	// first row and no model of the live release carries that name as its
	// schedule tag.
	ErrUnknownName = errors.New("no model of the live release belongs to this schedule")
)
