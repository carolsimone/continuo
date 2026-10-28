package schedule

import (
	"errors"
	"fmt"
)

// Errors a schedule's callers translate for their own audience.
var (
	// ErrStaleVersion is returned by ScheduleRepository.Save when the stored
	// row is not at the version its caller names, 0 naming no row: another
	// edit saved first, the schedule was created since its caller found none,
	// or it was deleted since it was read. Nothing is written.
	ErrStaleVersion = errors.New("schedule was changed by another edit")

	// ErrStoredCronInvalid is returned by ScheduleRepository.Get when the
	// cron or timezone stored for a schedule no longer parses (a zone the
	// timezone database has dropped, or a row written by hand), as a
	// *StoredCronInvalidError. List and ListDue leave such a schedule out and
	// log it, and Save and Delete work on its row.
	ErrStoredCronInvalid = errors.New("stored cron or timezone does not parse")

	// ErrUnknownName is returned when a command would give a schedule name its
	// first row and no model of the live release carries that name as its
	// schedule tag.
	ErrUnknownName = errors.New("no model of the live release belongs to this schedule")
)

// StoredCronInvalidError reports a stored schedule whose cron or timezone no
// longer parses. It is ErrStoredCronInvalid to errors.Is, and it carries the
// version the row is at, so an edit that saves a new cron over the row can
// raise that version rather than start it again.
type StoredCronInvalidError struct {
	TenantID string
	Name     string
	Cron     string
	Timezone string
	// Version is the version the stored row is at.
	Version int64
	// Reason is why the parser refused the stored cron or timezone.
	Reason error
}

func (e *StoredCronInvalidError) Error() string {
	return fmt.Sprintf("read schedule %q of tenant %q: its stored cron %q in %q: %v: %v",
		e.Name, e.TenantID, e.Cron, e.Timezone, ErrStoredCronInvalid, e.Reason)
}

// Is reports whether target is ErrStoredCronInvalid.
func (e *StoredCronInvalidError) Is(target error) bool { return target == ErrStoredCronInvalid }

// Unwrap returns the parser's reason.
func (e *StoredCronInvalidError) Unwrap() error { return e.Reason }
