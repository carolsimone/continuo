package model

// DerivedRunInput is the kind-agnostic handler input shared by rerun and
// rebase. Both triggers mint a new :Run on the source's schedule and drive
// Snapshot with a kind-specific selector; the only data they carry is the new
// run's identity plus the source run to read state from. The owning
// DerivedRunHandler supplies the selector, kind label, and stream name, so the
// two triggers reduce to one handler over this common input.
type DerivedRunInput struct {
	ScheduleName string
	RunID        string // schedule_id of the NEW run (target of Snapshot)
	SourceRunID  string // schedule_id of the SOURCE run (read by the selector)
}
