package catalog

import "errors"

// ErrEmptyReconciliation is returned by ScheduleCatalog.Reconcile when the
// payload's schedule_names list is empty. Without this guard a buggy
// topology-controller could publish [] and silently soft-delete every active
// catalog row.
var ErrEmptyReconciliation = errors.New(
	"empty schedule_names list in reconciliation payload — refusing to nuke catalog",
)
