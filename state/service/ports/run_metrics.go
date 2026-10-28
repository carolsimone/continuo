package ports

import "time"

// RunMetrics counts what state's reconcile loop, report handler, lease waker
// and cron loop do to runs.
type RunMetrics interface {
	// RunStalled counts one run failed as stalled.
	RunStalled()

	// RunsCancelling sets the gauges of the runs that are cancelling: how many
	// there are and how many of them have waited too long for their attempts
	// to stop.
	RunsCancelling(total, overdue int)

	// RunCancelled observes how long a run took from its cancel to its end.
	RunCancelled(after time.Duration)

	// LeaseWaiters sets the gauge of the tasks waiting for a node's write
	// lease.
	LeaseWaiters(n int)

	// LeaseGranted observes how long a task waited for the node lease it was
	// granted.
	LeaseGranted(waited time.Duration)

	// LeaseRepaired counts one lease deleted because the task that held it had
	// ended without releasing it.
	LeaseRepaired()

	// CommandRequest counts one call of command by its outcome: created (it ran
	// and stored its request), replayed (a repeat answered from the stored
	// request), conflict (a request id reused with other parameters) or no_run
	// (it ran but acted on no run, so nothing was stored).
	CommandRequest(command, outcome string)

	// CronFire counts one outcome of a schedule's fire: started (it started a
	// run), skipped_active (the schedule's previous run is still active),
	// skipped_maintenance (maintenance mode is on), empty_selection (it
	// recorded a failed run, the live release giving it nothing to run), or
	// missed (one older slot the fire of the latest one left unfired; a fire
	// counts one per such slot).
	CronFire(outcome string)

	// Schedules sets the gauges of the schedules of every tenant: how many
	// are scheduled and how many are paused.
	Schedules(scheduled, paused int)
}
