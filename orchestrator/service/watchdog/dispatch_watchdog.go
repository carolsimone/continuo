// Package watchdog detects schedules whose dispatch has silently stalled
// and terminates them via the cancellation pathway.
package watchdog

import (
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// IsScheduleStuck returns true iff both predicates hold:
//
//  1. No task is currently in TASK_STATUS_RUNNING. A running task means a
//     long-running model is in flight — that's not stuck.
//  2. The most recent task's created_at is older than noProgressFor. The
//     state proto Task message has no updated_at, so created_at is the
//     only "progress" signal available — and it's enough for the bug we
//     care about: tasks created, dispatcher silently failed, no RUNNING
//     transition ever happens.
//
// Empty input returns false — a schedule with no tasks isn't stuck, it
// just hasn't started.
func IsScheduleStuck(tasks []*statev1.Task, now time.Time, noProgressFor time.Duration) bool {
	if len(tasks) == 0 {
		return false
	}
	mostRecent := time.Time{}
	for _, t := range tasks {
		if t.GetStatus() == statev1.TaskStatus_TASK_STATUS_RUNNING {
			return false
		}
		ts := t.GetCreatedAt().AsTime()
		if ts.After(mostRecent) {
			mostRecent = ts
		}
	}
	return now.Sub(mostRecent) >= noProgressFor
}
