package run

import (
	"context"
	"fmt"
)

// RecordTaskExecution applies an execution record to the attempt its Job name
// names: the digits of the name's "-r<n>" suffix, else attempt 0. It locks the
// task row before the attempt row, as a status transaction does, so a record
// and a status of the same attempt serialize in either order. A record that
// arrives before any status of its attempt creates the attempt running, or
// cancelled when the task is cancelled, so a cancelled run is left with no
// open attempt; it takes the task's dispatch time for attempt 0 and none for
// a later attempt, which the database clock stamps. The record sets the
// attempt's Job name and facts and never changes the status of an attempt
// that has a row.
//
// Returns ErrTaskNotHeld when no run holds the task.
func RecordTaskExecution(ctx context.Context, tasks TaskCollection, rec ExecutionRecord) error {
	task, exists, err := tasks.LoadAttemptState(ctx, rec.TaskID)
	if err != nil {
		return fmt.Errorf("load task attempt: %w", err)
	}
	if !exists {
		return ErrTaskNotHeld
	}
	n := AttemptFromJobName(rec.JobName)
	a, held, err := tasks.LoadAttempt(ctx, rec.TaskID, n)
	if err != nil {
		return fmt.Errorf("load attempt %d: %w", n, err)
	}
	if !held {
		a = Attempt{TaskID: rec.TaskID, Number: n, Status: AttemptRunning}
		if task.Status == TaskStatusCancelled {
			a.Status = AttemptCancelled
		}
		if n == 0 {
			a.DispatchedAt = task.DispatchedAt
		}
	}
	a.JobName = rec.JobName
	a.RecordExecution(rec)
	if err := tasks.SaveAttempt(ctx, a); err != nil {
		return fmt.Errorf("save attempt %d: %w", n, err)
	}
	return nil
}
