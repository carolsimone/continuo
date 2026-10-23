package run_test

import (
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

func TestSchedulerStatusSkipped_ValidAndTerminal(t *testing.T) {
	if !run.SchedulerStatusSkipped.IsValid() {
		t.Error("skipped must be a valid scheduler status")
	}
	if !run.SchedulerStatusSkipped.IsTerminal() {
		t.Error("skipped must be a terminal scheduler status")
	}
	if run.SchedulerStatusSkipped != "skipped" {
		t.Errorf("wire value=%q, want %q", run.SchedulerStatusSkipped, "skipped")
	}
}

// A cancelled task never runs again, so it is terminal: counting it as
// non-terminal would keep its run's terminal_task_count short of the total
// forever.
func TestTaskStatusCancelled_IsTerminal(t *testing.T) {
	if !run.TaskStatusCancelled.IsTerminal() {
		t.Error("cancelled must be a terminal task status")
	}
}

func TestTerminalSchedulerStatuses_AreTheTerminalStatuses(t *testing.T) {
	want := map[run.SchedulerStatus]bool{
		run.SchedulerStatusSucceeded: true,
		run.SchedulerStatusFailed:    true,
		run.SchedulerStatusCancelled: true,
		run.SchedulerStatusSkipped:   true,
	}
	got := run.TerminalSchedulerStatuses()
	if len(got) != len(want) {
		t.Fatalf("TerminalSchedulerStatuses() = %v, want exactly %v", got, want)
	}
	for _, s := range got {
		if !want[s] || !s.IsTerminal() {
			t.Fatalf("TerminalSchedulerStatuses() contains %q, which is not terminal", s)
		}
	}
}

// A cancelling run still has work stopping, so it is active and not terminal:
// it keeps its schedule's single active slot until it ends cancelled.
func TestSchedulerStatus_CancellingIsActiveAndNotTerminal(t *testing.T) {
	if !run.SchedulerStatusCancelling.IsValid() {
		t.Error("cancelling must be a valid scheduler status")
	}
	if run.SchedulerStatusCancelling.IsTerminal() {
		t.Error("cancelling must not be a terminal scheduler status")
	}
	if run.SchedulerStatusCancelling != "cancelling" {
		t.Errorf("wire value=%q, want %q", run.SchedulerStatusCancelling, "cancelling")
	}
	for _, s := range run.TerminalSchedulerStatuses() {
		if s == run.SchedulerStatusCancelling {
			t.Errorf("TerminalSchedulerStatuses() contains %q", s)
		}
	}
	for _, s := range run.SchedulerStatuses() {
		r := run.HydrateRun(uuid.New(), "daily", s, run.InitStatusCompleted, run.KindCron, nil, "", "",
			time.Now(), nil, nil, nil, nil, nil, nil, nil, 0, nil)
		wantActive := s == run.SchedulerStatusPending || s == run.SchedulerStatusRunning ||
			s == run.SchedulerStatusCancelling
		if r.IsActive() != wantActive {
			t.Errorf("IsActive() of a %s run = %v, want %v", s, r.IsActive(), wantActive)
		}
	}
}

// A stopping task has a stop requested and its attempt may still be running,
// so it has not ended.
func TestTaskStatus_StoppingIsNotTerminal(t *testing.T) {
	if !run.TaskStatusStopping.IsValid() {
		t.Error("stopping must be a valid task status")
	}
	if run.TaskStatusStopping.IsTerminal() {
		t.Error("stopping must not be a terminal task status")
	}
	if !slices.Contains(run.TaskStatuses(), run.TaskStatusStopping) {
		t.Error("TaskStatuses() must list stopping")
	}
	if run.TaskStatusStopping != "stopping" {
		t.Errorf("wire value=%q, want %q", run.TaskStatusStopping, "stopping")
	}
}

// ActiveSchedulerStatuses is exactly the statuses of the runs Run.IsActive
// reports active: pending, running and cancelling.
func TestActiveSchedulerStatuses_AreTheStatusesOfTheActiveRuns(t *testing.T) {
	var want []run.SchedulerStatus
	for _, s := range run.SchedulerStatuses() {
		r := run.HydrateRun(uuid.New(), "daily", s, run.InitStatusCompleted, run.KindCron, nil, "", "",
			time.Now(), nil, nil, nil, nil, nil, nil, nil, 0, nil)
		if r.IsActive() {
			want = append(want, s)
		}
	}
	if got := run.ActiveSchedulerStatuses(); !slices.Equal(got, want) {
		t.Fatalf("ActiveSchedulerStatuses() = %v, want %v", got, want)
	}
	if want := []run.SchedulerStatus{run.SchedulerStatusPending, run.SchedulerStatusRunning, run.SchedulerStatusCancelling}; !slices.Equal(run.ActiveSchedulerStatuses(), want) {
		t.Fatalf("ActiveSchedulerStatuses() = %v, want %v", run.ActiveSchedulerStatuses(), want)
	}
}

// InFlightTaskStatuses is exactly the task statuses with an attempt in
// flight, none of them terminal: dispatched, queued, running and stopping.
func TestInFlightTaskStatuses_AreTheStatusesWithAnAttemptInFlight(t *testing.T) {
	var want []run.TaskStatus
	for _, s := range run.TaskStatuses() {
		if s.IsInFlight() {
			if s.IsTerminal() {
				t.Errorf("%s is in flight and terminal", s)
			}
			want = append(want, s)
		}
	}
	if got := run.InFlightTaskStatuses(); !slices.Equal(got, want) {
		t.Fatalf("InFlightTaskStatuses() = %v, want %v", got, want)
	}
	if want := []run.TaskStatus{run.TaskStatusDispatched, run.TaskStatusQueued, run.TaskStatusRunning, run.TaskStatusStopping}; !slices.Equal(run.InFlightTaskStatuses(), want) {
		t.Fatalf("InFlightTaskStatuses() = %v, want %v", run.InFlightTaskStatuses(), want)
	}
}
