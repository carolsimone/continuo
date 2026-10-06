package run_test

import (
	"testing"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
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
