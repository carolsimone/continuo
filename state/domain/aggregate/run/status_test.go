package run_test

import (
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

func TestRunStatusSkipped_ValidAndTerminal(t *testing.T) {
	if !run.RunStatusSkipped.IsValid() {
		t.Error("skipped must be a valid run status")
	}
	if !run.RunStatusSkipped.IsTerminal() {
		t.Error("skipped must be a terminal run status")
	}
	if run.RunStatusSkipped != "skipped" {
		t.Errorf("wire value=%q, want %q", run.RunStatusSkipped, "skipped")
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

func TestTerminalRunStatuses_AreTheTerminalStatuses(t *testing.T) {
	want := map[run.RunStatus]bool{
		run.RunStatusSucceeded: true,
		run.RunStatusFailed:    true,
		run.RunStatusCancelled: true,
		run.RunStatusSkipped:   true,
	}
	got := run.TerminalRunStatuses()
	if len(got) != len(want) {
		t.Fatalf("TerminalRunStatuses() = %v, want exactly %v", got, want)
	}
	for _, s := range got {
		if !want[s] || !s.IsTerminal() {
			t.Fatalf("TerminalRunStatuses() contains %q, which is not terminal", s)
		}
	}
}

// A cancelling run still has work stopping, so it is active and not terminal:
// it keeps its schedule's single active slot until it ends cancelled.
func TestRunStatus_CancellingIsActiveAndNotTerminal(t *testing.T) {
	if !run.RunStatusCancelling.IsValid() {
		t.Error("cancelling must be a valid run status")
	}
	if run.RunStatusCancelling.IsTerminal() {
		t.Error("cancelling must not be a terminal run status")
	}
	if run.RunStatusCancelling != "cancelling" {
		t.Errorf("wire value=%q, want %q", run.RunStatusCancelling, "cancelling")
	}
	for _, s := range run.TerminalRunStatuses() {
		if s == run.RunStatusCancelling {
			t.Errorf("TerminalRunStatuses() contains %q", s)
		}
	}
	for _, s := range run.RunStatuses() {
		r := run.HydrateRun(pkgevents.DefaultTenantID, uuid.New(), "daily", s, run.InitStatusCompleted, run.KindCron, nil, "", "",
			time.Now(), nil, nil, nil, nil, nil, nil, nil, 0, nil)
		wantActive := s == run.RunStatusPending || s == run.RunStatusRunning ||
			s == run.RunStatusCancelling
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

// ActiveRunStatuses is exactly the statuses of the runs Run.IsActive
// reports active: pending, running and cancelling.
func TestActiveRunStatuses_AreTheStatusesOfTheActiveRuns(t *testing.T) {
	var want []run.RunStatus
	for _, s := range run.RunStatuses() {
		r := run.HydrateRun(pkgevents.DefaultTenantID, uuid.New(), "daily", s, run.InitStatusCompleted, run.KindCron, nil, "", "",
			time.Now(), nil, nil, nil, nil, nil, nil, nil, 0, nil)
		if r.IsActive() {
			want = append(want, s)
		}
	}
	if got := run.ActiveRunStatuses(); !slices.Equal(got, want) {
		t.Fatalf("ActiveRunStatuses() = %v, want %v", got, want)
	}
	if want := []run.RunStatus{run.RunStatusPending, run.RunStatusRunning, run.RunStatusCancelling}; !slices.Equal(run.ActiveRunStatuses(), want) {
		t.Fatalf("ActiveRunStatuses() = %v, want %v", run.ActiveRunStatuses(), want)
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

// TestRunStatuses_MatchTheContractVocabulary: the run aggregate lists exactly
// the run statuses the contract declares, in the contract's order.
func TestRunStatuses_MatchTheContractVocabulary(t *testing.T) {
	var want []string
	for _, s := range model.RunStatusValues() {
		want = append(want, string(s))
	}
	var got []string
	for _, s := range run.RunStatuses() {
		got = append(got, string(s))
	}
	if !slices.Equal(got, want) {
		t.Errorf("run.RunStatuses() = %v, contract = %v", got, want)
	}
}

// TestTaskStatuses_MatchTheContractVocabulary: the run aggregate lists exactly
// the task statuses the contract declares, in the contract's order.
func TestTaskStatuses_MatchTheContractVocabulary(t *testing.T) {
	var want []string
	for _, s := range model.TaskStatusValues() {
		want = append(want, string(s))
	}
	var got []string
	for _, s := range run.TaskStatuses() {
		got = append(got, string(s))
	}
	if !slices.Equal(got, want) {
		t.Errorf("run.TaskStatuses() = %v, contract = %v", got, want)
	}
}
