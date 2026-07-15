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
