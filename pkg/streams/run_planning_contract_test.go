package streams_test

import (
	"os"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/pkg/streams"
)

// runPlanningRetired are the streams nothing produces or consumes once run-controller
// plans every run, dispatches ready tasks itself and skips the descendants of a
// failed task in the status-update transaction.
var runPlanningRetired = []string{
	"node.updated:v1",
	"run.entries.dispatch_failed:v1",
	"run.entries.dispatched:v1",
	"run.finalized:v1",
	"scheduler.started:v1",
	"trigger.promoted_seeds:v1",
	"trigger.rebase:v1",
	"trigger.rerun:v1",
	"trigger.single_node_run:v1",
}

// executionProtocolRetired are the streams nothing produces or consumes once
// run-controller dispatches each attempt on task.dispatch:v1 and execution-controller
// reports it on task.status.updated:v2, watching every Job itself.
var executionProtocolRetired = []string{
	"check.k8s:v1",
	"query.model:v1",
	"task.execution.recorded:v1",
	"task.status.updated:v1",
}

func TestRunPlanningStreams_AreRetired(t *testing.T) {
	for _, name := range slices.Concat(runPlanningRetired, executionProtocolRetired) {
		if _, live := streams.Groups[name]; live {
			t.Errorf("%s is still a live stream", name)
		}
		if !slices.Contains(streams.Retired, name) {
			t.Errorf("%s must be listed in retired_streams", name)
		}
	}
}

// TestRunDispatchStreams_HaveOneProducer pins who writes the two streams a
// run's tasks move on: run-controller dispatches every ready task, and
// execution-controller alone reports task statuses (no other service reports
// a skipped task).
func TestRunDispatchStreams_HaveOneProducer(t *testing.T) {
	raw, err := os.ReadFile("contract.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Streams []struct {
			Name      string   `yaml:"name"`
			Producers []string `yaml:"producers"`
		} `yaml:"streams"`
	}
	if err := yaml.Unmarshal(raw, &c); err != nil {
		t.Fatalf("parse contract.yaml: %v", err)
	}
	want := map[string][]string{
		streams.TaskDispatchV1:      {"run-controller"},
		streams.TaskStatusUpdatedV2: {"execution-controller"},
	}
	for _, s := range c.Streams {
		w, ok := want[s.Name]
		if !ok {
			continue
		}
		if !slices.Equal(s.Producers, w) {
			t.Errorf("%s producers = %v, want %v", s.Name, s.Producers, w)
		}
		delete(want, s.Name)
	}
	for name := range want {
		t.Errorf("%s is missing from contract.yaml", name)
	}
}

// TestRunCancelledV1_ExecutionControllerIsTheOnlyConsumer pins that a run's
// cancel reaches only execution-controller, which fences the run and stops its
// attempts.
func TestRunCancelledV1_ExecutionControllerIsTheOnlyConsumer(t *testing.T) {
	got := streams.Groups[streams.RunCancelledV1]
	if want := []string{streams.ExecutionControllerRunCancelled}; !slices.Equal(got, want) {
		t.Fatalf("%s groups = %v, want %v", streams.RunCancelledV1, got, want)
	}
}
