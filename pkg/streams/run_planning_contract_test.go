package streams_test

import (
	"os"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/pkg/streams"
)

// TestRunDispatchStreams_HaveOneProducer pins who writes the two streams a
// run's tasks move on: run-controller dispatches every ready task, and
// execution-controller alone reports task statuses (no other component reports
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
		streams.TaskStatusUpdatedV1: {"execution-controller"},
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
