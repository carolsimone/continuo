package streams_test

import (
	"os"
	"strings"
	"testing"
)

// frozenGroups are consumer-group strings that are live Redis state. A group
// is created at offset 0, so a renamed group replays the retained stream
// history (up to StreamMaxLen entries) against an empty dedup table. These
// strings therefore never change, whatever the owning service is called.
var frozenGroups = []string{
	"executor-query-model", "executor-retry", "executor-schedule-cancelled",
	"executor-validation-requested", "executor-validation-node-completed",
	"executor-seed-build-requested", "executor-seed-build-node-completed",
	"executor-compile-requested", "executor-compile-node-completed",
	"executor-validation-result-teardown", "executor-pipeline-run-finished",
	"executor-release-promoted", "executor-release-rejected",
	"k8s-deployed", "k8s-check-status",
}

func TestFrozenConsumerGroupsStillDeclared(t *testing.T) {
	raw, err := os.ReadFile("contract.yaml")
	if err != nil {
		t.Fatal(err)
	}
	contract := string(raw)
	for _, g := range frozenGroups {
		if !strings.Contains(contract, "group: "+g+"\n") {
			t.Errorf("consumer group %q is missing from contract.yaml: renaming a group creates a new group at offset 0 and replays the stream history; keep the string and change only the service field", g)
		}
	}
}
