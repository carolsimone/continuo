package streams_test

import (
	"os"
	"strings"
	"testing"
)

// frozenGroups are consumer-group strings that are live Redis state. A group
// is created at offset 0, so a renamed group replays the retained stream
// history (everything the dead-letter-controller's trim loop has not yet
// trimmed) against an empty dedup table. These strings therefore never change,
// whatever the owning service is called.
var frozenGroups = []string{
	"executor-task-dispatch", "executor-run-cancelled",
	"executor-validation-requested",
	"executor-seed-build-requested",
	"executor-compile-requested",
	"executor-validation-result-teardown", "executor-pipeline-run-finished",
	"executor-release-promoted", "executor-release-rejected",
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

// droppedGroups are consumer-group strings that went with the stream they read
// when the stream was retired. Their stream's key is deleted from Redis, so the
// group holds no state to keep; declaring the string again, on any stream,
// would make a new group look like the one that was dropped.
var droppedGroups = []string{
	"state-schedule-catalog",
}

func TestDroppedConsumerGroupsAreNotDeclared(t *testing.T) {
	raw, err := os.ReadFile("contract.yaml")
	if err != nil {
		t.Fatal(err)
	}
	contract := string(raw)
	for _, g := range droppedGroups {
		if strings.Contains(contract, "group: "+g+"\n") {
			t.Errorf("consumer group %q is declared in contract.yaml, but it was dropped with its retired stream; name a new group instead", g)
		}
	}
}
