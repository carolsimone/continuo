package run

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

func TestNewPendingRun_StoresOperation(t *testing.T) {
	r, _, err := NewPendingRun("sched", KindTrigger, nil, "user-1", nil, model.OperationTest, time.Now())
	if err != nil {
		t.Fatalf("NewPendingRun: %v", err)
	}
	if r.Operation() != model.OperationTest {
		t.Fatalf("Operation() = %q, want test", r.Operation())
	}
}

func TestNewSingleNodeRun_StoresOperation(t *testing.T) {
	r, _, err := NewSingleNodeRun("sched", NodeID{ServiceName: "s", SchemaName: "sc", TableName: "t"},
		MetadataSource("latest"), model.OperationBuild, nil, "user-1", time.Now())
	if err != nil {
		t.Fatalf("NewSingleNodeRun: %v", err)
	}
	if r.Operation() != model.OperationBuild {
		t.Fatalf("Operation() = %q, want build", r.Operation())
	}
}

func TestNewDerivedRun_DefaultsToRunOperation(t *testing.T) {
	r, _, err := NewDerivedRun("sched", KindRerun, uuid.New(), "user-1", time.Now())
	if err != nil {
		t.Fatalf("NewDerivedRun: %v", err)
	}
	if r.Operation() != model.OperationRun {
		t.Fatalf("Operation() = %q, want run(empty)", r.Operation())
	}
}

func TestHydrateRun_RoundTripsOperation(t *testing.T) {
	r := HydrateRun(uuid.New(), "sched", SchedulerStatusRunning, InitStatusCompleted,
		KindTrigger, nil, "user-1", model.OperationTest, time.Now(),
		nil, nil, nil, nil, nil, nil, nil, 0, nil)
	if r.Operation() != model.OperationTest {
		t.Fatalf("Operation() = %q, want test", r.Operation())
	}
}
