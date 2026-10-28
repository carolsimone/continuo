package run

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

func TestNewPendingRun_StoresOperation(t *testing.T) {
	r, err := NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "sched", KindTrigger, nil, "user-1", model.OperationTest, ReleaseStamp{}, time.Now())
	if err != nil {
		t.Fatalf("NewPendingRun: %v", err)
	}
	if r.Operation() != model.OperationTest {
		t.Fatalf("Operation() = %q, want test", r.Operation())
	}
}

func TestNewSingleNodeRun_StoresOperation(t *testing.T) {
	r, err := NewSingleNodeRun(pkgevents.DefaultTenantID, uuid.New(), "sched", model.OperationBuild, nil, "user-1", ReleaseStamp{}, time.Now())
	if err != nil {
		t.Fatalf("NewSingleNodeRun: %v", err)
	}
	if r.Operation() != model.OperationBuild {
		t.Fatalf("Operation() = %q, want build", r.Operation())
	}
}

func TestNewDerivedRun_InheritsOperation(t *testing.T) {
	r, err := NewDerivedRun(pkgevents.DefaultTenantID, uuid.New(), "sched", KindRerun, uuid.New(), ReleaseStamp{}, "user-1", model.OperationBuild, time.Now())
	if err != nil {
		t.Fatalf("NewDerivedRun: %v", err)
	}
	if r.Operation() != model.OperationBuild {
		t.Fatalf("Operation() = %q, want build", r.Operation())
	}

	r, err = NewDerivedRun(pkgevents.DefaultTenantID, uuid.New(), "sched", KindRerun, uuid.New(), ReleaseStamp{}, "user-1", model.OperationRun, time.Now())
	if err != nil {
		t.Fatalf("NewDerivedRun: %v", err)
	}
	if r.Operation() != model.OperationRun {
		t.Fatalf("Operation() = %q, want run(empty)", r.Operation())
	}
}

func TestHydrateRun_RoundTripsOperation(t *testing.T) {
	r := HydrateRun(pkgevents.DefaultTenantID, uuid.New(), "sched", RunStatusRunning, KindTrigger, nil, "user-1", model.OperationTest, time.Now(),
		nil, nil, nil, nil, nil, nil, nil, 0)
	if r.Operation() != model.OperationTest {
		t.Fatalf("Operation() = %q, want test", r.Operation())
	}
}
