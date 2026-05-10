package snapshot_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
)

func TestSourcePinnedDAG_TargetMissing_ReturnsErrTargetNotFound(t *testing.T) {
	srcID := uuid.New()
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {},
		},
	}
	sel := snapshot.SourcePinnedDAG{TargetService: "svc", TargetSchema: "sch", TargetTable: "missing"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if !errors.Is(err, snapshot.ErrTargetNotFound) { t.Fatalf("got %v", err) }
}

func TestSourcePinnedDAG_RebasesTargetAndNonSucceededDescendants_InheritsRest(t *testing.T) {
	srcID := uuid.New()
	target := snapshot.FQN{Service: "svc", Schema: "sch", Table: "tgt"}
	descSkipped := snapshot.FQN{Service: "svc", Schema: "sch", Table: "skip"}
	descSucceeded := snapshot.FQN{Service: "svc", Schema: "sch", Table: "ok"}
	unrelatedSucceeded := snapshot.FQN{Service: "svc", Schema: "sch", Table: "u"}

	rootTaskID := uuid.New()
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				target:             {TaskID: uuid.New(), Status: "FAILED",    ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
				descSkipped:        {TaskID: uuid.New(), Status: "SKIPPED",   ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
				descSucceeded:      {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
				unrelatedSucceeded: {TaskID: rootTaskID,   Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
			},
		},
		DescendantsSource: map[string]map[snapshot.FQN][]snapshot.FQN{
			srcID.String(): {target: {descSkipped, descSucceeded}},
		},
	}
	sel := snapshot.SourcePinnedDAG{TargetService: "svc", TargetSchema: "sch", TargetTable: "tgt"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err != nil { t.Fatal(err) }

	by := map[snapshot.FQN]snapshot.TaskProjection{}
	for _, p := range got { by[snapshot.FQN{Service: p.ServiceName, Schema: p.SchemaName, Table: p.TableName}] = p }

	if by[target].InitialStatus != "PENDING" || by[target].InheritedFromTaskID != nil { t.Errorf("target: %+v", by[target]) }
	if by[descSkipped].InitialStatus != "PENDING" || by[descSkipped].InheritedFromTaskID != nil { t.Errorf("skipped: %+v", by[descSkipped]) }
	if by[descSucceeded].InitialStatus != "SUCCEEDED" || by[descSucceeded].InheritedFromTaskID == nil { t.Errorf("succeeded desc: %+v", by[descSucceeded]) }
	if by[unrelatedSucceeded].InitialStatus != "SUCCEEDED" || by[unrelatedSucceeded].InheritedFromTaskID == nil ||
		*by[unrelatedSucceeded].InheritedFromTaskID != rootTaskID {
		t.Errorf("unrelated: %+v", by[unrelatedSucceeded])
	}
}

func TestSourcePinnedDAG_RootForwarding(t *testing.T) {
	srcID := uuid.New()
	target := snapshot.FQN{Service: "svc", Schema: "sch", Table: "tgt"}
	pinnedRoot := uuid.New()
	otherFQN := snapshot.FQN{Service: "svc", Schema: "sch", Table: "other"}

	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				target:   {TaskID: uuid.New(), Status: "FAILED",    ScheduleName: "x"},
				otherFQN: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x", InheritedFromRoot: &pinnedRoot},
			},
		},
	}
	sel := snapshot.SourcePinnedDAG{TargetService: "svc", TargetSchema: "sch", TargetTable: "tgt"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err != nil { t.Fatal(err) }
	for _, p := range got {
		if p.TableName == "other" {
			if p.InheritedFromTaskID == nil || *p.InheritedFromTaskID != pinnedRoot {
				t.Errorf("root not forwarded: %+v", p)
			}
		}
	}
}

func TestSourcePinnedDAG_NoSourceRunID_Errors(t *testing.T) {
	r := &fakeTopologyReader{}
	sel := snapshot.SourcePinnedDAG{TargetService: "s", TargetSchema: "c", TargetTable: "t"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{})
	if err == nil { t.Fatal("expected error") }
}

func TestSourcePinnedDAG_BlankTarget_Errors(t *testing.T) {
	srcID := uuid.New()
	r := &fakeTopologyReader{}
	sel := snapshot.SourcePinnedDAG{}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err == nil { t.Fatal("expected error") }
}
