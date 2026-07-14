package snapshot_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
)

func TestRebasePartition_RebasesNonSucceededAndDescendants_InheritsSucceeded(t *testing.T) {
	srcID := uuid.New()
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"} // failed → rebase
	b := snapshot.FQN{Service: "svc", Schema: "sch", Table: "b", ScheduleName: "x"} // descendant of a → rebase
	c := snapshot.FQN{Service: "svc", Schema: "sch", Table: "c", ScheduleName: "x"} // unrelated SUCCEEDED → inherit

	rootC := uuid.New()
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				a: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				b: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model"},
				c: {TaskID: rootC, Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model"},
			},
		},
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			a: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v2", ManifestVersion: "m2"},
			b: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v2", ManifestVersion: "m2"},
			c: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v2", ManifestVersion: "m2"},
		},
		DescendantsLatest:    map[snapshot.FQN][]snapshot.FQN{a: {b}},
		ImmDescendantsLatest: map[snapshot.FQN][]snapshot.FQN{a: {b}},
	}
	got, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, ScheduleName: "x"})
	if err != nil {
		t.Fatal(err)
	}

	by := map[snapshot.FQN]snapshot.TaskProjection{}
	for _, p := range got {
		by[snapshot.FQN{Service: p.ServiceName, Schema: p.SchemaName, Table: p.TableName, ScheduleName: p.ScheduleName}] = p
	}
	if by[a].InitialStatus != "PENDING" {
		t.Errorf("a: %+v", by[a])
	}
	if by[b].InitialStatus != "PENDING" {
		t.Errorf("b (descendant of failed a): %+v", by[b])
	}
	if by[c].InitialStatus != "SUCCEEDED" || by[c].InheritedFromTaskID == nil || *by[c].InheritedFromTaskID != rootC {
		t.Errorf("c (inherit, root forward): %+v", by[c])
	}
	// Rebased rows pinned to LATEST metadata.
	if by[a].ImageTag != "v2" {
		t.Errorf("a should pin to latest, got %q", by[a].ImageTag)
	}
	// Dispatch frontier: a (its upstreams inherited) dispatches now; b waits
	// behind its immediate rebased upstream a.
	if !by[a].ReadyToDispatch {
		t.Errorf("a must be on the dispatch frontier")
	}
	if by[b].ReadyToDispatch {
		t.Errorf("b must be blocked behind immediate rebased upstream a")
	}
}

func TestRebasePartition_NewArrivals_AreRebased(t *testing.T) {
	srcID := uuid.New()
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	new := snapshot.FQN{Service: "svc", Schema: "sch", Table: "new", ScheduleName: "x"}
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {a: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model"}},
		},
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			a:   {ScheduleName: "x", NodeType: "dbt-model"},
			new: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v3"},
		},
	}
	got, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, ScheduleName: "x"})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range got {
		if p.TableName == "new" && p.InitialStatus != "PENDING" {
			t.Errorf("new arrival should be PENDING: %+v", p)
		}
	}
}

func TestRebasePartition_DroppedSourceRowsExcluded(t *testing.T) {
	srcID := uuid.New()
	dropped := snapshot.FQN{Service: "svc", Schema: "sch", Table: "dropped", ScheduleName: "x"}
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {dropped: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x"}},
		},
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{}, // dropped from latest
	}
	_, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, ScheduleName: "x"})
	if !errors.Is(err, snapshot.ErrEmptyProjection) {
		t.Fatalf("want ErrEmptyProjection, got %v", err)
	}
}

func TestRebasePartition_SourceRunIsTest_ReturnsErrRerunOfTestUnsupported(t *testing.T) {
	srcID := uuid.New()
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {a: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"}},
		},
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			a: {ScheduleName: "x", NodeType: "dbt-model"},
		},
	}
	_, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, ScheduleName: "x", Operation: "test"})
	if !errors.Is(err, snapshot.ErrRerunOfTestUnsupported) {
		t.Fatalf("want ErrRerunOfTestUnsupported, got %v", err)
	}
}

func TestRebasePartition_SourceRunIsRun_ProceedsNormally(t *testing.T) {
	srcID := uuid.New()
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {a: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"}},
		},
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			a: {ScheduleName: "x", NodeType: "dbt-model"},
		},
	}
	got, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, ScheduleName: "x", Operation: "run"})
	if err != nil {
		t.Fatalf("unexpected error for run-operation source: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 task, got %d", len(got))
	}
}

func TestRebasePartition_NoSourceRunID_Errors(t *testing.T) {
	r := &fakeTopologyReader{}
	_, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{ScheduleName: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRebasePartition_NoScheduleName_Errors(t *testing.T) {
	srcID := uuid.New()
	r := &fakeTopologyReader{}
	_, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err == nil {
		t.Fatal("expected error")
	}
}
