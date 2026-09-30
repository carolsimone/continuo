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
				c: {TaskID: rootC, Status: "SUCCEEDED", ScheduleName: "x", NodeType: "python-api", SecretRef: "continuo-api-old"},
			},
		},
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			a: {ScheduleName: "x", NodeType: "python-api", ImageTag: "v2", ManifestVersion: "m2", SecretRef: "continuo-api-fx"},
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
	if by[a].SecretRef != "continuo-api-fx" {
		t.Errorf("a should pin the latest secret_ref, got %q", by[a].SecretRef)
	}
	// Inherited rows keep the source run's pinned metadata.
	if by[c].SecretRef != "continuo-api-old" {
		t.Errorf("c should keep the source run's secret_ref, got %q", by[c].SecretRef)
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
		SourceRunOperationByID: map[string]string{srcID.String(): "test"},
	}
	_, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, ScheduleName: "x"})
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

// TestRebasePartition_SingleNodeRunSource_NeverFansOut pins the invariant that
// stands in for a node-type guard on the rebase path. RebasePartition has no
// checkFullRefreshTarget-equivalent check of its own, but a single-node run's
// :Run carries the synthetic schedule_name TriggerSingleNodeRun mints
// ("single-node-run-<id>") — never a real catalog schedule tag, so no :Table
// in the topology is ever written with it. LoadLatestSourceDAG therefore
// always returns an empty DAG for a single-node-run source, so RebasePartition
// can never rebase the target — let alone fan out to its descendants — before
// a node-type check would even matter; it fails closed with
// ErrEmptyProjection. This holds regardless of the source's operation,
// including full_refresh against a node type full_refresh does not support
// (python-node here).
func TestRebasePartition_SingleNodeRunSource_NeverFansOut(t *testing.T) {
	srcID := uuid.New()
	syntheticSchedule := "single-node-run-abc12345"
	target := snapshot.FQN{Service: "svc", Schema: "sch", Table: "target", ScheduleName: syntheticSchedule}
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				target: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: syntheticSchedule, NodeType: "python-node"},
			},
		},
		// Models production reality: real :Table rows carry the catalog
		// schedule they belong to, never a single-node run's synthetic one, so
		// nothing matches when RebasePartition looks up the "latest" DAG by it.
		LatestDAG:              map[snapshot.FQN]snapshot.LatestTableRow{},
		SourceRunOperationByID: map[string]string{srcID.String(): "full_refresh"},
	}
	_, err := snapshot.RebasePartition{}.SelectTasks(context.Background(), r, snapshot.Params{
		SourceRunID:  &srcID,
		ScheduleName: syntheticSchedule,
		Operation:    "full_refresh",
	})
	if !errors.Is(err, snapshot.ErrEmptyProjection) {
		t.Fatalf("want ErrEmptyProjection (rebase of a single-node-run source can never fan out), got %v", err)
	}
}
