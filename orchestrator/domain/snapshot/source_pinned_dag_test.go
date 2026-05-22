package snapshot_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
)

func TestSourcePinnedDAG_SingleFailedTask_NoDescendants_RebasesOnlyThat(t *testing.T) {
	srcID := uuid.New()
	failed := snapshot.FQN{Service: "svc", Schema: "sch", Table: "f", ScheduleName: "x"}
	succA := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}

	rootA := uuid.New()
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				failed: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
				succA:  {TaskID: rootA, Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
			},
		},
		DescendantsSource: map[string]map[snapshot.FQN][]snapshot.FQN{srcID.String(): {failed: nil}},
	}
	got, err := snapshot.SourcePinnedDAG{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err != nil {
		t.Fatal(err)
	}
	by := indexByFQN(got)
	if by[failed].InitialStatus != "PENDING" || by[failed].InheritedFromTaskID != nil {
		t.Errorf("failed: want PENDING/no-inherit, got %+v", by[failed])
	}
	if by[succA].InitialStatus != "SUCCEEDED" || by[succA].InheritedFromTaskID == nil || *by[succA].InheritedFromTaskID != rootA {
		t.Errorf("succA: want SUCCEEDED inherit pointing at rootA, got %+v", by[succA])
	}
}

func TestSourcePinnedDAG_TwoIndependentFailedSubtrees_BothRebased(t *testing.T) {
	srcID := uuid.New()
	// Subtree 1: e (FAILED) → f (SKIPPED).
	e := snapshot.FQN{Service: "svc", Schema: "sch", Table: "e", ScheduleName: "x"}
	f := snapshot.FQN{Service: "svc", Schema: "sch", Table: "f", ScheduleName: "x"}
	// Subtree 2: g (FAILED) → h (SKIPPED).
	g := snapshot.FQN{Service: "svc", Schema: "sch", Table: "g", ScheduleName: "x"}
	h := snapshot.FQN{Service: "svc", Schema: "sch", Table: "h", ScheduleName: "x"}
	// SUCCEEDED ancestors.
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}

	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				a: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model"},
				e: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				f: {TaskID: uuid.New(), Status: "SKIPPED", ScheduleName: "x", NodeType: "dbt-model"},
				g: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				h: {TaskID: uuid.New(), Status: "SKIPPED", ScheduleName: "x", NodeType: "dbt-model"},
			},
		},
		DescendantsSource: map[string]map[snapshot.FQN][]snapshot.FQN{
			srcID.String(): {
				e: {f},
				g: {h},
				f: nil,
				h: nil,
			},
		},
	}
	got, err := snapshot.SourcePinnedDAG{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err != nil {
		t.Fatal(err)
	}
	by := indexByFQN(got)
	for _, fqn := range []snapshot.FQN{e, f, g, h} {
		if by[fqn].InitialStatus != "PENDING" || by[fqn].InheritedFromTaskID != nil {
			t.Errorf("%s: want PENDING/no-inherit, got %+v", fqn.Table, by[fqn])
		}
	}
	if by[a].InitialStatus != "SUCCEEDED" || by[a].InheritedFromTaskID == nil {
		t.Errorf("a: want SUCCEEDED inherit, got %+v", by[a])
	}
}

// Regression bite: a SUCCEEDED descendant of a FAILED root is added to the
// rebase set via Pass 2 (descendants of non-SUCCEEDED) and flipped to PENDING
// with source's pinned metadata. This documents the new rule explicitly so a
// future implementer doesn't misread it as "only non-SUCCEEDED descendants
// rebase".
func TestSourcePinnedDAG_SucceededDescendantOfFailedRoot_IsRebased(t *testing.T) {
	srcID := uuid.New()
	failedRoot := snapshot.FQN{Service: "svc", Schema: "sch", Table: "root", ScheduleName: "x"}
	succDesc := snapshot.FQN{Service: "svc", Schema: "sch", Table: "ok", ScheduleName: "x"}

	rootSuccID := uuid.New()
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				failedRoot: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				succDesc:   {TaskID: rootSuccID, Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model"},
			},
		},
		DescendantsSource: map[string]map[snapshot.FQN][]snapshot.FQN{
			srcID.String(): {failedRoot: {succDesc}},
		},
	}
	got, err := snapshot.SourcePinnedDAG{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err != nil {
		t.Fatal(err)
	}
	by := indexByFQN(got)
	if by[failedRoot].InitialStatus != "PENDING" {
		t.Errorf("failedRoot: want PENDING, got %+v", by[failedRoot])
	}
	if by[succDesc].InitialStatus != "PENDING" {
		t.Errorf("succDesc: want PENDING (descendant of failed root), got %+v", by[succDesc])
	}
}

// On rerun the failed subtree is re-pended, but only the frontier — the
// rebased nodes whose upstreams all SUCCEEDED — may dispatch immediately. A
// re-pended SKIPPED node (f, h) sits behind its still-pending upstream (e, g)
// and must NOT be dispatched until that upstream succeeds; if the upstream
// re-fails, the run aggregate cascade-skips it again. This guards the
// regression where the whole rebase subtree was dispatched at once and the
// downstream nodes ran when they should have stayed skipped.
func TestSourcePinnedDAG_OnlyRebaseFrontierIsDispatchable(t *testing.T) {
	srcID := uuid.New()
	e := snapshot.FQN{Service: "svc", Schema: "sch", Table: "e", ScheduleName: "x"}
	f := snapshot.FQN{Service: "svc", Schema: "sch", Table: "f", ScheduleName: "x"}
	g := snapshot.FQN{Service: "svc", Schema: "sch", Table: "g", ScheduleName: "x"}
	h := snapshot.FQN{Service: "svc", Schema: "sch", Table: "h", ScheduleName: "x"}
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}

	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				a: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model"},
				e: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				f: {TaskID: uuid.New(), Status: "SKIPPED", ScheduleName: "x", NodeType: "dbt-model"},
				g: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				h: {TaskID: uuid.New(), Status: "SKIPPED", ScheduleName: "x", NodeType: "dbt-model"},
			},
		},
		DescendantsSource: map[string]map[snapshot.FQN][]snapshot.FQN{
			srcID.String(): {e: {f}, g: {h}, f: nil, h: nil},
		},
	}
	got, err := snapshot.SourcePinnedDAG{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err != nil {
		t.Fatal(err)
	}
	by := indexByFQN(got)

	// Re-pended skipped nodes are PENDING but blocked behind their pending upstream.
	if by[f].InitialStatus != "PENDING" || by[h].InitialStatus != "PENDING" {
		t.Fatalf("skipped nodes must re-pend: f=%+v h=%+v", by[f], by[h])
	}
	// Frontier: e and g (upstream a succeeded) dispatch now; f and h wait.
	if !by[e].ReadyToDispatch || !by[g].ReadyToDispatch {
		t.Errorf("frontier roots e,g must be dispatchable: e=%v g=%v", by[e].ReadyToDispatch, by[g].ReadyToDispatch)
	}
	if by[f].ReadyToDispatch || by[h].ReadyToDispatch {
		t.Errorf("downstream f,h must NOT be dispatchable while upstream pending: f=%v h=%v", by[f].ReadyToDispatch, by[h].ReadyToDispatch)
	}
	// Inherited rows are never dispatched.
	if by[a].ReadyToDispatch {
		t.Errorf("inherited node a must not be dispatchable")
	}
}

func TestSourcePinnedDAG_AllSucceeded_ReturnsErrEmptyProjection(t *testing.T) {
	srcID := uuid.New()
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	b := snapshot.FQN{Service: "svc", Schema: "sch", Table: "b", ScheduleName: "x"}
	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				a: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x"},
				b: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x"},
			},
		},
	}
	_, err := snapshot.SourcePinnedDAG{}.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if !errors.Is(err, snapshot.ErrEmptyProjection) {
		t.Fatalf("want ErrEmptyProjection, got %v", err)
	}
}

func TestSourcePinnedDAG_NoSourceRunID_Errors(t *testing.T) {
	r := &fakeTopologyReader{}
	_, err := snapshot.SourcePinnedDAG{}.SelectTasks(context.Background(), r, snapshot.Params{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func indexByFQN(projection []snapshot.TaskProjection) map[snapshot.FQN]snapshot.TaskProjection {
	out := map[snapshot.FQN]snapshot.TaskProjection{}
	for _, p := range projection {
		out[snapshot.FQN{Service: p.ServiceName, Schema: p.SchemaName, Table: p.TableName, ScheduleName: p.ScheduleName}] = p
	}
	return out
}
