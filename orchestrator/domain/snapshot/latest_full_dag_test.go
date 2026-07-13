package snapshot_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

func TestLatestFullDAG_BuildsProjectionFromLatestRows(t *testing.T) {
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	s := snapshot.FQN{Service: "svc", Schema: "sch", Table: "s", ScheduleName: "seed"}
	r := &fakeTopologyReader{
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			a: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
			s: {ScheduleName: "seed", NodeType: "dbt-seed", ImageTag: "v2", ManifestVersion: "m2"},
		},
	}
	got, err := snapshot.LatestFullDAG{}.SelectTasks(context.Background(), r, snapshot.Params{ScheduleName: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2: %+v", len(got), got)
	}
	for _, p := range got {
		if p.InitialStatus != "PENDING" {
			t.Errorf("status=%q", p.InitialStatus)
		}
		if p.MaxRetries == 0 {
			t.Errorf("MaxRetries=0, want default")
		}
		if p.TaskID.String() == "" {
			t.Errorf("TaskID empty")
		}
		fqn := snapshot.FQN{Service: p.ServiceName, Schema: p.SchemaName, Table: p.TableName, ScheduleName: p.ScheduleName}
		want := r.LatestDAG[fqn]
		if p.ScheduleName != want.ScheduleName || p.NodeType != want.NodeType ||
			p.ImageTag != want.ImageTag || p.ManifestVersion != want.ManifestVersion {
			t.Errorf("row mismatch for %+v: got %+v want %+v", fqn, p, want)
		}
	}
}

func TestLatestFullDAG_ReaderErrorPropagates(t *testing.T) {
	want := errors.New("boom")
	r := &fakeTopologyReader{LatestDAGErr: want}
	_, err := snapshot.LatestFullDAG{}.SelectTasks(context.Background(), r, snapshot.Params{ScheduleName: "x"})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want errors.Is %v", err, want)
	}
}

func TestLatestFullDAG_EmptyDAGReturnsEmptyProjection(t *testing.T) {
	r := &fakeTopologyReader{}
	got, err := snapshot.LatestFullDAG{}.SelectTasks(context.Background(), r, snapshot.Params{ScheduleName: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %d", len(got))
	}
}

// Whole-DAG test runs are edgeless flat fan-out: only nodes with TestCount>0
// are projected, and every projected task dispatches immediately regardless
// of DAG position — including a node that would be blocked in a normal run.
func TestLatestFullDAG_TestOperation_FiltersZeroTestsAndAllReady(t *testing.T) {
	root := snapshot.FQN{Service: "svc", Schema: "sch", Table: "root", ScheduleName: "x"}
	downstream := snapshot.FQN{Service: "svc", Schema: "sch", Table: "downstream", ScheduleName: "x"}
	noTests := snapshot.FQN{Service: "svc", Schema: "sch", Table: "no_tests", ScheduleName: "x"}
	r := &fakeTopologyReader{
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			root:       {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 2, TestCountKnown: true},
			downstream: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 1, TestCountKnown: true},
			noTests:    {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 0, TestCountKnown: true},
		},
		// downstream is the immediate descendant of root, so in a normal run it
		// would be blocked (ReadyToDispatch == false).
		ImmDescendantsLatest: map[snapshot.FQN][]snapshot.FQN{
			root: {downstream},
		},
	}
	got, err := snapshot.LatestFullDAG{}.SelectTasks(context.Background(), r, snapshot.Params{Operation: "test", ScheduleName: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2 (no_tests excluded): %+v", len(got), got)
	}
	seen := map[string]bool{}
	for _, p := range got {
		seen[p.TableName] = true
		if p.TableName == "no_tests" {
			t.Errorf("no_tests node must be excluded, got %+v", p)
		}
		if !p.ReadyToDispatch {
			t.Errorf("table=%q: ReadyToDispatch=false, want true (edgeless test run)", p.TableName)
		}
	}
	if !seen["root"] || !seen["downstream"] {
		t.Fatalf("want root and downstream projected, got %+v", seen)
	}
}

// Graceful degradation: a node with test_count absent (topology written before
// test_count existed) must NOT be filtered out of a whole-DAG test run — only
// a KNOWN, explicit zero excludes a node.
func TestLatestFullDAG_TestOperation_TestCountAbsent_Included(t *testing.T) {
	known := snapshot.FQN{Service: "svc", Schema: "sch", Table: "known_zero", ScheduleName: "x"}
	absent := snapshot.FQN{Service: "svc", Schema: "sch", Table: "absent", ScheduleName: "x"}
	r := &fakeTopologyReader{
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			known:  {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 0, TestCountKnown: true},
			absent: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 0, TestCountKnown: false},
		},
	}
	got, err := snapshot.LatestFullDAG{}.SelectTasks(context.Background(), r, snapshot.Params{Operation: "test", ScheduleName: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d, want 1 (only absent included): %+v", len(got), got)
	}
	if got[0].TableName != "absent" {
		t.Errorf("table=%q, want %q", got[0].TableName, "absent")
	}
}

// Sanity check: the existing frontier behavior for normal runs (Operation ==
// "") is unaffected by the new test-operation branch.
func TestLatestFullDAG_RunOperation_FrontierUnchanged(t *testing.T) {
	root := snapshot.FQN{Service: "svc", Schema: "sch", Table: "root", ScheduleName: "x"}
	downstream := snapshot.FQN{Service: "svc", Schema: "sch", Table: "downstream", ScheduleName: "x"}
	r := &fakeTopologyReader{
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			root:       {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
			downstream: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
		},
		ImmDescendantsLatest: map[snapshot.FQN][]snapshot.FQN{
			root: {downstream},
		},
	}
	got, err := snapshot.LatestFullDAG{}.SelectTasks(context.Background(), r, snapshot.Params{ScheduleName: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2: %+v", len(got), got)
	}
	for _, p := range got {
		switch p.TableName {
		case "root":
			if !p.ReadyToDispatch {
				t.Errorf("root: ReadyToDispatch=false, want true")
			}
		case "downstream":
			if p.ReadyToDispatch {
				t.Errorf("downstream: ReadyToDispatch=true, want false (blocked by root)")
			}
		}
	}
}

// Regression test for P2: cross-schedule :Table duplicates with the same
// (service, schema, table) but different schedule_name must produce distinct
// TaskProjections.  Without the ScheduleName field in FQN the map collapses
// the two entries and only one projection is emitted.
func TestLatestFullDAG_CrossScheduleDuplicates_AreDistinct(t *testing.T) {
	fqnX := snapshot.FQN{Service: "svc", Schema: "sch", Table: "shared", ScheduleName: "x"}
	fqnY := snapshot.FQN{Service: "svc", Schema: "sch", Table: "shared", ScheduleName: "y"}
	r := &fakeTopologyReader{
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			fqnX: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
			fqnY: {ScheduleName: "y", NodeType: "dbt-model", ImageTag: "v2", ManifestVersion: "m2"},
		},
	}
	got, err := snapshot.LatestFullDAG{}.SelectTasks(context.Background(), r, snapshot.Params{ScheduleName: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 distinct projections, got %d: %+v", len(got), got)
	}
	schedules := map[string]bool{}
	for _, p := range got {
		schedules[p.ScheduleName] = true
	}
	if !schedules["x"] || !schedules["y"] {
		t.Fatalf("want both schedules x and y, got %+v", schedules)
	}
}
