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
	if err != nil { t.Fatal(err) }
	if len(got) != 2 { t.Fatalf("len=%d, want 2: %+v", len(got), got) }
	for _, p := range got {
		if p.InitialStatus != "PENDING" { t.Errorf("status=%q", p.InitialStatus) }
		if p.MaxRetries == 0 { t.Errorf("MaxRetries=0, want default") }
		if p.TaskID.String() == "" { t.Errorf("TaskID empty") }
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
	if !errors.Is(err, want) { t.Fatalf("got %v, want errors.Is %v", err, want) }
}

func TestLatestFullDAG_EmptyDAGReturnsEmptyProjection(t *testing.T) {
	r := &fakeTopologyReader{}
	got, err := snapshot.LatestFullDAG{}.SelectTasks(context.Background(), r, snapshot.Params{ScheduleName: "x"})
	if err != nil { t.Fatal(err) }
	if len(got) != 0 { t.Fatalf("want empty, got %d", len(got)) }
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
	if err != nil { t.Fatal(err) }
	if len(got) != 2 { t.Fatalf("want 2 distinct projections, got %d: %+v", len(got), got) }
	schedules := map[string]bool{}
	for _, p := range got { schedules[p.ScheduleName] = true }
	if !schedules["x"] || !schedules["y"] {
		t.Fatalf("want both schedules x and y, got %+v", schedules)
	}
}
