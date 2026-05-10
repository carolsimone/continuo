package snapshot_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

func TestLatestFullDAG_BuildsProjectionFromLatestRows(t *testing.T) {
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	s := snapshot.FQN{Service: "svc", Schema: "sch", Table: "s"}
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
		fqn := snapshot.FQN{Service: p.ServiceName, Schema: p.SchemaName, Table: p.TableName}
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
