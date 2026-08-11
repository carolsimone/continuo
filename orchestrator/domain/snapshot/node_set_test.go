package snapshot_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
)

func TestNodeSet_ResolvesEveryNodeInOrder(t *testing.T) {
	a := snapshot.FQN{Service: "core", Schema: "analytics", Table: "seed_users"}
	b := snapshot.FQN{Service: "core", Schema: "analytics", Table: "seed_fx_transactions"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			a: {ScheduleName: "seed", NodeType: "dbt-seed", ImageTag: "v1", ManifestVersion: "rel-1"},
			b: {ScheduleName: "seed", NodeType: "dbt-seed", ImageTag: "v1", ManifestVersion: "rel-1"},
		},
	}

	got, err := snapshot.NodeSet{Nodes: []snapshot.FQN{a, b}}.SelectTasks(context.Background(), r, snapshot.Params{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	if got[0].TableName != "seed_users" || got[1].TableName != "seed_fx_transactions" {
		t.Errorf("order not preserved: %s, %s", got[0].TableName, got[1].TableName)
	}
	for _, task := range got {
		if task.InitialStatus != "PENDING" {
			t.Errorf("%s: InitialStatus=%q", task.TableName, task.InitialStatus)
		}
		// The retry budget is what makes a failed promoted-seed build recoverable;
		// without it the run inherits no retries and a failure is terminal.
		if task.MaxRetries != pkgEvents.DefaultTaskMaxRetries {
			t.Errorf("%s: MaxRetries=%d, want %d", task.TableName, task.MaxRetries, pkgEvents.DefaultTaskMaxRetries)
		}
		if task.ScheduleName != "seed" || task.NodeType != "dbt-seed" {
			t.Errorf("%s: metadata not taken from topology: %+v", task.TableName, task)
		}
	}
}

func TestNodeSet_EmptyList_ReturnsErrEmptyProjection(t *testing.T) {
	r := &fakeTopologyReader{SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{}}
	_, err := snapshot.NodeSet{}.SelectTasks(context.Background(), r, snapshot.Params{})
	if !errors.Is(err, snapshot.ErrEmptyProjection) {
		t.Fatalf("got %v, want ErrEmptyProjection", err)
	}
}

// A node named in the set but missing from the topology fails the whole run.
// Shrinking the set instead would report a successful run that silently skipped
// work — the failure mode this whole change exists to remove.
func TestNodeSet_UnknownNode_FailsTheWholeSet(t *testing.T) {
	known := snapshot.FQN{Service: "core", Schema: "analytics", Table: "seed_users"}
	unknown := snapshot.FQN{Service: "core", Schema: "analytics", Table: "gone"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			known: {ScheduleName: "seed", NodeType: "dbt-seed"},
		},
	}

	_, err := snapshot.NodeSet{Nodes: []snapshot.FQN{known, unknown}}.SelectTasks(context.Background(), r, snapshot.Params{})
	if !errors.Is(err, snapshot.ErrTargetNotFound) {
		t.Fatalf("got %v, want ErrTargetNotFound", err)
	}
}

func TestNodeSet_IncompleteIdentity_IsRejected(t *testing.T) {
	r := &fakeTopologyReader{SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{}}
	_, err := snapshot.NodeSet{
		Nodes: []snapshot.FQN{{Service: "core", Schema: "analytics"}},
	}.SelectTasks(context.Background(), r, snapshot.Params{})
	if err == nil {
		t.Fatal("expected an error for a node with no table name")
	}
}
