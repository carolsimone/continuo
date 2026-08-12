package snapshot_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// Pinned metadata must win over the topology row. A promotion can be overtaken
// by a later one before its seeds are projected; without this the run would
// build the release's seeds with the newer release's image.
func TestNodeSet_PinnedMetadataOverridesTheTopologyRow(t *testing.T) {
	fqn := snapshot.FQN{Service: "core", Schema: "analytics", Table: "seed_users"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			// The topology has already moved on to a newer release's image.
			fqn: {ScheduleName: "seed", NodeType: "dbt-seed", ImageTag: "newer-release", ManifestVersion: "rel-2"},
		},
	}

	got, err := snapshot.NodeSet{
		Nodes:  []snapshot.FQN{fqn},
		Pinned: map[snapshot.FQN]snapshot.PinnedNodeMetadata{fqn: {NodeType: "dbt-seed", ImageTag: "rel-1-image"}},
	}.SelectTasks(context.Background(), r, snapshot.Params{})
	require.NoError(t, err)

	require.Len(t, got, 1)
	assert.Equal(t, "rel-1-image", got[0].ImageTag,
		"the triggering release's image must be used, not the topology's current one")
	// Fields the pin does not cover still come from the topology.
	assert.Equal(t, "seed", got[0].ScheduleName)
}

func TestNodeSet_WithoutPin_FallsBackToTheTopologyRow(t *testing.T) {
	fqn := snapshot.FQN{Service: "core", Schema: "analytics", Table: "seed_users"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			fqn: {ScheduleName: "seed", NodeType: "dbt-seed", ImageTag: "from-topology"},
		},
	}

	got, err := snapshot.NodeSet{Nodes: []snapshot.FQN{fqn}}.SelectTasks(context.Background(), r, snapshot.Params{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "from-topology", got[0].ImageTag)
}
