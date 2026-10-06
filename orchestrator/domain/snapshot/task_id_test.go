package snapshot_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The namespace and the name layout are baked into the ids TaskIDFor derives,
// so the id of a fixed (run, table) must never change.
func TestTaskIDFor_IsStable(t *testing.T) {
	got := snapshot.TaskIDFor("0b0e8f5e-2c55-4c47-9b4a-6d3f0c8a1e21",
		snapshot.FQN{Service: "svc", Schema: "sch", Table: "orders", ScheduleName: "daily"})
	assert.Equal(t, "a2846daf-9cf5-5963-90a3-1bd019aca5b9", got.String())
}

func TestTaskIDFor_DiffersByRunAndByEveryTableComponent(t *testing.T) {
	base := snapshot.FQN{Service: "svc", Schema: "sch", Table: "orders", ScheduleName: "daily"}
	id := snapshot.TaskIDFor("run-1", base)
	require.Equal(t, id, snapshot.TaskIDFor("run-1", base), "the same run and table give the same id")

	for name, other := range map[string]uuid.UUID{
		"run":      snapshot.TaskIDFor("run-2", base),
		"service":  snapshot.TaskIDFor("run-1", snapshot.FQN{Service: "svc2", Schema: "sch", Table: "orders", ScheduleName: "daily"}),
		"schema":   snapshot.TaskIDFor("run-1", snapshot.FQN{Service: "svc", Schema: "sch2", Table: "orders", ScheduleName: "daily"}),
		"table":    snapshot.TaskIDFor("run-1", snapshot.FQN{Service: "svc", Schema: "sch", Table: "orders2", ScheduleName: "daily"}),
		"schedule": snapshot.TaskIDFor("run-1", snapshot.FQN{Service: "svc", Schema: "sch", Table: "orders", ScheduleName: "hourly"}),
		// The separator keeps ("svcs", "ch") apart from ("svc", "sch").
		"boundary": snapshot.TaskIDFor("run-1", snapshot.FQN{Service: "svcs", Schema: "ch", Table: "orders", ScheduleName: "daily"}),
	} {
		assert.NotEqual(t, id, other, name)
	}
}

// selectorCase is one selector together with a reader that can serve it.
type selectorCase struct {
	name     string
	selector snapshot.Selector
	reader   *fakeTopologyReader
	params   func(runID string) snapshot.Params
	// wantRows is the projection length the case must select, so a fixture edit
	// that drops a row fails the test instead of shrinking its coverage.
	wantRows int
}

func selectorCases() []selectorCase {
	srcID := uuid.New()
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	b := snapshot.FQN{Service: "svc", Schema: "sch", Table: "b", ScheduleName: "x"}
	// Single-node and node-set lookups name a table without its schedule.
	single := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}

	latest := map[snapshot.FQN]snapshot.LatestTableRow{
		a: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", TestCount: 1, TestCountKnown: true},
		b: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", TestCount: 1, TestCountKnown: true},
	}
	// a failed and is rebased; b succeeded and is inherited.
	source := map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
		srcID.String(): {
			a: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1"},
			b: {TaskID: uuid.New(), Status: "SUCCEEDED", ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1"},
		},
	}
	singleRow := map[snapshot.FQN]snapshot.LatestTableRow{
		single: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", TestCount: 1, TestCountKnown: true},
	}

	return []selectorCase{
		{
			name: "LatestFullDAG", selector: snapshot.LatestFullDAG{}, wantRows: 2,
			reader: &fakeTopologyReader{LatestDAG: latest},
			params: func(runID string) snapshot.Params { return snapshot.Params{RunID: runID, ScheduleName: "x"} },
		},
		{
			name: "LatestFullDAG test fan-out", selector: snapshot.LatestFullDAG{}, wantRows: 2,
			reader: &fakeTopologyReader{LatestDAG: latest},
			params: func(runID string) snapshot.Params {
				return snapshot.Params{RunID: runID, ScheduleName: "x", Operation: "test"}
			},
		},
		{
			name: "RebasePartition", selector: snapshot.RebasePartition{}, wantRows: 2,
			reader: &fakeTopologyReader{SourceTasks: source, LatestDAG: latest},
			params: func(runID string) snapshot.Params {
				return snapshot.Params{RunID: runID, ScheduleName: "x", SourceRunID: &srcID}
			},
		},
		{
			name: "SourcePinnedDAG", selector: snapshot.SourcePinnedDAG{}, wantRows: 2,
			reader: &fakeTopologyReader{SourceTasks: source},
			params: func(runID string) snapshot.Params { return snapshot.Params{RunID: runID, SourceRunID: &srcID} },
		},
		{
			name:     "SingleNode latest",
			wantRows: 1,
			selector: snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "latest"},
			reader:   &fakeTopologyReader{SingleLatest: singleRow},
			params:   func(runID string) snapshot.Params { return snapshot.Params{RunID: runID} },
		},
		{
			name:     "SingleNode snapshot_of_run",
			wantRows: 1,
			selector: snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "snapshot_of_run"},
			reader: &fakeTopologyReader{SingleFromSourceRun: map[string]map[snapshot.FQN]snapshot.LatestTableRow{
				srcID.String(): singleRow,
			}},
			params: func(runID string) snapshot.Params { return snapshot.Params{RunID: runID, SourceRunID: &srcID} },
		},
		{
			name: "NodeSet", selector: snapshot.NodeSet{Nodes: []snapshot.FQN{single}}, wantRows: 1,
			reader: &fakeTopologyReader{SingleLatest: singleRow},
			params: func(runID string) snapshot.Params { return snapshot.Params{RunID: runID} },
		},
	}
}

// A redelivered trigger selects the same run's projection again while the
// run's :EXECUTES edges keep the task ids they were created with, so every
// selector must derive the same ids for the same run, and other ids for any
// other run.
func TestSelectors_DeriveTaskIDsFromRunAndTable(t *testing.T) {
	for _, tc := range selectorCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			first, err := tc.selector.SelectTasks(ctx, tc.reader, tc.params("run-1"))
			require.NoError(t, err)
			again, err := tc.selector.SelectTasks(ctx, tc.reader, tc.params("run-1"))
			require.NoError(t, err)
			other, err := tc.selector.SelectTasks(ctx, tc.reader, tc.params("run-2"))
			require.NoError(t, err)
			require.Len(t, first, tc.wantRows, "the case must keep selecting its full projection")

			firstIDs, otherIDs := taskIDsByFQN(first), taskIDsByFQN(other)
			assert.Equal(t, firstIDs, taskIDsByFQN(again), "the same run id must give the same task ids")
			for f, id := range firstIDs {
				assert.Equal(t, snapshot.TaskIDFor("run-1", f), id, "%v: the id is TaskIDFor(run, table)", f)
				assert.NotEqual(t, id, otherIDs[f], "%v: another run must get another id", f)
			}
		})
	}
}

// taskIDsByFQN indexes a projection's task ids by the :Table identity the
// snapshot writer matches each row on.
func taskIDsByFQN(projection []snapshot.TaskProjection) map[snapshot.FQN]uuid.UUID {
	out := make(map[snapshot.FQN]uuid.UUID, len(projection))
	for _, p := range projection {
		out[snapshot.FQN{Service: p.ServiceName, Schema: p.SchemaName, Table: p.TableName, ScheduleName: p.ScheduleName}] = p.TaskID
	}
	return out
}
