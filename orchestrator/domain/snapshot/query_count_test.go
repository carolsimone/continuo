package snapshot_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
)

// These tests pin the N+1 → O(1) reduction: each selector pass that resolves
// descendants must issue exactly one batched reader call, no matter how many
// FQNs are in the rebase/frontier set. A counting fake records call counts per
// reader method.

func TestRebasePartition_IssuesOneBatchCallPerPass(t *testing.T) {
	srcID := uuid.New()
	// Three non-SUCCEEDED seeds, each with a distinct descendant — pre-batch this
	// was 3 transitive + 6 immediate (one per rebased node) round trips.
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	b := snapshot.FQN{Service: "svc", Schema: "sch", Table: "b", ScheduleName: "x"}
	c := snapshot.FQN{Service: "svc", Schema: "sch", Table: "c", ScheduleName: "x"}
	da := snapshot.FQN{Service: "svc", Schema: "sch", Table: "da", ScheduleName: "x"}
	db := snapshot.FQN{Service: "svc", Schema: "sch", Table: "db", ScheduleName: "x"}
	dc := snapshot.FQN{Service: "svc", Schema: "sch", Table: "dc", ScheduleName: "x"}

	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				a: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				b: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				c: {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
			},
		},
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			a: {ScheduleName: "x", NodeType: "dbt-model"}, b: {ScheduleName: "x", NodeType: "dbt-model"}, c: {ScheduleName: "x", NodeType: "dbt-model"},
			da: {ScheduleName: "x", NodeType: "dbt-model"}, db: {ScheduleName: "x", NodeType: "dbt-model"}, dc: {ScheduleName: "x", NodeType: "dbt-model"},
		},
		DescendantsLatest:    map[snapshot.FQN][]snapshot.FQN{a: {da}, b: {db}, c: {dc}},
		ImmDescendantsLatest: map[snapshot.FQN][]snapshot.FQN{a: {da}, b: {db}, c: {dc}},
	}

	if _, err := (snapshot.RebasePartition{}).SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, ScheduleName: "x"}); err != nil {
		t.Fatal(err)
	}
	if r.DescendantsLatestCalls != 1 {
		t.Errorf("Pass 2 transitive descendants: want 1 batch call, got %d", r.DescendantsLatestCalls)
	}
	if r.ImmDescendantsLatestCalls != 1 {
		t.Errorf("Pass 3b frontier: want 1 batch call, got %d", r.ImmDescendantsLatestCalls)
	}
}

func TestSourcePinnedDAG_IssuesOneBatchCallPerPass(t *testing.T) {
	srcID := uuid.New()
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	b := snapshot.FQN{Service: "svc", Schema: "sch", Table: "b", ScheduleName: "x"}
	c := snapshot.FQN{Service: "svc", Schema: "sch", Table: "c", ScheduleName: "x"}
	da := snapshot.FQN{Service: "svc", Schema: "sch", Table: "da", ScheduleName: "x"}
	db := snapshot.FQN{Service: "svc", Schema: "sch", Table: "db", ScheduleName: "x"}
	dc := snapshot.FQN{Service: "svc", Schema: "sch", Table: "dc", ScheduleName: "x"}

	r := &fakeTopologyReader{
		SourceTasks: map[string]map[snapshot.FQN]snapshot.SourceTaskRow{
			srcID.String(): {
				a:  {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				b:  {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				c:  {TaskID: uuid.New(), Status: "FAILED", ScheduleName: "x", NodeType: "dbt-model"},
				da: {TaskID: uuid.New(), Status: "SKIPPED", ScheduleName: "x", NodeType: "dbt-model"},
				db: {TaskID: uuid.New(), Status: "SKIPPED", ScheduleName: "x", NodeType: "dbt-model"},
				dc: {TaskID: uuid.New(), Status: "SKIPPED", ScheduleName: "x", NodeType: "dbt-model"},
			},
		},
		DescendantsSource: map[string]map[snapshot.FQN][]snapshot.FQN{
			srcID.String(): {a: {da}, b: {db}, c: {dc}},
		},
		ImmDescendantsSource: map[string]map[snapshot.FQN][]snapshot.FQN{
			srcID.String(): {a: {da}, b: {db}, c: {dc}},
		},
	}

	if _, err := (snapshot.SourcePinnedDAG{}).SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID}); err != nil {
		t.Fatal(err)
	}
	if r.DescendantsSourceCalls != 1 {
		t.Errorf("Pass 2 transitive descendants: want 1 batch call, got %d", r.DescendantsSourceCalls)
	}
	if r.ImmDescendantsSourceCalls != 1 {
		t.Errorf("Pass 2b frontier: want 1 batch call, got %d", r.ImmDescendantsSourceCalls)
	}
}

func TestLatestFullDAG_IssuesOneFrontierBatchCall(t *testing.T) {
	a := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a", ScheduleName: "x"}
	b := snapshot.FQN{Service: "svc", Schema: "sch", Table: "b", ScheduleName: "x"}
	c := snapshot.FQN{Service: "svc", Schema: "sch", Table: "c", ScheduleName: "x"}
	r := &fakeTopologyReader{
		LatestDAG: map[snapshot.FQN]snapshot.LatestTableRow{
			a: {ScheduleName: "x", NodeType: "dbt-model"},
			b: {ScheduleName: "x", NodeType: "dbt-model"},
			c: {ScheduleName: "x", NodeType: "dbt-model"},
		},
		ImmDescendantsLatest: map[snapshot.FQN][]snapshot.FQN{a: {b}, b: {c}},
	}
	if _, err := (snapshot.LatestFullDAG{}).SelectTasks(context.Background(), r, snapshot.Params{ScheduleName: "x"}); err != nil {
		t.Fatal(err)
	}
	if r.ImmDescendantsLatestCalls != 1 {
		t.Errorf("frontier classification: want 1 batch call, got %d", r.ImmDescendantsLatestCalls)
	}
}
