package snapshot_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
)

func TestSingleNode_LatestMode_Hit(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1"},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "latest"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	if got[0].ImageTag != "v1" || got[0].InitialStatus != "PENDING" {
		t.Errorf("%+v", got[0])
	}
}

func TestSingleNode_LatestMode_PinsRuntimeManifest(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			fqn: {
				ScheduleName:       "x",
				NodeType:           "dbt-model",
				ImageTag:           "v1",
				ManifestVersion:    "m1",
				DBTUniqueID:        "model.finance.orders",
				RuntimeManifestRef: pinnedRef(),
			},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "latest"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].DBTUniqueID != "model.finance.orders" {
		t.Errorf("DBTUniqueID=%q", got[0].DBTUniqueID)
	}
	if got[0].RuntimeManifestRef != pinnedRef() {
		t.Errorf("RuntimeManifestRef=%+v, want %+v", got[0].RuntimeManifestRef, pinnedRef())
	}
}

// snapshot_of_run mode must reproduce the artifact the source run executed
// against. A release promoted after that run has since retargeted the latest
// topology at a different artifact; reading latest here would silently rerun
// the node against code the original run never saw.
func TestSingleNode_SnapshotOfRunMode_PinsSourceRefNotLatest(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	srcRun := uuid.New()
	r := &fakeTopologyReader{
		// Latest topology has drifted to a newer release.
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			fqn: {
				ScheduleName:       "x",
				NodeType:           "dbt-model",
				ImageTag:           "v2",
				ManifestVersion:    "m2",
				DBTUniqueID:        "model.finance.orders_renamed",
				RuntimeManifestRef: driftedRef(),
			},
		},
		// The source run is pinned to what it actually ran.
		SingleFromSourceRun: map[string]map[snapshot.FQN]snapshot.LatestTableRow{
			srcRun.String(): {
				fqn: {
					ScheduleName:       "x",
					NodeType:           "dbt-model",
					ImageTag:           "v1",
					ManifestVersion:    "m1",
					DBTUniqueID:        "model.finance.orders",
					RuntimeManifestRef: pinnedRef(),
				},
			},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "snapshot_of_run"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcRun})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].DBTUniqueID != "model.finance.orders" {
		t.Errorf("DBTUniqueID=%q, want the source run's pinned id, not latest's", got[0].DBTUniqueID)
	}
	if got[0].RuntimeManifestRef != pinnedRef() {
		t.Errorf("RuntimeManifestRef=%+v, want the source run's pinned ref %+v", got[0].RuntimeManifestRef, pinnedRef())
	}
}

func TestSingleNode_LatestMode_Miss_ReturnsErrTargetNotFound(t *testing.T) {
	r := &fakeTopologyReader{SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{}}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "latest"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{})
	if !errors.Is(err, snapshot.ErrTargetNotFound) {
		t.Fatalf("got %v, want ErrTargetNotFound", err)
	}
}

func TestSingleNode_SnapshotOfRunMode_Hit(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	srcID := uuid.New()
	r := &fakeTopologyReader{
		SingleFromSourceRun: map[string]map[snapshot.FQN]snapshot.LatestTableRow{
			srcID.String(): {fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "old", ManifestVersion: "om"}},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "snapshot_of_run"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ImageTag != "old" {
		t.Errorf("ImageTag=%q", got[0].ImageTag)
	}
}

func TestSingleNode_SnapshotOfRunMode_NoSourceRunID_Errors(t *testing.T) {
	r := &fakeTopologyReader{}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "snapshot_of_run"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSingleNode_InvalidMetadataSource_Errors(t *testing.T) {
	r := &fakeTopologyReader{}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "nope"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSingleNode_BlankIdentity_Errors(t *testing.T) {
	r := &fakeTopologyReader{}
	sel := snapshot.SingleNode{MetadataSource: "latest"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSingleNode_LatestMode_TestOperation_ZeroTests_ReturnsErrNoTests(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 0, TestCountKnown: true},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "latest"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{Operation: "test"})
	if !errors.Is(err, snapshot.ErrNoTests) {
		t.Fatalf("got %v, want ErrNoTests", err)
	}
}

// A node whose test_count is absent (TestCountKnown == false — topology written
// before test_count capture) has no known tests, so a test operation is gated
// with ErrNoTests: we never dispatch a `dbt test` we cannot confirm has tests.
// A re-release backfills a concrete test_count and makes such a node runnable.
func TestSingleNode_LatestMode_TestOperation_TestCountAbsent_ReturnsErrNoTests(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 0, TestCountKnown: false},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "latest"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{Operation: "test"})
	if !errors.Is(err, snapshot.ErrNoTests) {
		t.Fatalf("got %v, want ErrNoTests (absent test_count must gate)", err)
	}
}

func TestSingleNode_LatestMode_TestOperation_WithTests_ReturnsProjection(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 3, TestCountKnown: true},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "latest"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{Operation: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
}

func TestSingleNode_LatestMode_RunOperation_ZeroTests_DoesNotGate(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	r := &fakeTopologyReader{
		SingleLatest: map[snapshot.FQN]snapshot.LatestTableRow{
			fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "v1", ManifestVersion: "m1", TestCount: 0},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "latest"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{Operation: ""})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
}

func TestSingleNode_SnapshotOfRunMode_TestOperation_ZeroTests_ReturnsErrNoTests(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	srcID := uuid.New()
	r := &fakeTopologyReader{
		SingleFromSourceRun: map[string]map[snapshot.FQN]snapshot.LatestTableRow{
			srcID.String(): {fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "old", ManifestVersion: "om", TestCount: 0, TestCountKnown: true}},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "snapshot_of_run"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, Operation: "test"})
	if !errors.Is(err, snapshot.ErrNoTests) {
		t.Fatalf("got %v, want ErrNoTests", err)
	}
}

// The snapshot_of_run mode gates identically: an absent test_count on the
// source :EXECUTES-pinned row has no known tests, so a test operation returns
// ErrNoTests rather than dispatching a `dbt test` that cannot be confirmed.
func TestSingleNode_SnapshotOfRunMode_TestOperation_TestCountAbsent_ReturnsErrNoTests(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	srcID := uuid.New()
	r := &fakeTopologyReader{
		SingleFromSourceRun: map[string]map[snapshot.FQN]snapshot.LatestTableRow{
			srcID.String(): {fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "old", ManifestVersion: "om", TestCount: 0, TestCountKnown: false}},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "snapshot_of_run"}
	_, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, Operation: "test"})
	if !errors.Is(err, snapshot.ErrNoTests) {
		t.Fatalf("got %v, want ErrNoTests (absent test_count must gate)", err)
	}
}

func TestSingleNode_SnapshotOfRunMode_TestOperation_WithTests_ReturnsProjection(t *testing.T) {
	fqn := snapshot.FQN{Service: "svc", Schema: "sch", Table: "a"}
	srcID := uuid.New()
	r := &fakeTopologyReader{
		SingleFromSourceRun: map[string]map[snapshot.FQN]snapshot.LatestTableRow{
			srcID.String(): {fqn: {ScheduleName: "x", NodeType: "dbt-model", ImageTag: "old", ManifestVersion: "om", TestCount: 2, TestCountKnown: true}},
		},
	}
	sel := snapshot.SingleNode{ServiceName: "svc", SchemaName: "sch", TableName: "a", MetadataSource: "snapshot_of_run"}
	got, err := sel.SelectTasks(context.Background(), r, snapshot.Params{SourceRunID: &srcID, Operation: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	// The projection must carry the pinned test_count through to the writer, so
	// the :EXECUTES edge stamps it (P2-1): a later promotion changing the node's
	// tests must not retroactively change the no_tests gate for this stale rerun.
	if got[0].TestCount != 2 || !got[0].TestCountKnown {
		t.Errorf("TestCount=%d TestCountKnown=%v, want 2/true (pinned from source row)", got[0].TestCount, got[0].TestCountKnown)
	}
}
