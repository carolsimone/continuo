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
