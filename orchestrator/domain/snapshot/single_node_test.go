package snapshot_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleNode_LatestMode(t *testing.T) {
	driver := newDriver(t)
	sched := "test-sn-latest-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-sn-latest-") })

	seedTable(t, driver, sched, "svc", "s", "x", "img:7", "v7")

	got := runSelector(t, driver, snapshot.SingleNode{
		ServiceName: "svc", SchemaName: "s", TableName: "x", MetadataSource: "latest",
	}, snapshot.Params{})

	require.Len(t, got, 1)
	assert.Equal(t, "img:7", got[0].ImageTag)
	assert.Equal(t, "v7", got[0].ManifestVersion)
	assert.Equal(t, sched, got[0].ScheduleName)
	assert.Equal(t, "PENDING", got[0].InitialStatus)
	assert.Nil(t, got[0].InheritedFromTaskID)
	assert.NotEqual(t, uuid.Nil, got[0].TaskID, "fresh task_id assigned")
}

func TestSingleNode_StaleMode_ReadsFromSourceEdge(t *testing.T) {
	driver := newDriver(t)
	sched := "test-sn-stale-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-sn-stale-") })

	// Latest topology says img:NEW; source's :EXECUTES says img:OLD.
	seedTable(t, driver, sched, "svc", "s", "x", "img:NEW", "vNEW")

	srcRunID := uuid.New()
	srcTaskID := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "x", Status: "SUCCEEDED",
			TaskID: srcTaskID.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.SingleNode{
		ServiceName: "svc", SchemaName: "s", TableName: "x", MetadataSource: "snapshot_of_run",
	}, snapshot.Params{SourceRunID: &srcRunID})

	require.Len(t, got, 1)
	assert.Equal(t, "img:OLD", got[0].ImageTag, "stale mode must read source's pinned metadata, not latest")
	assert.Equal(t, "vOLD", got[0].ManifestVersion)
	assert.Equal(t, "PENDING", got[0].InitialStatus, "stale mode still produces PENDING (it's a re-execution)")
}

func TestSingleNode_LatestMode_TargetNotFound(t *testing.T) {
	driver := newDriver(t)

	sess := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer sess.Close(context.Background())
	_, err := sess.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, ierr := snapshot.SingleNode{
			ServiceName: "missing-svc", SchemaName: "s", TableName: "missing-tbl", MetadataSource: "latest",
		}.SelectTasks(context.Background(), tx, snapshot.Params{})
		return nil, ierr
	})
	require.ErrorIs(t, err, snapshot.ErrTargetNotFound)
}

func TestSingleNode_StaleMode_SourceMissing(t *testing.T) {
	driver := newDriver(t)
	srcRunID := uuid.New() // never seeded

	sess := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer sess.Close(context.Background())
	_, err := sess.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, ierr := snapshot.SingleNode{
			ServiceName: "svc", SchemaName: "s", TableName: "x", MetadataSource: "snapshot_of_run",
		}.SelectTasks(context.Background(), tx, snapshot.Params{SourceRunID: &srcRunID})
		return nil, ierr
	})
	require.ErrorIs(t, err, snapshot.ErrTargetNotFound)
}

func TestSingleNode_StaleMode_RequiresSourceRunID(t *testing.T) {
	driver := newDriver(t)
	sess := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer sess.Close(context.Background())
	_, err := sess.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, ierr := snapshot.SingleNode{
			ServiceName: "svc", SchemaName: "s", TableName: "x", MetadataSource: "snapshot_of_run",
		}.SelectTasks(context.Background(), tx, snapshot.Params{}) // no SourceRunID
		return nil, ierr
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SourceRunID")
}

func TestSingleNode_RejectsInvalidMetadataSource(t *testing.T) {
	driver := newDriver(t)
	sess := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer sess.Close(context.Background())
	_, err := sess.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, ierr := snapshot.SingleNode{
			ServiceName: "svc", SchemaName: "s", TableName: "x", MetadataSource: "bogus",
		}.SelectTasks(context.Background(), tx, snapshot.Params{})
		return nil, ierr
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MetadataSource")
}

func TestSingleNode_RejectsEmptyIdentity(t *testing.T) {
	driver := newDriver(t)
	sess := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer sess.Close(context.Background())
	_, err := sess.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, ierr := snapshot.SingleNode{
			ServiceName: "", SchemaName: "s", TableName: "x", MetadataSource: "latest",
		}.SelectTasks(context.Background(), tx, snapshot.Params{})
		return nil, ierr
	})
	require.Error(t, err)
}
