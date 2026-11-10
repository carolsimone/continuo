package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ingestProbeSchedule is the schedule of service-1's ingest_probe_reader
	// model, and of nothing else.
	ingestProbeSchedule = "ingest-probe"
	// ingestProbeReaderTable is the model that reads the outside table, and
	// ingestProbeReaderKey its node key (<schema>.<table>, lower-case).
	ingestProbeReaderTable = "ingest_probe_reader"
	ingestProbeReaderKey   = seedSchemaName + "." + ingestProbeReaderTable
	// ingestProbeReaderService owns the reader. The schedule graph names a node
	// <service>.<schema>.<table>, so the reader's id carries it.
	ingestProbeReaderService = "service-1"
	ingestProbeReaderGraphID = ingestProbeReaderService + "." + ingestProbeReaderKey
	// The outside table the reader selects from: scripts/setup.sh creates it in
	// the warehouse, and no model builds it.
	ingestedSchema      = "e2e_raw"
	ingestedTable       = "ingest_probe"
	ingestedKey         = ingestedSchema + "." + ingestedTable
	ingestedGraphNodeID = "." + ingestedKey // an ingested node has no service, so its id starts with a dot
)

// TestE2E_IngestedNode_ShowsInTheGraphAndNeverRuns proves that a table a model
// reads and no model builds is part of the topology as an ingested node and is
// never run:
//
//  1. A bootstrap release of service-1 promotes. service-1's
//     ingest_probe_reader model reads e2e_raw.ingest_probe, a table
//     scripts/setup.sh creates in the warehouse and no model builds.
//  2. run-controller's copy of the release holds a node for the table, of type
//     ingested, with no service and no schedule, and an edge into the reader.
//  3. catalog's live graph holds an active :Node of type ingested for it, and
//     the reader's :DEPENDS_ON reaches it.
//  4. The ui's schedule graph of ingest-probe lists the node, typed ingested
//     and named without a service, and the edge from it to the reader.
//  5. StartRun(ingest-probe) plans exactly the reader, which runs and
//     succeeds: the Job selects from the outside table. No Job is created for
//     the table.
//  6. A single-node run of the table is refused with InvalidArgument, with
//     no service and with the reader's service.
//  7. No run of the tenant, this one or any other, holds a task for the table.
//
// The release leaves the live topology as service-1's alone and starts the
// promote-seed runs of its seeds, so a deferred restore announces the default
// e2e DAG again and waits for those runs to end, the way
// TestDroppedNodeKeepsItsHistory does.
func TestE2E_IngestedNode_ShowsInTheGraphAndNeverRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: the promotion (5m) + run-controller's copy (2m) + the seed
	// loads of the release (5m) + the run (5m) = 17m. The restore has a context
	// of its own.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-ingest-" + uuid.NewString()[:8]
	t.Logf("release_id=%s changed_service=%s", releaseID, ingestProbeReaderService)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	changedImageTag := allServices[ingestProbeReaderService].imageTag
	require.NotEmpty(t, changedImageTag, "image_tag missing for %s", ingestProbeReaderService)
	readerInBaseline := false
	for _, n := range allServices[ingestProbeReaderService].nodes {
		readerInBaseline = readerInBaseline || n.uniqueID == ingestProbeReaderKey
	}
	require.True(t, readerInBaseline,
		"%s not found in %s's baseline manifest — is the model in service-1 and the image rebuilt + manifests re-uploaded?",
		ingestProbeReaderKey, ingestProbeReaderService)

	// ── 1. A bootstrap release of service-1 promotes ─────────────────────────
	// From here the test moves the live topology, current_prod and service_prod.
	defer restoreDefaultTopologyAfter(t, clients, releaseID)

	// Empty current_prod and service_prod, as the bootstrap test does: the
	// release promotes with no validation against no baseline.
	resetReleaseControllerQueue(t, ctx, clients)
	_, err := clients.releaseDB.ExecContext(ctx, "DELETE FROM current_prod")
	require.NoError(t, err, "clear current_prod")
	_, err = clients.releaseDB.ExecContext(ctx, "DELETE FROM service_prod")
	require.NoError(t, err, "clear service_prod")

	postRelease(t, clients, ingestProbeReaderService, releaseID, changedImageTag, true)

	pollUntil(t, ctx, 5*time.Minute, 2*time.Second, func() (bool, error) {
		return neo4jScalarString(ctx, clients,
			`MATCH (m:Meta {tenant_id: 'default', key: 'current_release'}) RETURN m.release_id AS v`, nil) == releaseID, nil
	}, "timeout waiting for release "+releaseID+" to promote + swap catalog's topology")

	// catalog and run-controller apply a promotion through groups of their own,
	// so run-controller's copy is awaited too: its live release is this one, with
	// a model of the schedule.
	var live runControllerLiveRelease
	var liveErr error
	pollUntilDescribed(t, ctx, 2*time.Minute, time.Second, func() (bool, error) {
		live, liveErr = readRunControllerLiveRelease(ctx, clients)
		if liveErr != nil {
			return false, liveErr
		}
		return live.releaseID == releaseID && live.models[ingestProbeSchedule] > 0, nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for run-controller's live release to be %s with a model of schedule %s: "+
			"its live release is %q with models %v (last read error: %v)",
			releaseID, ingestProbeSchedule, live.releaseID, live.models, liveErr)
	})

	// ── 2. run-controller stores the ingested node ───────────────────────────
	var stored struct {
		UniqueID     string `db:"unique_id"`
		ServiceName  string `db:"service_name"`
		SchemaName   string `db:"schema_name"`
		TableName    string `db:"table_name"`
		NodeType     string `db:"node_type"`
		ScheduleName string `db:"schedule_name"`
	}
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &stored, `
		SELECT unique_id, service_name, schema_name, table_name, node_type, schedule_name
		  FROM topology_nodes
		 WHERE tenant_id = $1 AND release_id = $2 AND node_key = $3`,
		events.DefaultTenantID, releaseID, ingestedKey),
		"run-controller's copy of release %s holds no node %s", releaseID, ingestedKey)
	assert.Equal(t, string(model.NodeTypeIngested), stored.NodeType, "the table's node is ingested")
	assert.Empty(t, stored.ServiceName, "no service builds an ingested node")
	assert.Empty(t, stored.ScheduleName, "an ingested node belongs to no schedule")
	assert.Equal(t, ingestedKey, stored.UniqueID)
	assert.Equal(t, ingestedSchema, stored.SchemaName)
	assert.Equal(t, ingestedTable, stored.TableName)
	var readers int
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &readers, `
		SELECT count(*) FROM topology_edges
		 WHERE tenant_id = $1 AND release_id = $2 AND upstream_key = $3 AND downstream_key = $4`,
		events.DefaultTenantID, releaseID, ingestedKey, ingestProbeReaderKey))
	assert.Equal(t, 1, readers, "run-controller stores the edge from %s into %s", ingestedKey, ingestProbeReaderKey)
	t.Logf("✅ run-controller stores %s as a %s node with no service and no schedule", ingestedKey, stored.NodeType)

	// ── 3. catalog's graph holds it, and the reader depends on it ────────────
	require.Equal(t, ingestedKey, neo4jScalarString(ctx, clients,
		`MATCH (n:Node {tenant_id: 'default', unique_id: $uid, node_type: $type, active: true}) RETURN n.unique_id AS v`,
		map[string]any{"uid": ingestedKey, "type": string(model.NodeTypeIngested)}),
		"catalog holds an active :Node %s of type %s", ingestedKey, model.NodeTypeIngested)
	require.Equal(t, ingestedKey, neo4jScalarString(ctx, clients,
		`MATCH (:Node {tenant_id: 'default', unique_id: $reader, active: true})-[:DEPENDS_ON]->
		       (n:Node {tenant_id: 'default', unique_id: $uid, node_type: $type, active: true})
		 RETURN n.unique_id AS v`,
		map[string]any{"reader": ingestProbeReaderKey, "uid": ingestedKey, "type": string(model.NodeTypeIngested)}),
		"%s depends on the ingested node %s in catalog's graph", ingestProbeReaderKey, ingestedKey)
	t.Logf("✅ catalog holds the ingested :Node %s, and %s depends on it", ingestedKey, ingestProbeReaderKey)

	// ── 4. the ui's schedule graph draws it ──────────────────────────────────
	graph := mustGetJSON(t, fmt.Sprintf("%s/api/schedules/%s/graph", clients.uiBase, ingestProbeSchedule))
	nodeTypes := uiGraphNodeTypes(t, graph)
	require.Contains(t, nodeTypes, ingestedGraphNodeID,
		"the graph of %s holds the ingested node, whose id has no service: %v", ingestProbeSchedule, nodeTypes)
	assert.Equal(t, string(model.NodeTypeIngested), nodeTypes[ingestedGraphNodeID])
	require.Contains(t, nodeTypes, ingestProbeReaderGraphID, "the graph of %s holds the reader: %v", ingestProbeSchedule, nodeTypes)
	// The ui sends an edge in execution order: from the node that runs first to
	// the node that reads it.
	assert.True(t, uiGraphHasEdge(t, graph, ingestedGraphNodeID, ingestProbeReaderGraphID),
		"the graph of %s has the edge from %s to %s: %v", ingestProbeSchedule, ingestedGraphNodeID, ingestProbeReaderGraphID, graph["edges"])
	t.Logf("✅ ui /api/schedules/%s/graph draws %s (%s) with an edge into %s",
		ingestProbeSchedule, ingestedGraphNodeID, nodeTypes[ingestedGraphNodeID], ingestProbeReaderGraphID)

	// The release planned the promote-seed runs of its seeds in the transaction
	// that made it live. Wait for them, so the run below meets no Job of theirs
	// and the cleanup that deletes the run's Jobs removes none of theirs.
	waitForPromoteSeedRunsToEnd(t, ctx, clients, releaseID, 5*time.Minute)

	// ── 5. a run of the schedule plans the reader and never the table ────────
	started, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{
		RequestId: uuid.NewString(), ScheduleName: ingestProbeSchedule,
	})
	require.NoError(t, err, "StartRun(%s)", ingestProbeSchedule)
	runID, err := uuid.Parse(started.GetRunId())
	require.NoError(t, err, "StartRun must return the run's id")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, ingestProbeSchedule)
	t.Logf("StartRun(%s) created run %s", ingestProbeSchedule, runID)

	// run-controller writes every task of a run in the transaction that creates
	// it, so the rows are all there are.
	var plannedKeys []string
	require.NoError(t, clients.runControllerDB.SelectContext(ctx, &plannedKeys, `
		SELECT coalesce(node_key, '') FROM tasks WHERE tenant_id = $1 AND run_id = $2 ORDER BY node_key`,
		events.DefaultTenantID, runID))
	require.Equal(t, []string{ingestProbeReaderKey}, plannedKeys,
		"the run of %s holds one task, the reader's, and none for the ingested table", ingestProbeSchedule)

	var runStatus, failureReason string
	pollUntilDescribed(t, ctx, 5*time.Minute, 3*time.Second, func() (bool, error) {
		var reason sql.NullString
		if err := clients.runControllerDB.QueryRowContext(ctx,
			`SELECT status, failure_reason FROM runs WHERE tenant_id = $1 AND run_id = $2`,
			events.DefaultTenantID, runID).Scan(&runStatus, &reason); err != nil {
			return false, err
		}
		failureReason = reason.String
		switch runStatus {
		case "succeeded", "failed", "cancelled", "skipped":
			return true, nil
		}
		return false, nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for run %s of schedule %s to end: its last status is %q",
			runID, ingestProbeSchedule, runStatus)
	})
	attempts, attemptsErr := attemptRows(ctx, clients, taskOfRun(t, ctx, clients, runID, ingestProbeReaderTable))
	require.Equal(t, "succeeded", runStatus,
		"the run of %s succeeds: its Job selects from the outside table %s (failure_reason %q, attempts %v, attempts read error %v)",
		ingestProbeSchedule, ingestedKey, failureReason, attempts, attemptsErr)
	verifyNoJobsDeployed(t, ctx, []string{ingestedTable})
	t.Logf("✅ the run of %s planned only %s and succeeded; no Job was created for %s",
		ingestProbeSchedule, ingestProbeReaderKey, ingestedKey)

	// ── 6. a single-node run of the table is refused ─────────────────────────
	// With no service the request is incomplete: a node run names its service.
	noServiceID := uuid.NewString()
	_, err = clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
		RequestId: noServiceID, ServiceName: "", SchemaName: ingestedSchema, TableName: ingestedTable, MetadataSource: "latest",
	})
	require.Error(t, err, "a node run of %s with no service must be refused", ingestedKey)
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "a node run with no service is INVALID_ARGUMENT: %v", err)
	assert.Zero(t, commandRecordsOf(t, ctx, clients, noServiceID), "a refused command stores no record")

	// With the reader's service the request is complete, so the refusal is the
	// planner's: a node run never names an ingested node.
	namedServiceID := uuid.NewString()
	_, err = clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
		RequestId: namedServiceID, ServiceName: ingestProbeReaderService, SchemaName: ingestedSchema, TableName: ingestedTable, MetadataSource: "latest",
	})
	require.Error(t, err, "a node run of the ingested node %s must be refused", ingestedKey)
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "a node run of an ingested node is INVALID_ARGUMENT: %v", err)
	assert.Contains(t, status.Convert(err).Message(), "ingested", "the refusal names why: %v", err)
	assert.Zero(t, commandRecordsOf(t, ctx, clients, namedServiceID), "a refused command stores no record")
	t.Logf("✅ a single-node run of %s was refused with INVALID_ARGUMENT, with and without a service", ingestedKey)

	// ── 7. no run ever holds a task for the table ────────────────────────────
	// The schedule's run could not hold one whatever the planner did, since the
	// ingested node belongs to no schedule: only the task rows of every run of
	// the tenant show that no run plans it.
	var ingestedTasks int
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &ingestedTasks,
		`SELECT count(*) FROM tasks WHERE tenant_id = $1 AND node_key = $2`,
		events.DefaultTenantID, ingestedKey))
	assert.Zero(t, ingestedTasks, "no run of tenant %s holds a task for the ingested node %s", events.DefaultTenantID, ingestedKey)
	t.Logf("✅ no task row of any run names %s", ingestedKey)
}

// uiGraphNodeTypes returns the node type of each node of a ui schedule graph,
// by node id.
func uiGraphNodeTypes(t *testing.T, graph map[string]interface{}) map[string]string {
	t.Helper()
	raw, ok := graph["nodes"].([]interface{})
	require.True(t, ok, "the schedule graph has a 'nodes' list: %v", graph)
	types := make(map[string]string, len(raw))
	for _, item := range raw {
		node, ok := item.(map[string]interface{})
		require.True(t, ok, "a graph node is a JSON object: %v", item)
		id, _ := node["node_id"].(string)
		nodeType, _ := node["node_type"].(string)
		types[id] = nodeType
	}
	return types
}

// uiGraphHasEdge reports whether a ui schedule graph holds the edge from one
// node id to another.
func uiGraphHasEdge(t *testing.T, graph map[string]interface{}, from, to string) bool {
	t.Helper()
	raw, ok := graph["edges"].([]interface{})
	require.True(t, ok, "the schedule graph has an 'edges' list: %v", graph)
	for _, item := range raw {
		edge, ok := item.(map[string]interface{})
		require.True(t, ok, "a graph edge is a JSON object: %v", item)
		if edge["from_node_id"] == from && edge["to_node_id"] == to {
			return true
		}
	}
	return false
}
