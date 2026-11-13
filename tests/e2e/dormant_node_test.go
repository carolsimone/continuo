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
	// dormantProbeService owns dormant_probe, the model of service-1 that
	// carries the project's owner and no schedule tag.
	dormantProbeService = "service-1"
	// dormantProbeTable is the dormant model, and dormantProbeKey its node key
	// (<schema>.<table>, lower-case).
	dormantProbeTable = "dormant_probe"
	dormantProbeKey   = seedSchemaName + "." + dormantProbeTable
	// scheduledProbeTable is a model of the same service with a schedule tag
	// (rel-probe): the node a list of dormant nodes leaves out.
	scheduledProbeTable = "rel_probe"
)

// TestE2E_DormantNode_IsListedRunsByHandAndNeverRunsOnASchedule proves that a
// model with an owner and no schedule tag is a dormant node: part of the
// topology, listed before it ever ran, and run by hand. dormant_probe is a
// table below no seed, so no seed reload rebuilds it either, and no schedule's
// run executes it:
//
//  1. A bootstrap release of service-1 promotes. service-1's dormant_probe
//     model carries the project's owner and no tag.
//  2. run-controller's copy of the release holds it as a dbt model with an
//     empty schedule, which model.IsDormant calls dormant, and catalog's live
//     graph holds it, active, with an empty schedule.
//  3. Before any run of it, ListNodes with dormant_only lists it, dormant and
//     never run, and leaves out rel_probe, a model of a schedule, which
//     ListNodes without dormant_only lists as not dormant. Every node of the
//     unfiltered dormant list is dormant, and the ui's node list
//     (GET /api/nodes?dormant=1) lists it the same way. ListNodes lists no
//     node of e2e_raw.ingest_probe, the release's unknown-external node.
//     ListScheduleTopologies counts the release's dormant nodes as
//     model.IsDormant counts them.
//  4. StartRun with no schedule name is refused, and no schedule ListSchedules
//     lists has an empty name, so no run selects the nodes of no schedule as
//     its own (a run whose load of a seed drops its table rebuilds a dormant
//     view below that seed, and dormant_probe is no such view).
//     StartRun of every schedule of the release creates a run with no task
//     for the node; each run is cancelled as soon as it is created, since its
//     tasks are written with it, and a ResourceQuota keeps every pod from
//     being created while the runs exist, so none of them builds a model. No
//     run of the tenant then holds a task for the node.
//  5. TriggerSingleNodeRun of it creates a run of exactly its task, which
//     succeeds, and ListNodes then lists it with that run, still dormant.
//
// The release leaves the live topology as service-1's alone and starts the
// promote-seed runs of its seeds, so a deferred restore announces the default
// e2e DAG again and waits for those runs to end, the way
// TestE2E_UnknownExternalNode_ShowsInTheGraphAndNeverRuns does.
func TestE2E_DormantNode_IsListedRunsByHandAndNeverRunsOnASchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: the promotion (5m) + run-controller's copy (2m) + the seed
	// loads of the release (5m) + the pod quota taking effect (1m) + the end of
	// the schedules' runs (3m, since they are cancelled together and end
	// together) + the single-node run (5m) = 21m. The restore and the removal
	// of the schedules' runs have contexts of their own.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-dormant-" + uuid.NewString()[:8]
	t.Logf("release_id=%s changed_service=%s dormant_node=%s", releaseID, dormantProbeService, dormantProbeKey)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	imageTag := allServices[dormantProbeService].imageTag
	require.NotEmpty(t, imageTag, "image_tag missing for %s", dormantProbeService)
	inBaseline := false
	for _, n := range allServices[dormantProbeService].nodes {
		inBaseline = inBaseline || n.uniqueID == dormantProbeKey
	}
	require.True(t, inBaseline,
		"topology-controller's parser yields no node %s from %s's baseline manifest — is the model in service-1, "+
			"the image rebuilt and the manifests re-uploaded, and does the parser keep a model with an owner and no tag?",
		dormantProbeKey, dormantProbeService)

	// Only this test runs the node, so a run of it is one an earlier attempt of
	// the test left behind: it is removed, and no other run may hold a task for
	// the node.
	removeLeftoverDormantProbeRuns(t, ctx, clients)
	require.Zero(t, tasksOfDormantProbe(t, ctx, clients),
		"no run of tenant %s holds a task for %s before the test runs it", events.DefaultTenantID, dormantProbeKey)

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

	postRelease(t, clients, dormantProbeService, releaseID, imageTag, true)

	pollUntil(t, ctx, 5*time.Minute, 2*time.Second, func() (bool, error) {
		return neo4jScalarString(ctx, clients,
			`MATCH (m:Meta {tenant_id: 'default', key: 'current_release'}) RETURN m.release_id AS v`, nil) == releaseID, nil
	}, "timeout waiting for release "+releaseID+" to promote + swap catalog's topology")

	// catalog and run-controller apply a promotion through groups of their own,
	// so run-controller's copy is awaited too. It stores a release's nodes and
	// moves its live release in one transaction, so a live release that names
	// this one holds every node of it.
	waitForRunControllerToApply(t, ctx, clients, releaseID, "", 2*time.Minute)

	// ── 2. run-controller and catalog hold it with no schedule ───────────────
	var stored struct {
		ServiceName  string `db:"service_name"`
		NodeType     string `db:"node_type"`
		ScheduleName string `db:"schedule_name"`
	}
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &stored, `
		SELECT service_name, node_type, schedule_name
		  FROM topology_nodes
		 WHERE tenant_id = $1 AND release_id = $2 AND node_key = $3`,
		events.DefaultTenantID, releaseID, dormantProbeKey),
		"run-controller's copy of release %s holds no node %s", releaseID, dormantProbeKey)
	assert.Equal(t, dormantProbeService, stored.ServiceName, "service-1 builds the dormant node")
	assert.Equal(t, string(model.NodeTypeDbtModel), stored.NodeType, "the dormant node is a dbt model")
	assert.Empty(t, stored.ScheduleName, "a model with no tag belongs to no schedule")
	require.True(t, model.IsDormant(model.NodeType(stored.NodeType), stored.ScheduleName),
		"%s, of type %q and schedule %q, is dormant", dormantProbeKey, stored.NodeType, stored.ScheduleName)
	require.Equal(t, dormantProbeKey, neo4jScalarString(ctx, clients,
		`MATCH (n:Node {tenant_id: 'default', unique_id: $uid, active: true, schedule_name: ''}) RETURN n.unique_id AS v`,
		map[string]any{"uid": dormantProbeKey}),
		"catalog holds an active :Node %s with an empty schedule", dormantProbeKey)
	t.Logf("✅ run-controller and catalog hold %s as a %s node with no schedule", dormantProbeKey, stored.NodeType)

	// ── 3. it is listed as dormant before any run ────────────────────────────
	before := listNodesOfDormantProbeService(t, ctx, clients, dormantProbeTable, true)
	require.Len(t, before.GetNodes(), 1,
		"ListNodes(dormant_only) lists %s before its first run: %v", dormantProbeKey, before.GetNodes())
	assert.EqualValues(t, 1, before.GetTotalCount(), "the total counts the one node the filters match")
	node := before.GetNodes()[0]
	assert.Equal(t, dormantProbeService, node.GetServiceName())
	assert.Equal(t, seedSchemaName, node.GetSchemaName())
	assert.Equal(t, dormantProbeTable, node.GetTableName())
	assert.Equal(t, string(model.NodeTypeDbtModel), node.GetNodeType(), "the node list carries the node's type")
	assert.True(t, node.GetDormant(), "the node list marks %s dormant", dormantProbeKey)
	assert.Zero(t, node.GetTaskCount(), "a node that never ran has no task")
	assert.Empty(t, node.GetLastStatus(), "a node that never ran has no last status")
	assert.Empty(t, node.GetLastRunAt(), "a node that never ran has no last run")

	// The control: a model of a schedule is listed, not dormant, and the
	// dormant filter leaves it out.
	scheduled := listNodesOfDormantProbeService(t, ctx, clients, scheduledProbeTable, false)
	require.Len(t, scheduled.GetNodes(), 1, "ListNodes lists %s, a model of a schedule: %v", scheduledProbeTable, scheduled.GetNodes())
	assert.False(t, scheduled.GetNodes()[0].GetDormant(), "a model of a schedule is not dormant")
	assert.Empty(t, listNodesOfDormantProbeService(t, ctx, clients, scheduledProbeTable, true).GetNodes(),
		"ListNodes(dormant_only) leaves out %s, a model of a schedule", scheduledProbeTable)

	everyDormant, err := clients.runClient.ListNodes(ctx, &runv1.ListNodesRequest{DormantOnly: true, Limit: 200})
	require.NoError(t, err, "ListNodes(dormant_only) with no other filter")
	listed := false
	for _, n := range everyDormant.GetNodes() {
		assert.True(t, n.GetDormant(), "ListNodes(dormant_only) lists only dormant nodes, not %s.%s.%s",
			n.GetServiceName(), n.GetSchemaName(), n.GetTableName())
		listed = listed || (n.GetServiceName() == dormantProbeService && n.GetTableName() == dormantProbeTable)
	}
	assert.True(t, listed, "ListNodes(dormant_only) with no other filter lists %s: %v", dormantProbeKey, everyDormant.GetNodes())

	// The live release holds e2e_raw.ingest_probe, the table service-1's
	// ingest_probe_reader reads and no model builds, as an unknown-external
	// node, and the node list never lists it.
	var externalType string
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &externalType, `
		SELECT node_type FROM topology_nodes WHERE tenant_id = $1 AND release_id = $2 AND node_key = $3`,
		events.DefaultTenantID, releaseID, unknownExternalKey),
		"release %s holds the unknown-external node %s", releaseID, unknownExternalKey)
	require.Equal(t, string(model.NodeTypeUnknownExternal), externalType, "%s is an unknown-external node", unknownExternalKey)
	external, err := clients.runClient.ListNodes(ctx, &runv1.ListNodesRequest{Search: unknownExternalTable})
	require.NoError(t, err, "ListNodes(search %s)", unknownExternalTable)
	assert.Empty(t, external.GetNodes(), "ListNodes lists no unknown-external node: %v", external.GetNodes())
	assert.Zero(t, external.GetTotalCount(), "ListNodes counts no unknown-external node")

	// The ui's node list, which the Nodes tab reads.
	uiList := mustGetJSON(t, fmt.Sprintf("%s/api/nodes?search=%s&service=%s&dormant=1",
		clients.uiBase, dormantProbeTable, dormantProbeService))
	uiNodes, ok := uiList["nodes"].([]interface{})
	require.True(t, ok, "GET /api/nodes: 'nodes' field missing or wrong type: %v", uiList)
	require.Len(t, uiNodes, 1, "GET /api/nodes?dormant=1 lists %s before its first run: %v", dormantProbeKey, uiNodes)
	uiNode, ok := uiNodes[0].(map[string]interface{})
	require.True(t, ok, "a node list entry is a JSON object: %v", uiNodes[0])
	assert.Equal(t, dormantProbeTable, uiNode["table_name"])
	assert.Equal(t, true, uiNode["dormant"], "the ui marks %s dormant", dormantProbeKey)
	assert.Equal(t, float64(0), uiNode["task_count"], "the ui counts no task of a node that never ran")
	assert.Nil(t, uiNode["last_run_at"], "the ui shows no last run of a node that never ran")

	// The topology tiles count the release's dormant nodes as model.IsDormant
	// counts them over run-controller's copy.
	var releaseNodes []struct {
		NodeType     string `db:"node_type"`
		ScheduleName string `db:"schedule_name"`
	}
	require.NoError(t, clients.runControllerDB.SelectContext(ctx, &releaseNodes, `
		SELECT node_type, schedule_name FROM topology_nodes WHERE tenant_id = $1 AND release_id = $2`,
		events.DefaultTenantID, releaseID))
	dormantNodes := 0
	for _, n := range releaseNodes {
		if model.IsDormant(model.CanonicalNodeType(n.NodeType), n.ScheduleName) {
			dormantNodes++
		}
	}
	tiles, err := clients.runClient.ListScheduleTopologies(ctx, &runv1.ListScheduleTopologiesRequest{})
	require.NoError(t, err, "ListScheduleTopologies")
	assert.EqualValues(t, dormantNodes, tiles.GetDormantNodeCount(),
		"ListScheduleTopologies counts the %d dormant nodes of release %s", dormantNodes, releaseID)
	for _, s := range tiles.GetSchedules() {
		assert.NotEmpty(t, s.GetScheduleName(), "no topology tile is a schedule with an empty name")
	}
	t.Logf("✅ %s is listed dormant before any run, by ListNodes and the ui; the release holds %d dormant node(s)",
		dormantProbeKey, dormantNodes)

	// The release planned the promote-seed runs of its seeds in the transaction
	// that made it live. Wait for them, and for any other release's, so the
	// runs below meet no Job of theirs and the pod quota below stops none.
	waitForPromoteSeedRunsToEnd(t, ctx, clients, "", 5*time.Minute)

	// ── 4. no run of a schedule holds a task for it ──────────────────────────
	// A run names its schedule, and no schedule has an empty name.
	noScheduleID := uuid.NewString()
	_, err = clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: noScheduleID, ScheduleName: ""})
	require.Error(t, err, "StartRun with no schedule name must be refused")
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "StartRun with no schedule name is INVALID_ARGUMENT: %v", err)
	assert.Zero(t, commandRecordsOf(t, ctx, clients, noScheduleID), "a refused command stores no record")
	listedSchedules, err := clients.runClient.ListSchedules(ctx, &runv1.ListSchedulesRequest{})
	require.NoError(t, err, "ListSchedules")
	for _, s := range listedSchedules.GetSchedules() {
		assert.NotEmpty(t, s.GetScheduleName(), "ListSchedules lists no schedule with an empty name")
	}

	var schedules []string
	require.NoError(t, clients.runControllerDB.SelectContext(ctx, &schedules, `
		SELECT DISTINCT schedule_name FROM topology_nodes
		 WHERE tenant_id = $1 AND release_id = $2 AND schedule_name <> ''
		 ORDER BY schedule_name`,
		events.DefaultTenantID, releaseID))
	require.NotEmpty(t, schedules, "release %s holds the schedules of service-1's models", releaseID)

	// No pod can be created in the Jobs' namespace while the schedules' runs
	// exist: a Job execution-controller creates for one of them before the
	// cancel reaches it gets no pod, so these runs build no model. The quota
	// is removed once the runs have ended, and when the test ends however it
	// ends. A promote-seed run of any release still going would lose its pods
	// to the quota too, so the test requires that none is.
	requireNoActivePromoteSeedRun(t, ctx, clients)
	blockPodCreation(t, ctx)

	var scheduleRuns []uuid.UUID
	defer func() { removeRuns(t, clients, scheduleRuns) }()
	for _, schedule := range schedules {
		started, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{
			RequestId: uuid.NewString(), ScheduleName: schedule,
		})
		require.NoError(t, err, "StartRun(%s)", schedule)
		runID, err := uuid.Parse(started.GetRunId())
		require.NoError(t, err, "StartRun must return the run's id")
		scheduleRuns = append(scheduleRuns, runID)

		// run-controller writes every task of a run in the transaction that
		// creates it, so the run is cancelled at once: its rows are all there
		// are, and none of its Jobs needs to run.
		_, err = clients.runClient.CancelRun(ctx, &runv1.CancelRunRequest{
			RunId: runID.String(), CancelledBy: "e2e", CancellationReason: "the dormant-node e2e test reads only the run's tasks",
		})
		require.NoError(t, err, "CancelRun(%s) of schedule %s", runID, schedule)

		var plannedKeys []string
		require.NoError(t, clients.runControllerDB.SelectContext(ctx, &plannedKeys, `
			SELECT coalesce(node_key, '') FROM tasks WHERE tenant_id = $1 AND run_id = $2 ORDER BY node_key`,
			events.DefaultTenantID, runID))
		require.NotEmpty(t, plannedKeys, "the run of %s holds a task for each of its nodes", schedule)
		assert.NotContains(t, plannedKeys, dormantProbeKey,
			"the run of %s holds no task for the dormant node %s", schedule, dormantProbeKey)
		t.Logf("StartRun(%s) created run %s with the tasks %v", schedule, runID, plannedKeys)
	}
	for _, runID := range scheduleRuns {
		verifySchedulerCancelled(t, ctx, clients, runID)
	}
	unblockPodCreation(t)
	require.Zero(t, tasksOfDormantProbe(t, ctx, clients),
		"no run of tenant %s holds a task for the dormant node %s", events.DefaultTenantID, dormantProbeKey)
	t.Logf("✅ runs of the %d schedules %v hold no task for %s", len(schedules), schedules, dormantProbeKey)

	// ── 5. a single-node run of it succeeds ──────────────────────────────────
	triggered, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
		RequestId: uuid.NewString(), ServiceName: dormantProbeService, SchemaName: seedSchemaName, TableName: dormantProbeTable,
		MetadataSource: "latest",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(%s)", dormantProbeKey)
	require.NotEmpty(t, triggered.GetScheduleName(), "TriggerSingleNodeRun names the run's schedule")
	runID, err := uuid.Parse(triggered.GetRunId())
	require.NoError(t, err, "TriggerSingleNodeRun must return the run's id")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, triggered.GetScheduleName())
	t.Logf("TriggerSingleNodeRun(%s) created run %s", dormantProbeKey, runID)

	var plannedKeys []string
	require.NoError(t, clients.runControllerDB.SelectContext(ctx, &plannedKeys, `
		SELECT coalesce(node_key, '') FROM tasks WHERE tenant_id = $1 AND run_id = $2 ORDER BY node_key`,
		events.DefaultTenantID, runID))
	require.Equal(t, []string{dormantProbeKey}, plannedKeys, "the single-node run holds one task, the dormant node's")
	assert.Equal(t, "single_node_run", queryRunKind(t, clients.runControllerDB, runID))

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
		return fmt.Sprintf("timeout waiting for the single-node run %s of %s to end: its last status is %q",
			runID, dormantProbeKey, runStatus)
	})
	attempts, attemptsErr := attemptRows(ctx, clients, taskOfRun(t, ctx, clients, runID, dormantProbeTable))
	require.Equal(t, "succeeded", runStatus,
		"the single-node run of %s succeeds (failure_reason %q, attempts %v, attempts read error %v)",
		dormantProbeKey, failureReason, attempts, attemptsErr)

	after := listNodesOfDormantProbeService(t, ctx, clients, dormantProbeTable, true)
	require.Len(t, after.GetNodes(), 1, "ListNodes(dormant_only) lists %s after its run: %v", dormantProbeKey, after.GetNodes())
	ran := after.GetNodes()[0]
	assert.True(t, ran.GetDormant(), "a run started by hand leaves %s dormant", dormantProbeKey)
	assert.EqualValues(t, 1, ran.GetTaskCount(), "the node list counts the single-node run's task")
	assert.Equal(t, "succeeded", ran.GetLastStatus(), "the node list shows the run's status")
	assert.NotEmpty(t, ran.GetLastRunAt(), "the node list shows when the run was created")
	assert.Equal(t, 1, tasksOfDormantProbe(t, ctx, clients),
		"the single-node run's task is the only task of %s", dormantProbeKey)
	t.Logf("✅ the single-node run %s of %s succeeded, and the node is still listed dormant", runID, dormantProbeKey)
}

// listNodesOfDormantProbeService lists the nodes of service-1 whose table is
// table through ListNodes, only the dormant ones when dormantOnly is set.
func listNodesOfDormantProbeService(t *testing.T, ctx context.Context, clients *testClients, table string, dormantOnly bool) *runv1.ListNodesResponse {
	t.Helper()
	resp, err := clients.runClient.ListNodes(ctx, &runv1.ListNodesRequest{
		Search: table, ServiceName: dormantProbeService, DormantOnly: dormantOnly,
	})
	require.NoError(t, err, "ListNodes(search %s, service %s, dormant_only %t)", table, dormantProbeService, dormantOnly)
	return resp
}

// tasksOfDormantProbe counts the tasks any run of the default tenant holds for
// the dormant node.
func tasksOfDormantProbe(t *testing.T, ctx context.Context, clients *testClients) int {
	t.Helper()
	var n int
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &n, `
		SELECT count(*) FROM tasks
		 WHERE tenant_id = $1 AND service_name = $2 AND schema_name = $3 AND table_name = $4`,
		events.DefaultTenantID, dormantProbeService, seedSchemaName, dormantProbeTable))
	return n
}

// removeLeftoverDormantProbeRuns removes each single-node run that holds a
// task for the dormant node, with its tasks, its outbox rows and the dbt Jobs.
// Only the dormant-node test starts such a run, and it removes its own when it
// ends, so one found here was left by an attempt that stopped before then.
func removeLeftoverDormantProbeRuns(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	var leftovers []struct {
		RunID        uuid.UUID `db:"run_id"`
		ScheduleName string    `db:"schedule_name"`
	}
	require.NoError(t, clients.runControllerDB.SelectContext(ctx, &leftovers, `
		SELECT DISTINCT r.run_id, r.schedule_name
		  FROM runs r
		  JOIN tasks t ON t.tenant_id = r.tenant_id AND t.run_id = r.run_id
		 WHERE r.tenant_id = $1 AND r.kind = 'single_node_run'
		   AND t.service_name = $2 AND t.schema_name = $3 AND t.table_name = $4`,
		events.DefaultTenantID, dormantProbeService, seedSchemaName, dormantProbeTable))
	for _, run := range leftovers {
		cleanupSingleNodeRun(t, ctx, clients, run.RunID, run.ScheduleName)
	}
}

// removeRuns puts the stack back after runs a test started: it cancels each
// run of runIDs that has not ended, waits up to three minutes for all of them
// to end, then deletes each run with its tasks (their attempts go with them
// through the foreign key's cascade) and its outbox rows, and deletes the dbt
// Jobs. It runs on a context of its own, because the test's may have expired
// by the time a cleanup runs, and it fails nothing: a run still active after
// the wait is logged and deleted all the same.
func removeRuns(t *testing.T, clients *testClients, runIDs []uuid.UUID) {
	t.Helper()
	if len(runIDs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	active := func() []uuid.UUID {
		var ids []uuid.UUID
		for _, id := range runIDs {
			var runStatus string
			err := clients.runControllerDB.GetContext(ctx, &runStatus,
				`SELECT status FROM runs WHERE tenant_id = $1 AND run_id = $2`, events.DefaultTenantID, id)
			if err == nil && (runStatus == "pending" || runStatus == "running" || runStatus == "cancelling") {
				ids = append(ids, id)
			}
		}
		return ids
	}
	for _, id := range active() {
		if _, err := clients.runClient.CancelRun(ctx, &runv1.CancelRunRequest{
			RunId: id.String(), CancelledBy: "e2e", CancellationReason: "the test that started the run ended",
		}); err != nil {
			t.Logf("cancel run %s: %v", id, err)
		}
	}
	deadline := time.Now().Add(3 * time.Minute)
	for remaining := active(); len(remaining) > 0; remaining = active() {
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Logf("runs %v are still active after the wait; deleting them all the same", remaining)
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}

	for _, id := range runIDs {
		if _, err := clients.runControllerDB.ExecContext(ctx, `DELETE FROM tasks WHERE run_id = $1`, id); err != nil {
			t.Logf("delete the tasks of run %s: %v", id, err)
		}
		if _, err := clients.runControllerDB.ExecContext(ctx, `DELETE FROM runs WHERE run_id = $1`, id); err != nil {
			t.Logf("delete run %s: %v", id, err)
		}
		if _, err := clients.runControllerDB.ExecContext(ctx, `DELETE FROM run_controller_outbox WHERE aggregate_id = $1`, id); err != nil {
			t.Logf("delete the run_controller_outbox rows of run %s: %v", id, err)
		}
	}
	cleanupK8s(t, ctx)
}

// requireNoActivePromoteSeedRun fails the test, naming each run, when a
// promote-seed run of any release is still active. The e2e-no-pods quota
// stops every pod in the Jobs' namespace, whatever run it belongs to, so such
// a run would fail its attempt as timeout start_timeout, which run-controller
// never retries.
func requireNoActivePromoteSeedRun(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	active, err := activePromoteSeedRuns(ctx, clients, "")
	require.NoError(t, err)
	require.Empty(t, active,
		"promote-seed run(s) are still active, and the e2e-no-pods quota would stop their pods: %v", active)
}
