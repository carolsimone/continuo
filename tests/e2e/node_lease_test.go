package e2e

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	// leaseService and leaseTable name the node both runs of the lease test
	// write: service-1's table_a, a node of e2e-schedule that reads
	// seed_table_1.
	leaseService = "service-1"
	leaseTable   = "table_a"
	// leaseNodeKey is that node's key in state's node_leases: "<schema>.<table>",
	// lowercased.
	leaseNodeKey = testSchemaName + "." + leaseTable

	// nodeLeaseWait is the wait_reason state's reads carry for a pending task
	// whose node another run's task holds.
	nodeLeaseWait = "node_lease"
)

// TestE2E_NodeLease_ASingleNodeRunWaitsForTheScheduledWriter proves that two
// runs never write one node at once:
//
//	StartRun(e2e-schedule) → the schedule run's table_a takes the node's
//	write lease when it is dispatched, and runs
//	→ TriggerSingleNodeRun(table_a) while it holds the lease → the single-node
//	  run's task is pending: ListTasks and GetRunGraph show
//	  wait_reason node_lease, the schedule run as the run it waits on, and since
//	  when
//	→ the schedule run's table_a succeeds and releases the lease in that report's
//	  transaction → the lease waker grants the node to the waiting task and
//	  dispatches it
//	→ the single-node run succeeds and releases the lease in turn, and the pod
//	  of its table_a started only once the schedule run's pod of table_a had
//	  finished. Neither lease is left for the waker's repair to delete.
//
// The single-node run is created while the schedule run's table_a holds the
// node's lease (from its dispatch until the report that ends it), so the test
// depends on that task holding it for longer than the trigger and the two
// reads take: a dbt model's container runs for several seconds, the reads a
// few milliseconds. When the schedule run's table_a has already ended by then, the
// failure says so.
func TestE2E_NodeLease_ASingleNodeRunWaitsForTheScheduledWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Topology (2m) + the schedule run's seeds and table_a on a cold stack (8m) +
	// table_a's end (3m) + the single-node run (7m) + the rest of the schedule run
	// (5m), with headroom.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)
	defer cleanupTestData(t, ctx, clients, testScheduleName)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, testScheduleName)

	waitForStateLiveRelease(t, ctx, clients, seedTopology(t, ctx, clients))
	waitForNodeUnleased(t, ctx, clients, leaseNodeKey)
	repairedBefore := leasesRepaired(t)

	// ── The schedule run's table_a takes the lease and runs ──────────────────────
	scheduleRunResp, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(), ScheduleName: testScheduleName})
	require.NoError(t, err, "StartRun(%s)", testScheduleName)
	scheduleRunID, err := uuid.Parse(scheduleRunResp.RunId)
	require.NoError(t, err, "StartRun must return the run's id")
	t.Logf("schedule run %s of %s started", scheduleRunID, testScheduleName)
	scheduleRunTaskID := taskOfRun(t, ctx, clients, scheduleRunID, leaseTable)

	scheduleRunStatus := waitForLeaseHeldBy(t, ctx, clients, leaseNodeKey, scheduleRunTaskID, "the schedule run's "+leaseTable, 8*time.Minute)
	t.Logf("the schedule run's %s holds the node's lease (task status %s)", leaseTable, scheduleRunStatus)

	// ── A single-node run of the same node waits for it ──────────────────────
	snResp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
		ServiceName:    leaseService,
		SchemaName:     testSchemaName,
		TableName:      leaseTable,
		MetadataSource: "latest",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(%s)", leaseTable)
	snID, err := uuid.Parse(snResp.RunId)
	require.NoError(t, err, "TriggerSingleNodeRun must return the run's id")
	defer cleanupSingleNodeRun(t, ctx, clients, snID, snResp.ScheduleName)
	t.Logf("single-node run %s of %s triggered", snID, leaseTable)

	// state plans the run and makes its task wait in the transaction that
	// creates it, so the wait is readable once the trigger has returned. The
	// run a task waits on is read from the node's lease when the task is read,
	// so both reads are made at once, while the schedule run's table_a still runs.
	waitingTask, err := readLeaseTask(ctx, clients, snID)
	require.NoError(t, err)
	graph, err := clients.runClient.GetRunGraph(ctx, &runv1.GetRunGraphRequest{RunId: snID.String()})
	require.NoError(t, err, "GetRunGraph(%s)", snID)
	scheduleRunAfter, err := taskStatus(ctx, clients, scheduleRunTaskID)
	require.NoError(t, err, "read the schedule run's %s", leaseTable)
	window := fmt.Sprintf("the schedule run's %s was %s right after these reads: while it still runs, a task that does not wait is a lease defect; "+
		"once it has ended, the test missed the window in which the single-node run had to wait", leaseTable, scheduleRunAfter)

	require.Equal(t, "pending", waitingTask.GetStatus(),
		"the single-node run's task is not dispatched while the schedule run writes the node (%s)", window)
	require.Equal(t, nodeLeaseWait, waitingTask.GetWaitReason(),
		"the single-node run's task waits for the node's lease (%s)", window)
	require.Equal(t, scheduleRunID.String(), waitingTask.GetWaitingOnRunId(),
		"the single-node run's task names the schedule run as the run it waits on (%s)", window)
	require.NotNil(t, waitingTask.GetWaitingSince(), "the single-node run's task says since when it waits")

	graphNode := runGraphNodeOf(t, graph, leaseTable)
	require.Equal(t, "pending", graphNode.GetStatus(), "the run graph shows the waiting task pending (%s)", window)
	require.Equal(t, nodeLeaseWait, graphNode.GetWaitReason(), "the run graph shows why the task waits (%s)", window)
	require.Equal(t, scheduleRunID.String(), graphNode.GetWaitingOnRunId(), "the run graph names the run the task waits on (%s)", window)
	require.NotEmpty(t, graphNode.GetWaitingSince(), "the run graph says since when the task waits")
	t.Logf("✅ the single-node run's %s waits for the node (wait_reason %s, waiting on run %s since %s)",
		leaseTable, waitingTask.GetWaitReason(), waitingTask.GetWaitingOnRunId(), graphNode.GetWaitingSince())

	// ── The schedule run's success frees the node; the waiting task dispatches ──
	scheduleRunEnded := waitForTaskEnded(t, ctx, clients, scheduleRunTaskID, "the schedule run's "+leaseTable, 3*time.Minute)
	require.Equal(t, "succeeded", scheduleRunEnded.status, "the schedule run's %s succeeds", leaseTable)
	requireNoLeaseOf(t, ctx, clients, scheduleRunTaskID, "the report that ended the schedule run's "+leaseTable+" released its lease")

	// The report that ended the schedule run's task released the lease, and the waker,
	// woken by that release or by its pass every 60 s, grants the node.
	var dispatched *runv1.Task
	pollUntilDescribed(t, ctx, 2*time.Minute, 500*time.Millisecond, func() (bool, error) {
		task, err := readLeaseTask(ctx, clients, snID)
		if err != nil {
			return false, err
		}
		dispatched = task
		return task.GetStatus() != "pending", nil
	}, func() string {
		return fmt.Sprintf("the single-node run's %s was not dispatched once the schedule run's had succeeded; last read: status %s, wait_reason %q, waiting on run %q",
			leaseTable, dispatched.GetStatus(), dispatched.GetWaitReason(), dispatched.GetWaitingOnRunId())
	})
	require.Empty(t, dispatched.GetWaitReason(), "a dispatched task no longer waits")
	require.Empty(t, dispatched.GetWaitingOnRunId(), "a dispatched task waits on no run")
	require.Nil(t, dispatched.GetWaitingSince(), "a dispatched task has no wait time")
	t.Logf("✅ the single-node run's %s was dispatched (%s) once the schedule run's succeeded", leaseTable, dispatched.GetStatus())

	snTaskID, err := uuid.Parse(dispatched.GetTaskId())
	require.NoError(t, err, "the single-node run's task carries its id")
	snEnded := waitForTaskEnded(t, ctx, clients, snTaskID, "the single-node run's "+leaseTable, 5*time.Minute)
	require.Equal(t, "succeeded", snEnded.status, "the single-node run's %s succeeds", leaseTable)
	requireNoLeaseOf(t, ctx, clients, snTaskID, "the report that ended the single-node run's "+leaseTable+" released its lease")
	verifySchedulerSucceeded(t, ctx, clients, snID)

	// ── The two Jobs ran one after the other ─────────────────────────────────
	scheduleRunPods := taskPodInterval(t, ctx, scheduleRunTaskID, "the schedule run's "+leaseTable)
	snPods := taskPodInterval(t, ctx, snTaskID, "the single-node run's "+leaseTable)
	t.Logf("the schedule run's %s ran in pods %v from %s to %s; the single-node run's in pods %v from %s to %s",
		leaseTable, scheduleRunPods.pods, scheduleRunPods.start.Format(time.RFC3339), scheduleRunPods.finish.Format(time.RFC3339),
		snPods.pods, snPods.start.Format(time.RFC3339), snPods.finish.Format(time.RFC3339))
	// Kubernetes truncates both times to the second, which keeps their order, so
	// an equal second is not an overlap.
	require.False(t, snPods.start.Before(scheduleRunPods.finish),
		"the single-node run's pod of %s started at %s, before the schedule run's finished at %s: two runs wrote the node at once",
		leaseTable, snPods.start.Format(time.RFC3339), scheduleRunPods.finish.Format(time.RFC3339))
	t.Logf("✅ the single-node run's %s started only after the schedule run's had finished", leaseTable)

	// ── The schedule run goes on and ends; nothing holds the node ───────────────
	var allTables []string
	for _, level := range getDAGLevels() {
		allTables = append(allTables, level...)
	}
	waitForAllTasksSucceeded(t, ctx, clients, scheduleRunID, allTables)
	verifySchedulerSucceeded(t, ctx, clients, scheduleRunID)
	var held int
	require.NoError(t, clients.stateDB.GetContext(ctx, &held,
		`SELECT count(*) FROM node_leases WHERE tenant_id = $1 AND node_key = $2`, events.DefaultTenantID, leaseNodeKey))
	require.Zero(t, held, "both tasks of %s released the node's lease when they ended", leaseTable)
	require.Equal(t, repairedBefore, leasesRepaired(t),
		"the lease waker repaired no lease: every task released its own in the report that ended it")
	t.Logf("✅ both runs succeeded and no task holds %s's lease", leaseTable)
}

// leasesRepairedPattern captures the value of state's
// continuo_node_leases_repaired_total, with or without labels.
var leasesRepairedPattern = regexp.MustCompile(`(?m)^continuo_node_leases_repaired_total(?:\{[^}]*\})? (\S+)$`)

// leasesRepaired returns state's continuo_node_leases_repaired_total: how many
// leases its lease waker has deleted because their task ended without
// releasing them.
func leasesRepaired(t *testing.T) float64 {
	t.Helper()
	m := leasesRepairedPattern.FindStringSubmatch(scrapeMetrics(t, getEnv("STATE_HOST", "state")))
	require.NotNil(t, m, "state exposes no continuo_node_leases_repaired_total")
	v, err := strconv.ParseFloat(m[1], 64)
	require.NoError(t, err, "parse continuo_node_leases_repaired_total %q", m[1])
	return v
}

// requireNoLeaseOf fails the test when taskID holds a node lease.
func requireNoLeaseOf(t *testing.T, ctx context.Context, clients *testClients, taskID uuid.UUID, msg string) {
	t.Helper()
	var held int
	require.NoError(t, clients.stateDB.GetContext(ctx, &held, `SELECT count(*) FROM node_leases WHERE task_id = $1`, taskID))
	require.Zero(t, held, msg)
}

// waitForNodeUnleased waits until no task holds the write lease on nodeKey, so
// the test's own runs are the only writers of the node; a lease that stays
// held fails the test naming its holder.
func waitForNodeUnleased(t *testing.T, ctx context.Context, clients *testClients, nodeKey string) {
	t.Helper()
	holder := ""
	pollUntilDescribed(t, ctx, 2*time.Minute, time.Second, func() (bool, error) {
		var holders []string
		if err := clients.stateDB.SelectContext(ctx, &holders, `
			SELECT 'task ' || task_id::text || ' of run ' || run_id::text
			  FROM node_leases WHERE tenant_id = $1 AND node_key = $2`,
			events.DefaultTenantID, nodeKey); err != nil {
			return false, err
		}
		holder = strings.Join(holders, ", ")
		return len(holders) == 0, nil
	}, func() string {
		return fmt.Sprintf("the write lease on %s is still held by %s, which an earlier test left running", nodeKey, holder)
	})
}

// taskStatus reads taskID's tasks status.
func taskStatus(ctx context.Context, clients *testClients, taskID uuid.UUID) (string, error) {
	var status string
	err := clients.stateDB.GetContext(ctx, &status, `SELECT status FROM tasks WHERE task_id = $1`, taskID)
	return status, err
}

// waitForLeaseHeldBy waits until taskID holds the write lease on nodeKey while
// it is dispatched, queued or running, and returns its status then. The task
// takes the lease in the transaction that dispatches it, so it holds it from
// dispatched on, whether or not a read ever sees it running. A task that ends
// first fails the test: no run could then be created while it writes the node.
func waitForLeaseHeldBy(t *testing.T, ctx context.Context, clients *testClients, nodeKey string, taskID uuid.UUID, label string, timeout time.Duration) string {
	t.Helper()
	status, holder := "(not read)", "(not read)"
	ended := false
	pollUntilDescribed(t, ctx, timeout, 250*time.Millisecond, func() (bool, error) {
		var row struct {
			Status string `db:"status"`
			Holder string `db:"holder"`
		}
		if err := clients.stateDB.GetContext(ctx, &row, `
			SELECT t.status, COALESCE(l.task_id::text, '') AS holder
			  FROM tasks t
			  LEFT JOIN node_leases l ON l.tenant_id = $2 AND l.node_key = $3
			 WHERE t.task_id = $1`, taskID, events.DefaultTenantID, nodeKey); err != nil {
			return false, err
		}
		status, holder = row.Status, row.Holder
		switch status {
		case "succeeded", "failed", "cancelled", "skipped":
			ended = true
			return true, nil
		case "dispatched", "queued", "running":
			return holder == taskID.String(), nil
		}
		return false, nil
	}, func() string {
		return fmt.Sprintf("%s (task %s) did not hold the lease on %s within %s; last status %q, lease holder %q",
			label, taskID, nodeKey, timeout, status, holder)
	})
	require.False(t, ended,
		"%s (task %s) ended %s before the test saw it hold the lease on %s, so no run could be created while it wrote the node",
		label, taskID, status, nodeKey)
	return status
}

// readLeaseTask reads runID's task at leaseTable through state's ListTasks.
func readLeaseTask(ctx context.Context, clients *testClients, runID uuid.UUID) (*runv1.Task, error) {
	resp, err := clients.runClient.ListTasks(ctx, &runv1.ListTasksRequest{RunId: runID.String()})
	if err != nil {
		return nil, fmt.Errorf("ListTasks(%s): %w", runID, err)
	}
	for _, task := range resp.GetTasks() {
		if task.GetServiceName() == leaseService && task.GetSchemaName() == testSchemaName && task.GetTableName() == leaseTable {
			return task, nil
		}
	}
	return nil, fmt.Errorf("run %s holds no task at %s", runID, leaseTable)
}

// runGraphNodeOf returns the node of graph whose table is table.
func runGraphNodeOf(t *testing.T, graph *runv1.GetRunGraphResponse, table string) *runv1.RunGraphNode {
	t.Helper()
	for _, node := range graph.GetNodes() {
		if node.GetTableName() == table {
			return node
		}
	}
	require.FailNow(t, "the run graph holds no node at "+table)
	return nil
}

// podInterval is when the pods of one task ran: from the earliest time the
// kubelet accepted one of them to the latest time one of their containers
// terminated.
type podInterval struct {
	start, finish time.Time
	pods          []string
}

// taskPodInterval waits until every pod of taskID has stopped for good (phase
// Succeeded or Failed) and each Succeeded pod reports a terminated container
// with its finish time, and returns the interval they ran in. A container of a
// Failed pod that never ran has no termination time and does not extend the
// interval.
func taskPodInterval(t *testing.T, ctx context.Context, taskID uuid.UUID, label string) podInterval {
	t.Helper()
	selector := "task-id=" + taskID.String()
	var interval podInterval
	lastSeen := "no pod listed"
	pollUntilDescribed(t, ctx, 2*time.Minute, time.Second, func() (bool, error) {
		list, err := getK8sPods(ctx, selector)
		if err != nil {
			return false, err
		}
		if len(list.Items) == 0 {
			lastSeen = "no pod listed"
			return false, nil
		}
		var cur podInterval
		for _, pod := range list.Items {
			if pod.Status.Phase != "Succeeded" && pod.Status.Phase != "Failed" {
				lastSeen = fmt.Sprintf("pod %s is %s", pod.Metadata.Name, pod.Status.Phase)
				return false, nil
			}
			if pod.Status.StartTime == nil {
				lastSeen = fmt.Sprintf("pod %s has no start time", pod.Metadata.Name)
				return false, nil
			}
			if cur.start.IsZero() || pod.Status.StartTime.Before(cur.start) {
				cur.start = *pod.Status.StartTime
			}
			var podFinish time.Time
			for _, statuses := range [][]k8sContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
				for _, c := range statuses {
					if c.State.Terminated != nil && c.State.Terminated.FinishedAt.After(podFinish) {
						podFinish = c.State.Terminated.FinishedAt
					}
				}
			}
			if pod.Status.Phase == "Succeeded" && podFinish.IsZero() {
				lastSeen = fmt.Sprintf("pod %s succeeded but reports no terminated container with a finish time", pod.Metadata.Name)
				return false, nil
			}
			if podFinish.After(cur.finish) {
				cur.finish = podFinish
			}
			cur.pods = append(cur.pods, pod.Metadata.Name)
		}
		interval = cur
		return true, nil
	}, func() string {
		return fmt.Sprintf("Timeout waiting for every pod of %s (task %s) to stop: %s", label, taskID, lastSeen)
	})
	return interval
}
