package run_test

import (
	"context"
	"slices"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// memTaskGraph answers run.TaskGraph from a run's graph and the task
// statuses a fakeTaskCollection holds, the way the Postgres queries do.
type memTaskGraph struct {
	g     topology.Graph
	tc    *fakeTaskCollection
	reads int
}

func (m *memTaskGraph) task(k topology.NodeKey) (run.Task, bool) {
	for _, t := range m.tc.bulkCreated {
		if t.NodeKey == k {
			return t, true
		}
	}
	return run.Task{}, false
}

func (m *memTaskGraph) waiting(k topology.NodeKey) (run.Task, bool) {
	t, ok := m.task(k)
	if !ok || m.tc.statuses[t.TaskID] != run.TaskStatusPending || slices.Contains(m.tc.dispatched, t.TaskID) {
		return run.Task{}, false
	}
	return t, true
}

func (m *memTaskGraph) Node(_ context.Context, _ string, key topology.NodeKey) (topology.Node, bool, error) {
	n, ok := m.g.Node(key)
	return n, ok, nil
}

func (m *memTaskGraph) ReadyCandidates(_ context.Context, _ uuid.UUID, _ string, key topology.NodeKey) ([]run.ReadyCandidate, error) {
	m.reads++
	var out []run.ReadyCandidate
	for _, d := range m.g.Downstreams(key) {
		t, ok := m.waiting(d)
		if !ok {
			continue
		}
		var upstream []run.TaskStatus
		for _, u := range m.g.Upstreams(d) {
			if ut, ok := m.task(u); ok {
				upstream = append(upstream, m.tc.statuses[ut.TaskID])
			}
		}
		n, _ := m.g.Node(d)
		out = append(out, run.ReadyCandidate{
			TaskID: t.TaskID, Node: n, JobName: t.JobName,
			UpstreamStatuses: upstream,
		})
	}
	return out, nil
}

func (m *memTaskGraph) PendingDescendants(_ context.Context, _ uuid.UUID, _ string, key topology.NodeKey) ([]uuid.UUID, error) {
	m.reads++
	seen := map[topology.NodeKey]bool{}
	stack := []topology.NodeKey{key}
	for len(stack) > 0 {
		k := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, d := range m.g.Downstreams(k) {
			if _, ok := m.waiting(d); ok && !seen[d] {
				seen[d] = true
				stack = append(stack, d)
			}
		}
	}
	keys := make([]topology.NodeKey, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := make([]uuid.UUID, 0, len(keys))
	for _, k := range keys {
		t, _ := m.task(k)
		out = append(out, t.TaskID)
	}
	return out, nil
}

// startedFailureRun starts the e2e-schedule-failure run: ftable_a and
// ftable_b are dispatched, every other task waits.
func startedFailureRun(t *testing.T) (*run.Run, *fakeTaskCollection, *memTaskGraph) {
	t.Helper()
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newCronRun(t, model.OperationRun)
	tc := newFakeTaskCollection()
	tc.derived = true
	_, err = r.Start(context.Background(), tc, "default", plan, g, taskDefaults, startNow)
	require.NoError(t, err)
	return r, tc, &memTaskGraph{g: g, tc: tc}
}

func taskOf(r *run.Run, table string) uuid.UUID {
	return run.TaskIDFor("default", r.ScheduleID(), fkey(table))
}

// attemptOf returns the stored attempt n of the task of r at table.
func attemptOf(t *testing.T, tc *fakeTaskCollection, r *run.Run, table string, n int32) (run.Attempt, bool) {
	t.Helper()
	a, ok := tc.attemptRows[attemptKey{taskOf(r, table), n}]
	return a, ok
}
