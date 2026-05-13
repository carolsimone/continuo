package run_test

import (
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helpers

func key(schema, table string) run.NodeKey {
	return run.NodeKey{ServiceName: "svc", SchemaName: schema, TableName: table}
}

func node(k run.NodeKey, status string, ups, downs []run.NodeKey) *run.RunNode {
	return &run.RunNode{
		Key:         k,
		TaskID:      uuid.New(),
		Status:      status,
		Upstreams:   ups,
		Downstreams: downs,
	}
}

func makeRun(nodes ...*run.RunNode) *run.Run {
	return run.NewRun("run-1", "daily", nodes)
}

// finalization tests

func TestCompleteNode_LastNodeSucceeded_FinalizesAsSucceeded(t *testing.T) {
	k := key("public", "orders")
	r := makeRun(node(k, "RUNNING", nil, nil))

	events, err := r.CompleteNode(k, "SUCCEEDED")

	require.NoError(t, err)
	assert.Equal(t, run.RunStatusSucceeded, r.Status)
	require.Len(t, events, 1)
	_, ok := events[0].(run.RunFinalized)
	assert.True(t, ok)
	evt := events[0].(run.RunFinalized)
	assert.Equal(t, "SUCCEEDED", evt.TerminalStatus)
}

func TestCompleteNode_LastNodeFailed_FinalizesAsFailed(t *testing.T) {
	k := key("public", "orders")
	r := makeRun(node(k, "RUNNING", nil, nil))

	events, err := r.CompleteNode(k, "FAILED")

	require.NoError(t, err)
	assert.Equal(t, run.RunStatusFailed, r.Status)
	evt := events[len(events)-1].(run.RunFinalized)
	assert.Equal(t, "FAILED", evt.TerminalStatus)
}

func TestCompleteNode_NonLastNode_DoesNotFinalize(t *testing.T) {
	k1 := key("public", "orders")
	k2 := key("public", "customers")
	r := makeRun(
		node(k1, "RUNNING", nil, nil),
		node(k2, "PENDING", nil, nil),
	)

	events, err := r.CompleteNode(k1, "SUCCEEDED")

	require.NoError(t, err)
	assert.Equal(t, run.RunStatusInProgress, r.Status)
	for _, e := range events {
		_, isFinalized := e.(run.RunFinalized)
		assert.False(t, isFinalized)
	}
}

func TestCompleteNode_AnyFailedAmongTerminal_FinalizesAsFailed(t *testing.T) {
	kSucc := key("public", "orders")
	kFail := key("public", "customers")
	r := makeRun(
		node(kSucc, "SUCCEEDED", nil, nil),
		node(kFail, "RUNNING", nil, nil),
	)
	r.TerminalCount = 1 // orders already counted

	events, err := r.CompleteNode(kFail, "FAILED")

	require.NoError(t, err)
	evt := events[len(events)-1].(run.RunFinalized)
	assert.Equal(t, "FAILED", evt.TerminalStatus)
}

// state machine tests

func TestCompleteNode_AlreadyTerminal_ReturnsError(t *testing.T) {
	k := key("public", "orders")
	r := makeRun(node(k, "SUCCEEDED", nil, nil))
	r.TerminalCount = 1

	_, err := r.CompleteNode(k, "FAILED")

	assert.ErrorIs(t, err, run.ErrNodeAlreadyTerminal)
}

func TestCompleteNode_NodeNotInSubgraph_ReturnsError(t *testing.T) {
	k := key("public", "orders")
	r := makeRun(node(k, "RUNNING", nil, nil))
	missing := key("public", "missing")

	_, err := r.CompleteNode(missing, "SUCCEEDED")

	assert.ErrorIs(t, err, run.ErrNodeNotInScope)
}

func TestCompleteNode_TerminalCountIncrements(t *testing.T) {
	k := key("public", "orders")
	r := makeRun(
		node(k, "RUNNING", nil, nil),
		node(key("public", "other"), "PENDING", nil, nil),
	)

	_, err := r.CompleteNode(k, "SUCCEEDED")
	require.NoError(t, err)
	assert.Equal(t, 1, r.TerminalCount)
}

func TestCompleteNode_VersionBumpsOnEachCall(t *testing.T) {
	k := key("public", "a")
	r := makeRun(
		node(k, "RUNNING", nil, nil),
		node(key("public", "b"), "PENDING", nil, nil),
	)

	_, _ = r.CompleteNode(k, "SUCCEEDED")
	assert.Equal(t, 1, r.Version)
}

// cascade skip tests

func TestCompleteNode_Failed_CascadesSkipToDownstream(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b")
	kC := key("public", "c")
	r := makeRun(
		node(kA, "RUNNING", nil, []run.NodeKey{kB}),
		node(kB, "PENDING", []run.NodeKey{kA}, []run.NodeKey{kC}),
		node(kC, "PENDING", []run.NodeKey{kB}, nil),
	)

	events, err := r.CompleteNode(kA, "FAILED")

	require.NoError(t, err)
	skipped := filterEvents[run.NodeCascadeSkipped](events)
	require.Len(t, skipped, 2)
	skippedKeys := make(map[run.NodeKey]bool)
	for _, e := range skipped {
		skippedKeys[e.Key] = true
	}
	assert.True(t, skippedKeys[kB])
	assert.True(t, skippedKeys[kC])
}

func TestCompleteNode_Failed_CascadeStopsAtTerminalNode(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b") // already SUCCEEDED — cascade must not re-skip
	r := makeRun(
		node(kA, "RUNNING", nil, []run.NodeKey{kB}),
		node(kB, "SUCCEEDED", []run.NodeKey{kA}, nil),
	)
	r.TerminalCount = 1

	events, err := r.CompleteNode(kA, "FAILED")

	require.NoError(t, err)
	skipped := filterEvents[run.NodeCascadeSkipped](events)
	assert.Empty(t, skipped, "should not cascade-skip an already-terminal node")
}

func TestCompleteNode_Failed_TerminalCountIncludesCascaded(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b")
	r := makeRun(
		node(kA, "RUNNING", nil, []run.NodeKey{kB}),
		node(kB, "PENDING", []run.NodeKey{kA}, nil),
	)

	_, err := r.CompleteNode(kA, "FAILED")
	require.NoError(t, err)
	// kA (FAILED) + kB (cascade SKIPPED) = 2
	assert.Equal(t, 2, r.TerminalCount)
}

// unblocking tests

func TestCompleteNode_Succeeded_UnblocksDownstreamWhenAllUpstreamsTerminal(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b")
	kC := key("public", "c") // depends on both A and B
	r := makeRun(
		node(kA, "RUNNING", nil, []run.NodeKey{kC}),
		node(kB, "SUCCEEDED", nil, []run.NodeKey{kC}),
		node(kC, "PENDING", []run.NodeKey{kA, kB}, nil),
	)
	r.TerminalCount = 1 // B already terminal

	events, err := r.CompleteNode(kA, "SUCCEEDED")

	require.NoError(t, err)
	unblocked := filterEvents[run.NodeUnblocked](events)
	require.Len(t, unblocked, 1)
	assert.Equal(t, kC, unblocked[0].Key)
}

func TestCompleteNode_Succeeded_DoesNotUnblockWhenUpstreamStillPending(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b") // still PENDING
	kC := key("public", "c")
	r := makeRun(
		node(kA, "RUNNING", nil, []run.NodeKey{kC}),
		node(kB, "PENDING", nil, []run.NodeKey{kC}),
		node(kC, "PENDING", []run.NodeKey{kA, kB}, nil),
	)

	events, err := r.CompleteNode(kA, "SUCCEEDED")

	require.NoError(t, err)
	unblocked := filterEvents[run.NodeUnblocked](events)
	assert.Empty(t, unblocked)
}

// helper

func filterEvents[T run.DomainEvent](events []run.DomainEvent) []T {
	var out []T
	for _, e := range events {
		if v, ok := e.(T); ok {
			out = append(out, v)
		}
	}
	return out
}

// ResetDownstream tests

func TestResetDownstream_ResetsSkippedNodesToPending(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b")
	kC := key("public", "c")
	r := makeRun(
		node(kA, "FAILED", nil, []run.NodeKey{kB}),
		node(kB, "SKIPPED", []run.NodeKey{kA}, []run.NodeKey{kC}),
		node(kC, "SKIPPED", []run.NodeKey{kB}, nil),
	)
	r.TerminalCount = 3

	_, err := r.ResetDownstream(kA)
	require.NoError(t, err)

	byKey := map[run.NodeKey]string{}
	for _, n := range r.Nodes() {
		byKey[n.Key] = n.Status
	}
	assert.Equal(t, "PENDING", byKey[kB])
	assert.Equal(t, "PENDING", byKey[kC])
	assert.Equal(t, 1, r.TerminalCount) // only kA remains terminal
}

func TestResetDownstream_DoesNotResetNonSkippedNodes(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b")
	r := makeRun(
		node(kA, "FAILED", nil, []run.NodeKey{kB}),
		node(kB, "SUCCEEDED", []run.NodeKey{kA}, nil),
	)
	r.TerminalCount = 2

	_, err := r.ResetDownstream(kA)
	require.NoError(t, err)

	for _, n := range r.Nodes() {
		if n.Key == kB {
			assert.Equal(t, "SUCCEEDED", n.Status, "SUCCEEDED node must not be reset")
		}
	}
	assert.Equal(t, 2, r.TerminalCount)
}

func TestResetDownstream_NodeNotInScope_ReturnsError(t *testing.T) {
	r := makeRun(node(key("public", "a"), "FAILED", nil, nil))

	_, err := r.ResetDownstream(key("public", "missing"))

	assert.ErrorIs(t, err, run.ErrNodeNotInScope)
}

// FailedCount tests

func TestCompleteNode_FailedDirect_IncrementsFailedCount(t *testing.T) {
	kA := key("public", "a")
	r := makeRun(
		node(kA, "RUNNING", nil, nil),
		node(key("public", "b"), "PENDING", nil, nil),
	)

	_, err := r.CompleteNode(kA, "FAILED")
	require.NoError(t, err)
	assert.Equal(t, 1, r.FailedCount)
}

func TestCompleteNode_CascadeSkipped_DoesNotIncrementFailedCount(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b")
	r := makeRun(
		node(kA, "RUNNING", nil, []run.NodeKey{kB}),
		node(kB, "PENDING", []run.NodeKey{kA}, nil),
	)

	_, err := r.CompleteNode(kA, "FAILED")
	require.NoError(t, err)
	assert.Equal(t, 1, r.FailedCount, "cascade-skipped nodes must not count as failures")
}

func TestCompleteNode_PartialSubgraph_FinalisesAsFailedFromFailedCount(t *testing.T) {
	// 3-node run; only the last node is in the current scope. A failed earlier
	// in a different subgraph and is not loaded, but FailedCount carries the
	// signal across operations.
	kCurrent := key("public", "c")
	r := makeRun(node(kCurrent, "RUNNING", nil, nil))
	r.TotalNodes = 3
	r.TerminalCount = 2 // A and B were counted in prior subgraph operations
	r.FailedCount = 1   // A failed in a prior subgraph; not in scope here

	events, err := r.CompleteNode(kCurrent, "SUCCEEDED")
	require.NoError(t, err)

	assert.Equal(t, run.RunStatusFailed, r.Status,
		"FailedCount must drive terminal status even when no failed node is in scope")
	require.NotEmpty(t, events)
	evt, ok := events[len(events)-1].(run.RunFinalized)
	require.True(t, ok, "last event must be RunFinalized, got %T", events[len(events)-1])
	assert.Equal(t, "FAILED", evt.TerminalStatus)
}

// EffectsForTerminal tests

func TestEffectsForTerminal_Succeeded_ReturnsUnblockedEvents(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b")
	kC := key("public", "c")
	r := makeRun(
		node(kA, "SUCCEEDED", nil, []run.NodeKey{kC}),
		node(kB, "SUCCEEDED", nil, []run.NodeKey{kC}),
		node(kC, "PENDING", []run.NodeKey{kA, kB}, nil),
	)
	r.TerminalCount = 2

	events, err := r.EffectsForTerminal(kA)
	require.NoError(t, err)

	unblocked := filterEvents[run.NodeUnblocked](events)
	require.Len(t, unblocked, 1)
	assert.Equal(t, kC, unblocked[0].Key)
}

func TestEffectsForTerminal_Failed_ReturnsCascadeSkippedEvents(t *testing.T) {
	kA := key("public", "a")
	kB := key("public", "b")
	kC := key("public", "c")
	r := makeRun(
		node(kA, "FAILED", nil, []run.NodeKey{kB}),
		node(kB, "SKIPPED", []run.NodeKey{kA}, []run.NodeKey{kC}),
		node(kC, "SKIPPED", []run.NodeKey{kB}, nil),
	)
	r.TerminalCount = 3

	events, err := r.EffectsForTerminal(kA)
	require.NoError(t, err)

	skipped := filterEvents[run.NodeCascadeSkipped](events)
	require.Len(t, skipped, 2)
	keys := map[run.NodeKey]bool{}
	for _, e := range skipped {
		keys[e.Key] = true
	}
	assert.True(t, keys[kB])
	assert.True(t, keys[kC])
}

func TestEffectsForTerminal_NotTerminal_ReturnsError(t *testing.T) {
	kA := key("public", "a")
	r := makeRun(node(kA, "RUNNING", nil, nil))

	_, err := r.EffectsForTerminal(kA)
	require.Error(t, err)
}

func TestEffectsForTerminal_NodeNotInScope_ReturnsError(t *testing.T) {
	r := makeRun(node(key("public", "a"), "SUCCEEDED", nil, nil))
	r.TerminalCount = 1

	_, err := r.EffectsForTerminal(key("public", "missing"))
	assert.ErrorIs(t, err, run.ErrNodeNotInScope)
}
