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
