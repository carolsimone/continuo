package chat

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-chat/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBudgetSession is newTestSession with an explicit per-turn token budget.
func newBudgetSession(t *testing.T, provider *scriptedProvider, exec *fakeExecutor, defs map[string]ports.ToolDef, budget int) (*Session, *recordingSink) {
	t.Helper()
	sink := newSink()
	s, err := NewSession(context.Background(), Deps{
		Provider: provider, Catalog: &fakeCatalog{defs: defs}, Executor: exec, Repo: newFakeRepo(),
		Limiter: allowAllLimiter{},
		Cfg: Config{
			SystemPrompt: "sys", MaxIterations: 5, MaxTurnTokens: budget,
			WindowTokens: 100000, ConfirmTTL: 50 * time.Millisecond,
			CLIName: "continuo",
		},
	}, "alice", "", sink)
	require.NoError(t, err)
	go s.Run(context.Background())
	t.Cleanup(s.Close)
	return s, sink
}

// A tool call the model has already produced is executed even when that
// iteration pushes the turn over budget; the budget then refuses to start the
// next provider call. Dropping the call while the persisted text claims it
// happened is the failure this guards against.
func TestSession_BudgetOverrunStillExecutesProducedToolCalls(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{
			Text:      "Triggering.",
			ToolCalls: []ports.ToolCall{{ID: "c1", Name: "schedule_status", Args: map[string]string{"schedule-name": "daily"}}},
			Usage:     ports.Usage{InputTokens: 80, OutputTokens: 40},
		},
		{Text: "never reached"},
	}}
	exec := &fakeExecutor{}
	s, sink := newBudgetSession(t, provider, exec, map[string]ports.ToolDef{"schedule_status": statusDef}, 100)

	s.Enqueue("how is daily?")
	waitFor(t, sink, "error:token_budget")

	require.Len(t, exec.calls, 1, "the produced tool call must run before the budget stops the turn")
	assert.Contains(t, sink.snapshot(), "tool:continuo schedule status daily")
	assert.Len(t, provider.calls, 1, "no further provider call once over budget")
	assert.NotContains(t, sink.snapshot(), "final:never reached")
}

// Tokens the provider served from its prompt cache are not spend: a turn that
// re-reads a large cached thread on every iteration stays within budget.
func TestSession_CacheReadsDoNotCountTowardBudget(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{
			ToolCalls: []ports.ToolCall{{ID: "c1", Name: "schedule_status", Args: map[string]string{"schedule-name": "daily"}}},
			Usage:     ports.Usage{InputTokens: 30, OutputTokens: 10, CacheReadInputTokens: 9000},
		},
		{Text: "All good.", Usage: ports.Usage{InputTokens: 30, OutputTokens: 10, CacheReadInputTokens: 9500}},
	}}
	exec := &fakeExecutor{}
	s, sink := newBudgetSession(t, provider, exec, map[string]ports.ToolDef{"schedule_status": statusDef}, 100)

	s.Enqueue("how is daily?")
	waitFor(t, sink, "final:All good.")

	assert.NotContains(t, sink.snapshot(), "error:token_budget")
	assert.Len(t, provider.calls, 2)
}

// The first call of a turn on a cold cache writes the prompt the turn starts
// from (tool catalog, system prompt and windowed thread). That write is the
// price of starting, not growth the turn caused, so a prompt larger than the
// budget still lets the turn use its tool result and answer.
func TestSession_FirstCallCacheWriteDoesNotExhaustBudget(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{
			ToolCalls: []ports.ToolCall{{ID: "c1", Name: "schedule_status", Args: map[string]string{"schedule-name": "daily"}}},
			Usage:     ports.Usage{InputTokens: 4, OutputTokens: 28, CacheCreationInputTokens: 31351},
		},
		{Text: "All good.", Usage: ports.Usage{InputTokens: 4, OutputTokens: 20, CacheReadInputTokens: 31351, CacheCreationInputTokens: 300}},
	}}
	exec := &fakeExecutor{}
	s, sink := newBudgetSession(t, provider, exec, map[string]ports.ToolDef{"schedule_status": statusDef}, 30000)

	s.Enqueue("how is daily?")
	waitFor(t, sink, "final:All good.")

	assert.NotContains(t, sink.snapshot(), "error:token_budget")
	assert.Len(t, provider.calls, 2)
}

// The first call's uncached input and output still count: only its cache write
// is the price of starting.
func TestSession_FirstCallInputAndOutputCountTowardBudget(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{
			ToolCalls: []ports.ToolCall{{ID: "c1", Name: "schedule_status", Args: map[string]string{"schedule-name": "daily"}}},
			Usage:     ports.Usage{InputTokens: 60, OutputTokens: 50, CacheCreationInputTokens: 31351},
		},
		{Text: "never reached"},
	}}
	exec := &fakeExecutor{}
	s, sink := newBudgetSession(t, provider, exec, map[string]ports.ToolDef{"schedule_status": statusDef}, 100)

	s.Enqueue("how is daily?")
	waitFor(t, sink, "error:token_budget")

	assert.Len(t, provider.calls, 1)
}

// After the first call, a cache write is what the turn added to the thread,
// such as a large tool result, and counts as spend: a turn cannot grow without
// bound by writing to the cache.
func TestSession_LaterCacheWritesCountTowardBudget(t *testing.T) {
	statusCall := []ports.ToolCall{{ID: "c1", Name: "schedule_status", Args: map[string]string{"schedule-name": "daily"}}}
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{ToolCalls: statusCall, Usage: ports.Usage{InputTokens: 5, OutputTokens: 5, CacheCreationInputTokens: 500}},
		{ToolCalls: statusCall, Usage: ports.Usage{InputTokens: 5, OutputTokens: 5, CacheCreationInputTokens: 90}},
		{Text: "never reached"},
	}}
	exec := &fakeExecutor{}
	s, sink := newBudgetSession(t, provider, exec, map[string]ports.ToolDef{"schedule_status": statusDef}, 100)

	s.Enqueue("how is daily?")
	waitFor(t, sink, "error:token_budget")

	assert.Len(t, provider.calls, 2)
	require.Len(t, exec.calls, 2)
}

// A final text answer is delivered as final even when the iteration that
// produced it crossed the budget: the budget bounds further iterations, not
// the answer already streamed to the user.
func TestSession_FinalAnswerOverBudgetIsNotAnError(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{Text: "Done.", Usage: ports.Usage{InputTokens: 500, OutputTokens: 20}},
	}}
	exec := &fakeExecutor{}
	s, sink := newBudgetSession(t, provider, exec, map[string]ports.ToolDef{"schedule_status": statusDef}, 100)

	s.Enqueue("how is daily?")
	waitFor(t, sink, "final:Done.")

	assert.NotContains(t, sink.snapshot(), "error:token_budget")
}
