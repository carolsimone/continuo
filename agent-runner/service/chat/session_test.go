package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allowAllLimiter is a ports.RateLimiter that never throttles, used where a
// test exercises session behaviour unrelated to rate limiting.
type allowAllLimiter struct{}

func (allowAllLimiter) Allow(context.Context, string) (bool, error) { return true, nil }

type fakeRepo struct {
	mu      sync.Mutex
	threads map[uuid.UUID]*domain.Thread
	msgs    map[uuid.UUID][]domain.Message
	actions map[uuid.UUID]*domain.PendingAction
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{threads: map[uuid.UUID]*domain.Thread{}, msgs: map[uuid.UUID][]domain.Message{}, actions: map[uuid.UUID]*domain.PendingAction{}}
}
func (f *fakeRepo) CreateThread(_ context.Context, userID string) (*domain.Thread, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &domain.Thread{ID: uuid.New(), UserID: userID}
	f.threads[t.ID] = t
	return t, nil
}
func (f *fakeRepo) GetThread(_ context.Context, id uuid.UUID, userID string) (*domain.Thread, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.threads[id]
	if !ok || t.UserID != userID {
		return nil, fmt.Errorf("not found")
	}
	return t, nil
}
func (f *fakeRepo) AppendMessage(_ context.Context, threadID uuid.UUID, role domain.Role, content json.RawMessage) (*domain.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := domain.Message{ID: uuid.New(), ThreadID: threadID, Seq: len(f.msgs[threadID]) + 1, Role: role, Content: content}
	f.msgs[threadID] = append(f.msgs[threadID], m)
	return &m, nil
}
func (f *fakeRepo) ListMessages(_ context.Context, threadID uuid.UUID) ([]domain.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.Message{}, f.msgs[threadID]...), nil
}
func (f *fakeRepo) CreatePendingAction(_ context.Context, a *domain.PendingAction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actions[a.ID] = a
	return nil
}
func (f *fakeRepo) ResolvePendingAction(_ context.Context, id uuid.UUID, s domain.ActionStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.actions[id]
	if !ok {
		return fmt.Errorf("not found")
	}
	return a.Resolve(s)
}
func (f *fakeRepo) ListIdleThreads(context.Context, time.Time, int) ([]domain.Thread, error) {
	return nil, nil
}
func (f *fakeRepo) DeleteThread(context.Context, uuid.UUID) error { return nil }

type scriptedProvider struct {
	mu      sync.Mutex
	results []*ports.TurnResult
	calls   []ports.TurnRequest
}

func (p *scriptedProvider) StreamTurn(_ context.Context, req ports.TurnRequest, onDelta func(string)) (*ports.TurnResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, req)
	if len(p.results) == 0 {
		return nil, fmt.Errorf("provider script exhausted")
	}
	r := p.results[0]
	p.results = p.results[1:]
	if r.Text != "" {
		onDelta(r.Text)
	}
	return r, nil
}

type fakeExecutor struct {
	mu    sync.Mutex
	calls []ports.ToolCall
}

func (e *fakeExecutor) Execute(_ context.Context, _ string, _ uuid.UUID, c ports.ToolCall) ports.ToolResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, c)
	return ports.ToolResult{Output: `{"ok":true}`}
}

type fakeCatalog struct{ defs map[string]ports.ToolDef }

func (c *fakeCatalog) Tools() []ports.ToolDef {
	var out []ports.ToolDef
	for _, d := range c.defs {
		out = append(out, d)
	}
	return out
}
func (c *fakeCatalog) Lookup(n string) (ports.ToolDef, bool) { d, ok := c.defs[n]; return d, ok }

type recordingSink struct {
	mu         sync.Mutex
	events     []string
	confirmIDs chan uuid.UUID
}

func newSink() *recordingSink { return &recordingSink{confirmIDs: make(chan uuid.UUID, 4)} }
func (s *recordingSink) add(e string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}
func (s *recordingSink) Thread(uuid.UUID)         { s.add("thread") }
func (s *recordingSink) History([]domain.Message) { s.add("history") }
func (s *recordingSink) Tool(cmd string)          { s.add("tool:" + cmd) }
func (s *recordingSink) Text(t string)            { s.add("text:" + t) }
func (s *recordingSink) Final(t string)           { s.add("final:" + t) }
func (s *recordingSink) Error(code, _ string)     { s.add("error:" + code) }
func (s *recordingSink) ConfirmRequest(id uuid.UUID, sum string) {
	s.add("confirm:" + sum)
	s.confirmIDs <- id
}
func (s *recordingSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.events...)
}

func waitFor(t *testing.T, sink *recordingSink, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range sink.snapshot() {
			if e == want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("event %q never emitted; got %v", want, sink.snapshot())
}

func newTestSession(t *testing.T, provider *scriptedProvider, exec *fakeExecutor, defs map[string]ports.ToolDef) (*Session, *recordingSink, *fakeRepo) {
	repo := newFakeRepo()
	sink := newSink()
	s, err := NewSession(context.Background(), Deps{
		Provider: provider, Catalog: &fakeCatalog{defs: defs}, Executor: exec, Repo: repo,
		Limiter: allowAllLimiter{},
		Cfg: Config{
			SystemPrompt: "sys", MaxIterations: 5, MaxTurnTokens: 100000,
			WindowTokens: 100000, ConfirmTTL: 50 * time.Millisecond,
			CLIName: "continuo",
		},
	}, "alice", "", sink)
	require.NoError(t, err)
	go s.Run(context.Background())
	t.Cleanup(s.Close)
	return s, sink, repo
}

var statusDef = ports.ToolDef{
	Name: "schedule_status", CLIPath: []string{"schedule", "status"},
	Params: []ports.ToolParam{{Name: "schedule-name", Required: true}}, ParamOrder: []string{"schedule-name"},
}
var triggerDef = ports.ToolDef{
	Name: "schedule_trigger", Mutating: true, CLIPath: []string{"schedule", "trigger"},
	Params: []ports.ToolParam{{Name: "schedule-name", Required: true}}, ParamOrder: []string{"schedule-name"},
}

func TestSession_ReadToolLoopEndsInFinal(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{ToolCalls: []ports.ToolCall{{ID: "c1", Name: "schedule_status", Args: map[string]string{"schedule-name": "daily"}}}},
		{Text: "All good."},
	}}
	exec := &fakeExecutor{}
	s, sink, repo := newTestSession(t, provider, exec, map[string]ports.ToolDef{"schedule_status": statusDef})

	s.Enqueue("how is daily?")
	waitFor(t, sink, "final:All good.")

	assert.Contains(t, sink.snapshot(), "tool:continuo schedule status daily")
	require.Len(t, exec.calls, 1)
	msgs, _ := repo.ListMessages(context.Background(), s.ThreadID())
	roles := []domain.Role{}
	for _, m := range msgs {
		roles = append(roles, m.Role)
	}
	assert.Equal(t, []domain.Role{domain.RoleUser, domain.RoleToolCall, domain.RoleToolResult, domain.RoleAssistant}, roles)
}

func TestSession_MutatingToolWaitsForApproval(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{ToolCalls: []ports.ToolCall{{ID: "c1", Name: "schedule_trigger", Args: map[string]string{"schedule-name": "daily"}}}},
		{Text: "Triggered."},
	}}
	exec := &fakeExecutor{}
	s, sink, _ := newTestSession(t, provider, exec, map[string]ports.ToolDef{"schedule_trigger": triggerDef})

	s.Enqueue("trigger daily")
	id := <-sink.confirmIDs
	assert.Empty(t, exec.calls)
	s.Confirm(id, true)
	waitFor(t, sink, "final:Triggered.")
	require.Len(t, exec.calls, 1)
}

func TestSession_DenialFeedsRefusalToModel(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{ToolCalls: []ports.ToolCall{{ID: "c1", Name: "schedule_trigger", Args: map[string]string{"schedule-name": "daily"}}}},
		{Text: "Understood, not triggering."},
	}}
	exec := &fakeExecutor{}
	s, sink, _ := newTestSession(t, provider, exec, map[string]ports.ToolDef{"schedule_trigger": triggerDef})

	s.Enqueue("trigger daily")
	id := <-sink.confirmIDs
	s.Confirm(id, false)
	waitFor(t, sink, "final:Understood, not triggering.")
	assert.Empty(t, exec.calls)
}

func TestSession_ConfirmExpiryCountsAsDenial(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{
		{ToolCalls: []ports.ToolCall{{ID: "c1", Name: "schedule_trigger", Args: map[string]string{"schedule-name": "daily"}}}},
		{Text: "Request expired."},
	}}
	exec := &fakeExecutor{}
	s, sink, _ := newTestSession(t, provider, exec, map[string]ports.ToolDef{"schedule_trigger": triggerDef})

	s.Enqueue("trigger daily")
	<-sink.confirmIDs
	waitFor(t, sink, "final:Request expired.")
	assert.Empty(t, exec.calls)
}

func TestSession_IterationCapEmitsError(t *testing.T) {
	loop := &ports.TurnResult{ToolCalls: []ports.ToolCall{{ID: "c", Name: "schedule_status", Args: map[string]string{"schedule-name": "d"}}}}
	provider := &scriptedProvider{results: []*ports.TurnResult{loop, loop, loop, loop, loop, loop}}
	s, sink, _ := newTestSession(t, provider, &fakeExecutor{}, map[string]ports.ToolDef{"schedule_status": statusDef})

	s.Enqueue("loop forever")
	waitFor(t, sink, "error:iteration_limit")
	_ = s
}

func TestSession_QueuedMessagesAnswerInOrder(t *testing.T) {
	provider := &scriptedProvider{results: []*ports.TurnResult{{Text: "first"}, {Text: "second"}}}
	s, sink, _ := newTestSession(t, provider, &fakeExecutor{}, nil)

	s.Enqueue("q1")
	s.Enqueue("q2")
	waitFor(t, sink, "final:second")
	events := sink.snapshot()
	i1, i2 := -1, -1
	for i, e := range events {
		if e == "final:first" {
			i1 = i
		}
		if e == "final:second" {
			i2 = i
		}
	}
	assert.True(t, i1 >= 0 && i2 > i1, "answers must arrive in order: %v", events)
}

// partialThenErrorProvider is a scripted provider whose first call streams a
// delta and then returns an error, simulating a partial-stream failure.
type partialThenErrorProvider struct {
	mu      sync.Mutex
	calls   int
	results []*ports.TurnResult // fallback results for calls after the first
}

func (p *partialThenErrorProvider) StreamTurn(_ context.Context, req ports.TurnRequest, onDelta func(string)) (*ports.TurnResult, error) {
	p.mu.Lock()
	call := p.calls
	p.calls++
	p.mu.Unlock()

	if call == 0 {
		// First call: emit a delta then fail.
		onDelta("partial")
		return nil, fmt.Errorf("transient network error")
	}
	// Subsequent calls: return the next scripted result.
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.results) == 0 {
		return nil, fmt.Errorf("provider script exhausted")
	}
	r := p.results[0]
	p.results = p.results[1:]
	if r.Text != "" {
		onDelta(r.Text)
	}
	return r, nil
}

func TestSession_NoRetryAfterPartialStream(t *testing.T) {
	// The provider streams "partial" then fails on the first call. Because a
	// delta was already emitted, the session must NOT retry (which would
	// duplicate "partial") and must emit an error event instead.
	provider := &partialThenErrorProvider{
		results: []*ports.TurnResult{{Text: "should not appear"}},
	}
	repo := newFakeRepo()
	sink := newSink()
	s, err := NewSession(context.Background(), Deps{
		Provider: provider, Catalog: &fakeCatalog{defs: nil}, Executor: &fakeExecutor{}, Repo: repo,
		Limiter: allowAllLimiter{},
		Cfg: Config{
			SystemPrompt: "sys", MaxIterations: 5, MaxTurnTokens: 100000,
			WindowTokens: 100000, ConfirmTTL: 50 * time.Millisecond,
			CLIName: "continuo",
		},
	}, "alice", "", sink)
	require.NoError(t, err)
	go s.Run(context.Background())
	t.Cleanup(s.Close)

	s.Enqueue("hello")
	// Must get an error event (no retry took place).
	waitFor(t, sink, "error:provider_unavailable")

	// "partial" must appear exactly once — the retry must not have doubled it.
	count := 0
	for _, e := range sink.snapshot() {
		if e == "text:partial" {
			count++
		}
	}
	assert.Equal(t, 1, count, "streamed delta must appear exactly once, got events: %v", sink.snapshot())

	// "should not appear" must never have been streamed.
	for _, e := range sink.snapshot() {
		assert.NotEqual(t, "text:should not appear", e, "retry must not have fired: %v", sink.snapshot())
	}

	// Provider was only called once (no retry).
	provider.mu.Lock()
	calls := provider.calls
	provider.mu.Unlock()
	assert.Equal(t, 1, calls, "provider must be called exactly once when a delta was streamed")
}

func TestSession_RunStopsOnContextCancel(t *testing.T) {
	// Run(ctx) must return when the provided ctx is cancelled, not just when
	// Close() is called. This prevents goroutine leaks per connection.
	provider := &scriptedProvider{}
	repo := newFakeRepo()
	sink := newSink()
	s, err := NewSession(context.Background(), Deps{
		Provider: provider, Catalog: &fakeCatalog{defs: nil}, Executor: &fakeExecutor{}, Repo: repo,
		Limiter: allowAllLimiter{},
		Cfg: Config{
			SystemPrompt: "sys", MaxIterations: 5, MaxTurnTokens: 100000,
			WindowTokens: 100000, ConfirmTTL: 50 * time.Millisecond,
		},
	}, "bob", "", sink)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Run(ctx)
	}()

	// Cancel the external context and expect Run to return within 1 s.
	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// Run returned as expected.
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
	// Close should still work (idempotent).
	s.Close()
}

// blockingProvider blocks inside StreamTurn until the turn context is cancelled.
type blockingProvider struct {
	inFlight chan struct{} // closed when StreamTurn is entered
}

func (p *blockingProvider) StreamTurn(ctx context.Context, _ ports.TurnRequest, _ func(string)) (*ports.TurnResult, error) {
	close(p.inFlight)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSession_InterruptCancelsInFlightTurn(t *testing.T) {
	// A blocking provider parks inside StreamTurn. Calling Interrupt() must
	// cancel the turn context, causing the provider to return, and the session
	// must emit a final:"" (empty final) event.
	provider := &blockingProvider{inFlight: make(chan struct{})}
	repo := newFakeRepo()
	sink := newSink()
	s, err := NewSession(context.Background(), Deps{
		Provider: provider, Catalog: &fakeCatalog{defs: nil}, Executor: &fakeExecutor{}, Repo: repo,
		Limiter: allowAllLimiter{},
		Cfg: Config{
			SystemPrompt: "sys", MaxIterations: 5, MaxTurnTokens: 100000,
			WindowTokens: 100000, ConfirmTTL: 50 * time.Millisecond,
		},
	}, "carol", "", sink)
	require.NoError(t, err)
	go s.Run(context.Background())
	t.Cleanup(s.Close)

	s.Enqueue("block me")

	// Wait until the provider is in-flight.
	select {
	case <-provider.inFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("provider never entered StreamTurn")
	}

	s.Interrupt()

	// The turn must end with a final:"" event.
	waitFor(t, sink, "final:")
}
