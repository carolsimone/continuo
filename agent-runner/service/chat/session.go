package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/domain/repository"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/google/uuid"
)

// EventSink receives streaming events for one chat turn. All methods must be
// safe to call concurrently. Implementations decide whether to write to SSE,
// a channel, or a recorder.
type EventSink interface {
	// Thread is emitted once on session start with the thread's UUID.
	Thread(id uuid.UUID)
	// History is emitted on resume with the existing messages.
	History(msgs []domain.Message)
	// Tool is emitted before a non-mutating tool call is executed, or after
	// a mutating call is approved, with the human-readable CLI command.
	Tool(command string)
	// Text is emitted for each assistant text delta during streaming.
	Text(delta string)
	// Final is emitted when the provider produces text and no tool calls.
	Final(text string)
	// ConfirmRequest is emitted when a mutating tool call awaits human approval.
	ConfirmRequest(actionID uuid.UUID, summary string)
	// Error is emitted for terminal turn errors (rate_limited, iteration_limit, etc.).
	Error(code, message string)
}

// Config holds tunable parameters for a Session.
type Config struct {
	// SystemPrompt is prepended to every provider request.
	SystemPrompt string
	// MaxIterations caps the tool→result loop per turn to prevent runaway agents.
	MaxIterations int
	// MaxTurnTokens caps the cumulative token spend across iterations in one turn.
	MaxTurnTokens int
	// WindowTokens is the token budget passed to the sliding-window trimmer.
	WindowTokens int
	// ConfirmTTL is how long the session waits for a mutating tool approval.
	ConfirmTTL time.Duration
	// CLIName is the binary name used in human-readable command display (e.g. "continuo").
	CLIName string
}

// Deps bundles all collaborators required by a Session.
type Deps struct {
	Provider ports.LLMProvider
	Catalog  ports.ToolCatalog
	Executor ports.ToolExecutor
	Repo     repository.ThreadRepository
	Limiter  *RateLimiter
	Logger   *slog.Logger
	Cfg      Config
}

// confirmReply carries the user's decision on a pending action.
type confirmReply struct {
	actionID uuid.UUID
	approved bool
}

// Session is a per-connection agent loop that serializes turns for one user
// on one thread. Its Run goroutine must be started by the caller.
type Session struct {
	threadID uuid.UUID
	userID   string
	deps     Deps
	sink     EventSink

	// ctx / cancel govern the session lifetime; Close cancels ctx causing Run to exit.
	ctx    context.Context
	cancel context.CancelFunc

	queue    chan string       // inbound user messages
	confirms chan confirmReply // confirm/deny replies for pending actions
	done     chan struct{}     // closed when Run returns

	// turnCancel holds the cancel func for the in-flight turn (cap 1).
	// Interrupt pushes to this channel; the turn goroutine reads it.
	turnCancel chan context.CancelFunc

	// resumed is a pending tool confirmation carried over from a previous
	// connection. It is set on resume when a still-pending, non-expired action
	// exists, and cleared once handled. The approval/denial for it arrives
	// outside any in-flight turn and is processed by Run between turns. Accessed
	// only from the Run goroutine, so it needs no lock.
	resumed *domain.PendingAction

	mu     sync.Mutex
	closed bool
}

// NewSession creates or resumes a chat session. If threadID is empty a new
// thread is created; otherwise the existing thread is loaded and its history
// is replayed to the sink.
func NewSession(ctx context.Context, deps Deps, userID, threadID string, sink EventSink) (*Session, error) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	var thread *domain.Thread
	var err error

	if threadID == "" {
		thread, err = deps.Repo.CreateThread(ctx, userID)
		if err != nil {
			return nil, fmt.Errorf("create thread: %w", err)
		}
	} else {
		id, parseErr := uuid.Parse(threadID)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid thread id: %w", parseErr)
		}
		thread, err = deps.Repo.GetThread(ctx, id, userID)
		if err != nil {
			return nil, fmt.Errorf("get thread: %w", err)
		}
	}

	sink.Thread(thread.ID)

	// resumed holds a pending confirmation carried over from a previous
	// connection, if any (only meaningful when resuming an existing thread).
	var resumed *domain.PendingAction

	if threadID != "" {
		msgs, err := deps.Repo.ListMessages(ctx, thread.ID)
		if err != nil {
			return nil, fmt.Errorf("list messages: %w", err)
		}
		if len(msgs) > 0 {
			sink.History(msgs)
		}

		// If a mutating tool confirmation was still pending (and not yet expired)
		// when the previous connection dropped, re-offer it so an approval can
		// run the tool and continue the turn. ErrNotFound simply means there is
		// nothing to resume.
		action, err := deps.Repo.GetPendingAction(ctx, thread.ID)
		switch {
		case err == nil:
			resumed = action
			sink.ConfirmRequest(action.ID, action.Summary)
		case errors.Is(err, repository.ErrNotFound):
			// nothing to resume
		default:
			return nil, fmt.Errorf("get pending action: %w", err)
		}
	}

	sessionCtx, sessionCancel := context.WithCancel(ctx)

	s := &Session{
		threadID:   thread.ID,
		userID:     userID,
		deps:       deps,
		sink:       sink,
		ctx:        sessionCtx,
		cancel:     sessionCancel,
		queue:      make(chan string, 64),
		confirms:   make(chan confirmReply, 16),
		done:       make(chan struct{}),
		turnCancel: make(chan context.CancelFunc, 1),
		resumed:    resumed,
	}
	return s, nil
}

// ThreadID returns the UUID of the thread this session operates on.
func (s *Session) ThreadID() uuid.UUID {
	return s.threadID
}

// Enqueue submits a user message for processing. It is rate-limited; if the
// user exceeds their per-minute quota the sink receives an error event and
// the message is dropped.
func (s *Session) Enqueue(text string) {
	if !s.deps.Limiter.Allow(s.userID, time.Now()) {
		s.sink.Error("rate_limited", "too many messages; please wait before sending another")
		return
	}
	select {
	case s.queue <- text:
	default:
		s.sink.Error("queue_full", "message queue is full; please wait before sending another")
	}
}

// Confirm submits the user's approval or denial for a pending action.
func (s *Session) Confirm(actionID uuid.UUID, approved bool) {
	select {
	case s.confirms <- confirmReply{actionID: actionID, approved: approved}:
	default:
	}
}

// Interrupt cancels the in-flight turn, if any. The current turn emits
// Final("") and exits; the next queued message will start a new turn.
func (s *Session) Interrupt() {
	select {
	case cancel := <-s.turnCancel:
		cancel()
		// Return the func so runTurn's deferred drain can collect it.
		s.turnCancel <- cancel
	default:
	}
}

// Close shuts down the session. It cancels the session context and waits
// for Run to return.
func (s *Session) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	<-s.done
}

// defaultMaxResponseTokens is the per-call token cap sent to the provider.
// It bounds individual completions independently of the cumulative turn budget.
const defaultMaxResponseTokens = 4096

// Run is the main event loop. It must be started in a goroutine by the
// caller. It runs until either the provided ctx is cancelled or Close() is
// called — whichever comes first.
func (s *Session) Run(ctx context.Context) {
	defer close(s.done)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ctx.Done():
			return
		case text := <-s.queue:
			s.runTurn(text)
		case reply := <-s.confirms:
			// A confirm reaching the Run loop arrived between turns: while a turn
			// is in flight, runTurn blocks here and executeWithConfirm consumes
			// confirms itself, so this case fires only when idle. It is the
			// approval/denial for a confirmation resumed from a prior connection.
			s.handleResumedConfirm(reply)
		}
	}
}

// runTurn persists the user message, then drives the provider→tool loop.
func (s *Session) runTurn(text string) {
	// Persist user message.
	userContent, _ := json.Marshal(domain.TextContent{Text: text})
	if _, err := s.deps.Repo.AppendMessage(s.ctx, s.threadID, domain.RoleUser, userContent); err != nil {
		s.sink.Error("internal", fmt.Sprintf("persist user message: %v", err))
		return
	}
	s.driveTurn()
}

// driveTurn runs the provider→tool loop up to MaxIterations times against the
// current persisted history, emitting events to the sink throughout. The caller
// appends whatever message starts the turn first — a user message for a fresh
// turn, or the tool result for a resumed confirmation.
func (s *Session) driveTurn() {
	ctx := s.ctx

	// Set up a per-turn context so Interrupt() can abort only this turn.
	turnCtx, turnCancel := context.WithCancel(ctx)
	defer turnCancel()
	// Publish the cancel func so Interrupt() can pull it.
	s.turnCancel <- turnCancel
	defer func() { <-s.turnCancel }()

	var tokenSpend int

	for iter := 0; iter < s.deps.Cfg.MaxIterations; iter++ {
		// Load and window the conversation history.
		allMsgs, err := s.deps.Repo.ListMessages(ctx, s.threadID)
		if err != nil {
			s.sink.Error("internal", fmt.Sprintf("list messages: %v", err))
			return
		}
		msgs := window(allMsgs, s.deps.Cfg.WindowTokens)

		req := ports.TurnRequest{
			System:    s.deps.Cfg.SystemPrompt,
			Messages:  msgs,
			Tools:     s.deps.Catalog.Tools(),
			MaxTokens: defaultMaxResponseTokens,
		}

		// Stream one provider turn with one retry on transient failure.
		// Retry is only attempted if the first attempt streamed nothing; if any
		// delta was emitted we must not retry (that would duplicate streamed text).
		var result *ports.TurnResult
		var streamed bool
		result, err = s.deps.Provider.StreamTurn(turnCtx, req, func(delta string) {
			streamed = true
			s.sink.Text(delta)
		})
		if err != nil {
			if turnCtx.Err() != nil {
				s.sink.Final("")
				return
			}
			if streamed {
				// Partial stream already delivered — do not retry to avoid duplication.
				if strings.Contains(err.Error(), "429") {
					s.sink.Error("rate_limited", fmt.Sprintf("provider rate limit: %v", err))
				} else {
					s.sink.Error("provider_unavailable", fmt.Sprintf("provider error: %v", err))
				}
				return
			}
			// Nothing streamed yet — safe to retry once.
			var streamed2 bool
			result, err = s.deps.Provider.StreamTurn(turnCtx, req, func(delta string) {
				streamed2 = true
				s.sink.Text(delta)
			})
			_ = streamed2
			if err != nil {
				if turnCtx.Err() != nil {
					s.sink.Final("")
					return
				}
				if strings.Contains(err.Error(), "429") {
					s.sink.Error("rate_limited", fmt.Sprintf("provider rate limit: %v", err))
				} else {
					s.sink.Error("provider_unavailable", fmt.Sprintf("provider error: %v", err))
				}
				return
			}
		}

		// Check for interrupt after provider call.
		if turnCtx.Err() != nil {
			s.sink.Final("")
			return
		}

		tokenSpend += result.Usage.InputTokens + result.Usage.OutputTokens
		if tokenSpend > s.deps.Cfg.MaxTurnTokens {
			s.sink.Error("token_budget", "turn exceeded token budget")
			return
		}

		// Persist assistant text when the model produced any.
		if result.Text != "" {
			assistantContent, _ := json.Marshal(domain.TextContent{Text: result.Text})
			if _, err := s.deps.Repo.AppendMessage(ctx, s.threadID, domain.RoleAssistant, assistantContent); err != nil {
				s.sink.Error("internal", fmt.Sprintf("persist assistant message: %v", err))
				return
			}
		}

		// No tool calls — this is the final response.
		if len(result.ToolCalls) == 0 {
			s.sink.Final(result.Text)
			return
		}

		// Execute each tool call and persist call+result.
		for _, call := range result.ToolCalls {
			// Persist the tool call.
			callContent, _ := json.Marshal(domain.ToolCallContent{
				CallID: call.ID,
				Tool:   call.Name,
				Args:   call.Args,
			})
			if _, err := s.deps.Repo.AppendMessage(ctx, s.threadID, domain.RoleToolCall, callContent); err != nil {
				s.sink.Error("internal", fmt.Sprintf("persist tool call: %v", err))
				return
			}

			// Execute (with optional confirm gate for mutating tools).
			toolResult, suspended := s.executeWithConfirm(turnCtx, call)
			if suspended {
				// The client disconnected while awaiting approval. The pending
				// action is left pending (resumable) and this tool call dangles
				// without a result; stop the turn. On reconnect the confirmation
				// is re-offered and an approval continues the turn from here.
				return
			}

			// Persist the tool result.
			resultContent, _ := json.Marshal(domain.ToolResultContent{
				CallID:  call.ID,
				Output:  toolResult.Output,
				IsError: toolResult.IsError,
			})
			if _, err := s.deps.Repo.AppendMessage(ctx, s.threadID, domain.RoleToolResult, resultContent); err != nil {
				s.sink.Error("internal", fmt.Sprintf("persist tool result: %v", err))
				return
			}

			if turnCtx.Err() != nil {
				s.sink.Final("")
				return
			}
		}
	}

	// Iteration cap reached without the model producing a final text response.
	s.sink.Error("iteration_limit", fmt.Sprintf("turn exceeded %d iterations", s.deps.Cfg.MaxIterations))
}

// executeWithConfirm runs one tool call, pausing for human approval when the
// tool is marked Mutating. It returns the tool result and whether the turn was
// suspended: suspended is true only when the client disconnected while awaiting
// approval, in which case the pending action is left pending (resumable) and the
// returned result must be ignored (the caller stops the turn without persisting
// a result). In every other case suspended is false and the result is populated.
func (s *Session) executeWithConfirm(ctx context.Context, call ports.ToolCall) (ports.ToolResult, bool) {
	def, ok := s.deps.Catalog.Lookup(call.Name)
	if !ok {
		return ports.ToolResult{
			Output:  fmt.Sprintf(`{"error":"unknown tool %q"}`, call.Name),
			IsError: true,
		}, false
	}

	cmd := commandString(s.deps.Cfg.CLIName, def, call.Args)

	if !def.Mutating {
		s.sink.Tool(cmd)
		return s.deps.Executor.Execute(ctx, s.userID, s.threadID, call), false
	}

	// Mutating tool: create a pending action and wait for approval.
	now := time.Now()
	action := &domain.PendingAction{
		ID:        uuid.New(),
		ThreadID:  s.threadID,
		Tool:      call.Name,
		Args:      call.Args,
		Summary:   fmt.Sprintf("Run `%s`?", cmd),
		Status:    domain.ActionPending,
		CreatedAt: now,
		ExpiresAt: now.Add(s.deps.Cfg.ConfirmTTL),
	}

	if err := s.deps.Repo.CreatePendingAction(ctx, action); err != nil {
		return ports.ToolResult{
			Output:  fmt.Sprintf(`{"error":"failed to create pending action: %v"}`, err),
			IsError: true,
		}, false
	}

	// Drain any stale confirm replies that arrived before this action was
	// created. Without this drain, a burst of prior-action replies can fill
	// the channel and cause the live approval to be dropped.
	for {
		select {
		case <-s.confirms:
		default:
			goto drained
		}
	}
drained:

	s.sink.ConfirmRequest(action.ID, action.Summary)

	denial := ports.ToolResult{
		Output:  `{"denied":"the user declined this action; do not retry"}`,
		IsError: true,
	}

	timer := time.NewTimer(s.deps.Cfg.ConfirmTTL)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Distinguish a disconnect from an in-session interrupt. When the
			// whole session is shutting down (s.ctx cancelled — the gRPC stream
			// dropped or the service is stopping), leave the action pending and
			// non-expired so a reconnect can resume the confirmation; do not run
			// the tool and do not persist a result. When only this turn was
			// cancelled (Interrupt, s.ctx still alive), expire the action — the
			// user actively abandoned it.
			if s.ctx.Err() != nil {
				return ports.ToolResult{}, true
			}
			_ = s.deps.Repo.ResolvePendingAction(context.Background(), action.ID, domain.ActionExpired)
			return denial, false

		case <-timer.C:
			_ = s.deps.Repo.ResolvePendingAction(context.Background(), action.ID, domain.ActionExpired)
			return denial, false

		case reply := <-s.confirms:
			if reply.actionID != action.ID {
				// Stale reply for a different action; keep waiting.
				continue
			}
			if !reply.approved {
				_ = s.deps.Repo.ResolvePendingAction(ctx, action.ID, domain.ActionDenied)
				return denial, false
			}
			_ = s.deps.Repo.ResolvePendingAction(ctx, action.ID, domain.ActionApproved)
			s.sink.Tool(cmd)
			return s.deps.Executor.Execute(ctx, s.userID, s.threadID, call), false
		}
	}
}

// handleResumedConfirm processes the approval or denial of a confirmation that
// was resumed from a previous connection (see Run). The matching tool call was
// persisted before the disconnect and left without a result; this appends that
// result — the tool's output on approval, a denial marker otherwise — and then
// continues the turn so the model can react to the outcome.
func (s *Session) handleResumedConfirm(reply confirmReply) {
	if s.resumed == nil || reply.actionID != s.resumed.ID {
		return // not the resumed action (or nothing to resume)
	}
	action := s.resumed
	s.resumed = nil

	call, ok := s.danglingToolCall()
	if !ok {
		// No dangling tool call to complete (history already consistent); just
		// record the terminal status so the row is not left pending.
		status := domain.ActionApproved
		if !reply.approved {
			status = domain.ActionDenied
		}
		_ = s.deps.Repo.ResolvePendingAction(s.ctx, action.ID, status)
		return
	}

	var toolResult ports.ToolResult
	if reply.approved {
		if err := s.deps.Repo.ResolvePendingAction(s.ctx, action.ID, domain.ActionApproved); err != nil {
			s.sink.Error("internal", fmt.Sprintf("approve pending action: %v", err))
			return
		}
		if def, ok := s.deps.Catalog.Lookup(call.Name); ok {
			s.sink.Tool(commandString(s.deps.Cfg.CLIName, def, call.Args))
		}
		toolResult = s.deps.Executor.Execute(s.ctx, s.userID, s.threadID, call)
	} else {
		_ = s.deps.Repo.ResolvePendingAction(s.ctx, action.ID, domain.ActionDenied)
		toolResult = ports.ToolResult{
			Output:  `{"denied":"the user declined this action; do not retry"}`,
			IsError: true,
		}
	}

	resultContent, _ := json.Marshal(domain.ToolResultContent{
		CallID:  call.ID,
		Output:  toolResult.Output,
		IsError: toolResult.IsError,
	})
	if _, err := s.deps.Repo.AppendMessage(s.ctx, s.threadID, domain.RoleToolResult, resultContent); err != nil {
		s.sink.Error("internal", fmt.Sprintf("persist tool result: %v", err))
		return
	}

	// Continue the turn so the model sees the tool result and produces a reply.
	s.driveTurn()
}

// danglingToolCall returns the most recent persisted tool call that has no
// matching tool result — the call left pending when a confirmation was
// suspended on a prior connection. It returns false when none is found.
func (s *Session) danglingToolCall() (ports.ToolCall, bool) {
	msgs, err := s.deps.Repo.ListMessages(s.ctx, s.threadID)
	if err != nil {
		return ports.ToolCall{}, false
	}
	resolved := map[string]bool{}
	for _, m := range msgs {
		if m.Role == domain.RoleToolResult {
			var rc domain.ToolResultContent
			if json.Unmarshal(m.Content, &rc) == nil {
				resolved[rc.CallID] = true
			}
		}
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role != domain.RoleToolCall {
			continue
		}
		var cc domain.ToolCallContent
		if json.Unmarshal(msgs[i].Content, &cc) != nil {
			continue
		}
		if resolved[cc.CallID] {
			continue
		}
		return ports.ToolCall{ID: cc.CallID, Name: cc.Tool, Args: cc.Args}, true
	}
	return ports.ToolCall{}, false
}

// commandString renders the CLI command as a human-readable string for display.
// It mirrors the argv construction in the cliexec adapter without importing it.
func commandString(cliName string, def ports.ToolDef, args map[string]string) string {
	parts := append([]string{cliName}, def.CLIPath...)
	for _, name := range def.ParamOrder {
		if v, ok := args[name]; ok && strings.TrimSpace(v) != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, " ")
}
