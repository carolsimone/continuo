package chat

import (
	"context"
	"encoding/json"
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
	Limiter  ports.RateLimiter
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

	if threadID != "" {
		msgs, err := deps.Repo.ListMessages(ctx, thread.ID)
		if err != nil {
			return nil, fmt.Errorf("list messages: %w", err)
		}
		if len(msgs) > 0 {
			sink.History(msgs)
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
	allowed, err := s.deps.Limiter.Allow(s.ctx, s.userID)
	if err != nil {
		// Fail open: a transient limiter-backend failure must not block a
		// legitimate user. Log it so a persistent outage is visible.
		s.deps.Logger.Warn("rate limiter backend error; allowing message", "user_id", s.userID, "error", err)
	} else if !allowed {
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
		}
	}
}

// runTurn persists the user message, then drives the provider→tool loop
// up to MaxIterations times, emitting events to the sink throughout.
func (s *Session) runTurn(text string) {
	ctx := s.ctx

	// Persist user message.
	userContent, _ := json.Marshal(domain.TextContent{Text: text})
	if _, err := s.deps.Repo.AppendMessage(ctx, s.threadID, domain.RoleUser, userContent); err != nil {
		s.sink.Error("internal", fmt.Sprintf("persist user message: %v", err))
		return
	}

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
			toolResult := s.executeWithConfirm(turnCtx, call)

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
// tool is marked Mutating. The returned ToolResult is always populated.
func (s *Session) executeWithConfirm(ctx context.Context, call ports.ToolCall) ports.ToolResult {
	def, ok := s.deps.Catalog.Lookup(call.Name)
	if !ok {
		return ports.ToolResult{
			Output:  fmt.Sprintf(`{"error":"unknown tool %q"}`, call.Name),
			IsError: true,
		}
	}

	cmd := commandString(s.deps.Cfg.CLIName, def, call.Args)

	if !def.Mutating {
		s.sink.Tool(cmd)
		return s.deps.Executor.Execute(ctx, s.userID, s.threadID, call)
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
		}
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
			// A disconnect (WebSocket/gRPC drop or shutdown) deliberately expires
			// the pending action: a mutating tool must never run without a live,
			// explicit approval, so a confirmation does NOT survive a reconnect.
			// The persisted row records the expired outcome for audit; on resume
			// the client re-asks and a fresh confirm_request is issued.
			_ = s.deps.Repo.ResolvePendingAction(context.Background(), action.ID, domain.ActionExpired)
			return denial

		case <-timer.C:
			_ = s.deps.Repo.ResolvePendingAction(context.Background(), action.ID, domain.ActionExpired)
			return denial

		case reply := <-s.confirms:
			if reply.actionID != action.ID {
				// Stale reply for a different action; keep waiting.
				continue
			}
			if !reply.approved {
				_ = s.deps.Repo.ResolvePendingAction(ctx, action.ID, domain.ActionDenied)
				return denial
			}
			_ = s.deps.Repo.ResolvePendingAction(ctx, action.ID, domain.ActionApproved)
			s.sink.Tool(cmd)
			return s.deps.Executor.Execute(ctx, s.userID, s.threadID, call)
		}
	}
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
