package grpcserver

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/domain/repository"
	agentchatv1 "github.com/carolsimone/continuo/agent-runner/proto/agentchat/v1"
	"github.com/carolsimone/continuo/agent-runner/service/chat"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the gRPC adapter that bridges AgentChat bidi streams onto chat.Session.
type Server struct {
	agentchatv1.UnimplementedAgentChatServer
	deps   chat.Deps
	logger *slog.Logger

	// maxSessions caps the number of concurrent Chat streams this instance
	// serves (each holds a goroutine and an upstream provider stream open).
	// Zero or less means unlimited. active tracks the live count.
	maxSessions int
	active      atomic.Int64
}

// NewServer creates a Server. If logger is nil, slog.Default() is used.
// maxSessions caps concurrent Chat streams (<= 0 means unlimited).
func NewServer(deps chat.Deps, logger *slog.Logger, maxSessions int) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{deps: deps, logger: logger, maxSessions: maxSessions}
}

// streamSink adapts chat.EventSink to a gRPC bidi stream. All Send calls are
// mutex-guarded because session.Run and the Chat receive loop can call them
// concurrently from different goroutines.
type streamSink struct {
	mu     sync.Mutex
	stream grpc.BidiStreamingServer[agentchatv1.ClientEvent, agentchatv1.ServerEvent]
}

func (s *streamSink) send(ev *agentchatv1.ServerEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.stream.Send(ev)
}

// Thread emits the thread UUID immediately after session creation or resume.
func (s *streamSink) Thread(id uuid.UUID) {
	s.send(&agentchatv1.ServerEvent{
		Event: &agentchatv1.ServerEvent_Thread{
			Thread: &agentchatv1.Thread{ThreadId: id.String()},
		},
	})
}

// History replays existing messages to the client on thread resume.
func (s *streamSink) History(msgs []domain.Message) {
	var hmsgs []*agentchatv1.HistoryMessage
	for _, m := range msgs {
		hm := toHistoryMessage(m)
		if hm == nil {
			continue
		}
		hmsgs = append(hmsgs, hm)
	}
	s.send(&agentchatv1.ServerEvent{
		Event: &agentchatv1.ServerEvent_History{
			History: &agentchatv1.History{Messages: hmsgs},
		},
	})
}

// Tool emits the human-readable CLI command string for an upcoming tool execution.
func (s *streamSink) Tool(command string) {
	s.send(&agentchatv1.ServerEvent{
		Event: &agentchatv1.ServerEvent_Tool{
			Tool: &agentchatv1.Tool{Command: command},
		},
	})
}

// Text emits a streaming assistant text delta.
func (s *streamSink) Text(delta string) {
	s.send(&agentchatv1.ServerEvent{
		Event: &agentchatv1.ServerEvent_Text{
			Text: &agentchatv1.Text{Text: delta},
		},
	})
}

// Final emits the complete assistant response text when the turn ends.
func (s *streamSink) Final(text string) {
	s.send(&agentchatv1.ServerEvent{
		Event: &agentchatv1.ServerEvent_Final{
			Final: &agentchatv1.Final{Text: text},
		},
	})
}

// ConfirmRequest emits a mutating-tool approval request to the client.
func (s *streamSink) ConfirmRequest(actionID uuid.UUID, summary string) {
	s.send(&agentchatv1.ServerEvent{
		Event: &agentchatv1.ServerEvent_ConfirmRequest{
			ConfirmRequest: &agentchatv1.ConfirmRequest{
				ActionId: actionID.String(),
				Summary:  summary,
			},
		},
	})
}

// Error emits a terminal turn error to the client.
func (s *streamSink) Error(code, message string) {
	s.send(&agentchatv1.ServerEvent{
		Event: &agentchatv1.ServerEvent_Error{
			Error: &agentchatv1.Error{Code: code, Message: message},
		},
	})
}

// toHistoryMessage converts a domain.Message to its proto representation.
// RoleUser and RoleAssistant produce {role, text}; RoleToolCall produces
// {role:"tool", command: tool name}; RoleToolResult is internal and returns nil
// (callers must skip nil results).
func toHistoryMessage(m domain.Message) *agentchatv1.HistoryMessage {
	switch m.Role {
	case domain.RoleUser, domain.RoleAssistant:
		var tc domain.TextContent
		if err := json.Unmarshal(m.Content, &tc); err != nil {
			return nil
		}
		return &agentchatv1.HistoryMessage{Role: string(m.Role), Text: tc.Text}
	case domain.RoleToolCall:
		var cc domain.ToolCallContent
		if err := json.Unmarshal(m.Content, &cc); err != nil {
			return nil
		}
		return &agentchatv1.HistoryMessage{Role: "tool", Command: cc.Tool}
	case domain.RoleToolResult:
		// Tool results are internal; they are not replayed to the client.
		return nil
	default:
		return nil
	}
}

// Chat is the gRPC bidi streaming handler. The first event must be an Open with
// a non-empty UserId. Subsequent events drive the session: UserMessage enqueues
// text, ConfirmResponse approves/denies a pending action, Interrupt cancels the
// in-flight turn, and NewChat resets to a fresh thread on the same stream.
func (s *Server) Chat(stream grpc.BidiStreamingServer[agentchatv1.ClientEvent, agentchatv1.ServerEvent]) error {
	// Concurrency cap: bound the number of live streams this instance serves so
	// a burst of connections cannot exhaust goroutines / upstream provider
	// streams. Reserve a slot up front and release it when the stream ends.
	if s.maxSessions > 0 {
		if n := s.active.Add(1); n > int64(s.maxSessions) {
			s.active.Add(-1)
			s.logger.Warn("chat session rejected: at capacity", "max_sessions", s.maxSessions)
			return status.Error(codes.ResourceExhausted, "agent-runner at capacity; please retry shortly")
		}
		defer s.active.Add(-1)
	}

	// The first event must be Open.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	openMsg := first.GetOpen()
	if openMsg == nil || openMsg.GetUserId() == "" {
		return status.Error(codes.InvalidArgument, "first event must be Open with a non-empty user_id")
	}

	sink := &streamSink{stream: stream}
	session, err := chat.NewSession(stream.Context(), s.deps, openMsg.GetUserId(), openMsg.GetThreadId(), sink)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return status.Errorf(codes.NotFound, "open session: %v", err)
		}
		return status.Errorf(codes.Internal, "open session: %v", err)
	}
	defer session.Close()

	go session.Run(stream.Context())

	for {
		ev, err := stream.Recv()
		if err != nil {
			// Client closed the stream or the context was cancelled. session.Run
			// exits when the stream context is done, so we just return.
			return nil
		}

		switch e := ev.GetEvent().(type) {
		case *agentchatv1.ClientEvent_UserMessage:
			session.Enqueue(e.UserMessage.GetText())

		case *agentchatv1.ClientEvent_ConfirmResponse:
			cr := e.ConfirmResponse
			actionID, parseErr := uuid.Parse(cr.GetActionId())
			if parseErr != nil {
				s.logger.Warn("invalid action_id in ConfirmResponse", "action_id", cr.GetActionId(), "error", parseErr)
				continue
			}
			session.Confirm(actionID, cr.GetApproved())

		case *agentchatv1.ClientEvent_Interrupt:
			session.Interrupt()

		case *agentchatv1.ClientEvent_NewChat:
			// Interrupt any in-flight turn, close the current session, then start a
			// fresh one on a new thread. The sink and stream remain the same.
			session.Interrupt()
			session.Close()

			session, err = chat.NewSession(stream.Context(), s.deps, openMsg.GetUserId(), "", sink)
			if err != nil {
				return status.Errorf(codes.Internal, "new chat session: %v", err)
			}
			go session.Run(stream.Context())

		case *agentchatv1.ClientEvent_Open:
			// Duplicate Open on an already-open stream is a protocol error.
			s.logger.Warn("unexpected Open on established stream; ignoring")
		}
	}
}
