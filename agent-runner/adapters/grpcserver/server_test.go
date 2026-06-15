package grpcserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-runner/adapters/grpcserver"
	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/domain/repository"
	agentchatv1 "github.com/carolsimone/continuo/agent-runner/proto/agentchat/v1"
	"github.com/carolsimone/continuo/agent-runner/service/chat"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeRepo struct {
	mu      sync.Mutex
	threads map[uuid.UUID]*domain.Thread
	msgs    map[uuid.UUID][]domain.Message
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{threads: map[uuid.UUID]*domain.Thread{}, msgs: map[uuid.UUID][]domain.Message{}}
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
func (f *fakeRepo) CreatePendingAction(context.Context, *domain.PendingAction) error { return nil }
func (f *fakeRepo) ResolvePendingAction(context.Context, uuid.UUID, domain.ActionStatus) error {
	return nil
}
func (f *fakeRepo) GetPendingAction(context.Context, uuid.UUID) (*domain.PendingAction, error) {
	return nil, repository.ErrNotFound
}
func (f *fakeRepo) ListIdleThreads(context.Context, time.Time, int) ([]domain.Thread, error) {
	return nil, nil
}
func (f *fakeRepo) DeleteThread(context.Context, uuid.UUID) error { return nil }

type fakeProvider struct{}

func (fakeProvider) StreamTurn(_ context.Context, _ ports.TurnRequest, onDelta func(string)) (*ports.TurnResult, error) {
	onDelta("All ")
	onDelta("good.")
	return &ports.TurnResult{Text: "All good."}, nil
}

type emptyCatalog struct{}

func (emptyCatalog) Tools() []ports.ToolDef              { return nil }
func (emptyCatalog) Lookup(string) (ports.ToolDef, bool) { return ports.ToolDef{}, false }

type noopExecutor struct{}

func (noopExecutor) Execute(context.Context, string, uuid.UUID, ports.ToolCall) ports.ToolResult {
	return ports.ToolResult{}
}

func dialServer(t *testing.T) agentchatv1.AgentChatClient {
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	deps := chat.Deps{
		Provider: fakeProvider{}, Catalog: emptyCatalog{}, Executor: noopExecutor{},
		Repo: newFakeRepo(), Limiter: chat.NewRateLimiter(100),
		Cfg: chat.Config{SystemPrompt: "s", MaxIterations: 3, MaxTurnTokens: 100000, WindowTokens: 100000, ConfirmTTL: time.Second, CLIName: "continuo"},
	}
	agentchatv1.RegisterAgentChatServer(srv, grpcserver.NewServer(deps, nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return agentchatv1.NewAgentChatClient(conn)
}

func TestChat_OpenThenMessageStreamsTextAndFinal(t *testing.T) {
	client := dialServer(t)
	stream, err := client.Chat(context.Background())
	require.NoError(t, err)

	require.NoError(t, stream.Send(&agentchatv1.ClientEvent{Event: &agentchatv1.ClientEvent_Open{Open: &agentchatv1.Open{UserId: "alice"}}}))

	ev, err := stream.Recv()
	require.NoError(t, err)
	threadID := ev.GetThread().GetThreadId()
	assert.NotEmpty(t, threadID)

	require.NoError(t, stream.Send(&agentchatv1.ClientEvent{Event: &agentchatv1.ClientEvent_UserMessage{UserMessage: &agentchatv1.UserMessage{Text: "hi"}}}))

	var sawText, sawFinal bool
	for !sawFinal {
		ev, err = stream.Recv()
		require.NoError(t, err)
		if ev.GetText() != nil {
			sawText = true
		}
		if ev.GetFinal() != nil {
			sawFinal = true
			assert.Equal(t, "All good.", ev.GetFinal().GetText())
		}
	}
	assert.True(t, sawText)
}

func TestChat_FirstEventMustBeOpen(t *testing.T) {
	client := dialServer(t)
	stream, err := client.Chat(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&agentchatv1.ClientEvent{Event: &agentchatv1.ClientEvent_UserMessage{UserMessage: &agentchatv1.UserMessage{Text: "hi"}}}))
	_, err = stream.Recv()
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
