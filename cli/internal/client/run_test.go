package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// scriptedRunServer answers StartRun and ListSchedules from a script of
// errors: call n fails with errs[n] while there is one, then succeeds. It
// records the request id and the user id of every StartRun and counts the
// calls of each RPC.
type scriptedRunServer struct {
	runv1.UnimplementedRunServiceServer

	mu            sync.Mutex
	errs          []error
	startCalls    int
	startIDs      []string
	listCalls     int
	gotUserID     string
	gotSingleNode *runv1.TriggerSingleNodeRunRequest
	gotCancel     *runv1.CancelActiveRunRequest
}

func (s *scriptedRunServer) StartRun(ctx context.Context, req *runv1.StartRunRequest) (*runv1.StartRunResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startCalls++
	s.startIDs = append(s.startIDs, req.GetRequestId())
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-continuo-user-id"); len(v) > 0 {
			s.gotUserID = v[0]
		}
	}
	if len(s.errs) >= s.startCalls && s.errs[s.startCalls-1] != nil {
		return nil, s.errs[s.startCalls-1]
	}
	return &runv1.StartRunResponse{RunId: "r1"}, nil
}

func (s *scriptedRunServer) ListSchedules(context.Context, *runv1.ListSchedulesRequest) (*runv1.ListSchedulesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	if len(s.errs) >= s.listCalls && s.errs[s.listCalls-1] != nil {
		return nil, s.errs[s.listCalls-1]
	}
	return &runv1.ListSchedulesResponse{}, nil
}

func (s *scriptedRunServer) TriggerSingleNodeRun(_ context.Context, req *runv1.TriggerSingleNodeRunRequest) (*runv1.TriggerSingleNodeRunResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gotSingleNode = req
	return &runv1.TriggerSingleNodeRunResponse{RunId: "r2"}, nil
}

func (s *scriptedRunServer) CancelActiveRun(_ context.Context, req *runv1.CancelActiveRunRequest) (*runv1.CancelActiveRunResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gotCancel = req
	return &runv1.CancelActiveRunResponse{RunId: "r3"}, nil
}

// dialScripted serves srv over an in-memory connection and returns a client
// built with the production dial options, with the retry delays shortened so
// tests do not sleep.
func dialScripted(t *testing.T, srv *scriptedRunServer) *runGRPCClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	g := grpc.NewServer()
	runv1.RegisterRunServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	prev := retryDelays
	retryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { retryDelays = prev })

	opts := append(runDialOptions(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &runGRPCClient{conn: conn, api: runv1.NewRunServiceClient(conn)}
}

func TestRunClient_RetriesACommandOnUnavailableWithTheSameRequestID(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "connection reset")
	srv := &scriptedRunServer{errs: []error{unavailable, unavailable}}
	c := dialScripted(t, srv)

	resp, err := c.TriggerSchedule(context.Background(), "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "daily", "")
	require.NoError(t, err, "the third try succeeds")
	assert.Equal(t, "r1", resp.GetRunId())
	assert.Equal(t, 3, srv.startCalls, "a command is tried up to three times")
	assert.Equal(t, []string{"5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b"}, srv.startIDs,
		"every try carries the id of the first")
}

func TestRunClient_GivesUpAfterThreeTriesOnUnavailable(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "down")
	srv := &scriptedRunServer{errs: []error{unavailable, unavailable, unavailable, unavailable}}
	c := dialScripted(t, srv)

	_, err := c.TriggerSchedule(context.Background(), "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "daily", "")
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, 3, srv.startCalls)
}

func TestRunClient_DoesNotRetryACommandThatFailedForAnotherReason(t *testing.T) {
	srv := &scriptedRunServer{errs: []error{status.Error(codes.FailedPrecondition, "a run is already active")}}
	c := dialScripted(t, srv)

	_, err := c.TriggerSchedule(context.Background(), "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "daily", "")
	require.Error(t, err)
	assert.Equal(t, 1, srv.startCalls)
}

func TestRunClient_DoesNotRetryAMaintenanceRefusal(t *testing.T) {
	srv := &scriptedRunServer{errs: []error{status.Error(codes.Unavailable, "continuo is in maintenance mode")}}
	lis := bufconn.Listen(1024 * 1024)
	g := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if err != nil {
			_ = grpc.SetTrailer(ctx, metadata.Pairs(maintenanceTrailerKey, "true"))
		}
		return resp, err
	}))
	runv1.RegisterRunServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	opts := append(runDialOptions(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := &runGRPCClient{conn: conn, api: runv1.NewRunServiceClient(conn)}

	_, err = c.TriggerSchedule(context.Background(), "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "daily", "")
	require.Error(t, err)
	assert.Equal(t, 1, srv.startCalls, "maintenance lasts until an operator ends it; a retry cannot succeed")
}

func TestRunClient_DoesNotRetryARead(t *testing.T) {
	srv := &scriptedRunServer{errs: []error{status.Error(codes.Unavailable, "connection reset")}}
	c := dialScripted(t, srv)

	_, err := c.ListAllSchedules(context.Background())
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, 1, srv.listCalls, "a read is sent once")
}

func TestRetryDelays_AreOneHundredThenFourHundredMilliseconds(t *testing.T) {
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 400 * time.Millisecond}, retryDelays)
}

func TestRunClient_StopsRetryingWhenTheContextEnds(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "down")
	srv := &scriptedRunServer{errs: []error{unavailable, unavailable}}
	c := dialScripted(t, srv)
	retryDelays = []time.Duration{time.Hour, time.Hour}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.TriggerSchedule(ctx, "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "daily", "")
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second, "the wait between tries ends with the context")
}

func TestTriggerSchedule_SetsUserIDMetadata(t *testing.T) {
	srv := &scriptedRunServer{}
	c := dialScripted(t, srv)
	_, err := c.TriggerSchedule(context.Background(), "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "daily", "okta.example.com|alice")
	require.NoError(t, err)
	assert.Equal(t, "okta.example.com|alice", srv.gotUserID)
}

func TestTriggerSchedule_EmptyActorSetsNoMetadata(t *testing.T) {
	srv := &scriptedRunServer{}
	c := dialScripted(t, srv)
	_, err := c.TriggerSchedule(context.Background(), "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "daily", "")
	require.NoError(t, err)
	assert.Equal(t, "", srv.gotUserID)
}

func TestTriggerNodeRun_SendsTheRequestIDAndTheMetadataMode(t *testing.T) {
	srv := &scriptedRunServer{}
	c := dialScripted(t, srv)
	const id = "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b"

	_, err := c.TriggerNodeRun(context.Background(), id, "svc", "sch", "tbl", "", "")
	require.NoError(t, err)
	assert.Equal(t, id, srv.gotSingleNode.GetRequestId())
	assert.Equal(t, "latest", srv.gotSingleNode.GetMetadataSource())
	assert.Empty(t, srv.gotSingleNode.GetSourceRunId())
	assert.Empty(t, srv.gotSingleNode.GetOperation())

	_, err = c.TriggerNodeFullRefresh(context.Background(), id, "svc", "sch", "tbl", "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "")
	require.NoError(t, err)
	assert.Equal(t, "snapshot_of_run", srv.gotSingleNode.GetMetadataSource())
	assert.Equal(t, "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", srv.gotSingleNode.GetSourceRunId())
	assert.Equal(t, "full_refresh", srv.gotSingleNode.GetOperation())

	_, err = c.TriggerNodeTest(context.Background(), id, "svc", "sch", "tbl", "", "")
	require.NoError(t, err)
	assert.Equal(t, "test", srv.gotSingleNode.GetOperation())

	_, err = c.TriggerNodeBuild(context.Background(), id, "svc", "sch", "tbl", "", "")
	require.NoError(t, err)
	assert.Equal(t, "build", srv.gotSingleNode.GetOperation())
}

func TestCancelSchedule_SendsTheRequestIDAndWhoCancelled(t *testing.T) {
	srv := &scriptedRunServer{}
	c := dialScripted(t, srv)

	resp, err := c.CancelSchedule(context.Background(), "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "daily", "bad data", "alice")
	require.NoError(t, err)
	assert.Equal(t, "r3", resp.GetRunId())
	assert.Equal(t, "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", srv.gotCancel.GetRequestId())
	assert.Equal(t, "daily", srv.gotCancel.GetScheduleName())
	assert.Equal(t, "bad data", srv.gotCancel.GetCancellationReason())
	assert.Equal(t, "alice", srv.gotCancel.GetCancelledBy())
}
