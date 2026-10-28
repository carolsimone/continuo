package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/output"
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

// scheduleEditServer answers the schedule-edit RPCs from a script of errors:
// call n of an RPC fails with errs[n] while there is one, then succeeds. It
// records every request and the user id each upsert and delete carried.
type scheduleEditServer struct {
	runv1.UnimplementedRunServiceServer

	mu          sync.Mutex
	upsertErrs  []error
	deleteErrs  []error
	upserts     []*runv1.UpsertScheduleRequest
	deletes     []*runv1.DeleteScheduleRequest
	previews    []*runv1.PreviewScheduleRequest
	gotUserIDs  []string
	previewResp *runv1.PreviewScheduleResponse
	previewErr  error
}

func (s *scheduleEditServer) userID(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-continuo-user-id"); len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func (s *scheduleEditServer) UpsertSchedule(ctx context.Context, req *runv1.UpsertScheduleRequest) (*runv1.UpsertScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, req)
	s.gotUserIDs = append(s.gotUserIDs, s.userID(ctx))
	if n := len(s.upserts); len(s.upsertErrs) >= n && s.upsertErrs[n-1] != nil {
		return nil, s.upsertErrs[n-1]
	}
	return &runv1.UpsertScheduleResponse{Schedule: &runv1.Schedule{ScheduleName: req.GetScheduleName(), Version: 4}}, nil
}

func (s *scheduleEditServer) DeleteSchedule(ctx context.Context, req *runv1.DeleteScheduleRequest) (*runv1.DeleteScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes = append(s.deletes, req)
	s.gotUserIDs = append(s.gotUserIDs, s.userID(ctx))
	if n := len(s.deletes); len(s.deleteErrs) >= n && s.deleteErrs[n-1] != nil {
		return nil, s.deleteErrs[n-1]
	}
	return &runv1.DeleteScheduleResponse{}, nil
}

func (s *scheduleEditServer) PreviewSchedule(_ context.Context, req *runv1.PreviewScheduleRequest) (*runv1.PreviewScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.previews = append(s.previews, req)
	return s.previewResp, s.previewErr
}

// dialScheduleEdit serves srv over an in-memory connection and returns a client
// built with the production dial options, with the retry delays shortened.
func dialScheduleEdit(t *testing.T, srv *scheduleEditServer, extra ...grpc.ServerOption) *runGRPCClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	g := grpc.NewServer(extra...)
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

func TestUpsertSchedule_SendsTheRequestAndTheActor(t *testing.T) {
	srv := &scheduleEditServer{}
	c := dialScheduleEdit(t, srv)

	resp, err := c.UpsertSchedule(context.Background(), "okta.example.com|alice", &runv1.UpsertScheduleRequest{
		ScheduleName: "daily", Cron: "0 1 * * *", Timezone: "Europe/Paris", Description: "d", Paused: true, ExpectedVersion: 3,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 4, resp.GetSchedule().GetVersion())
	require.Len(t, srv.upserts, 1)
	assert.Equal(t, "daily", srv.upserts[0].GetScheduleName())
	assert.EqualValues(t, 3, srv.upserts[0].GetExpectedVersion())
	assert.Equal(t, []string{"okta.example.com|alice"}, srv.gotUserIDs)
}

func TestDeleteSchedule_SendsTheNameAndTheActor(t *testing.T) {
	srv := &scheduleEditServer{}
	c := dialScheduleEdit(t, srv)

	_, err := c.DeleteSchedule(context.Background(), "alice", "daily")
	require.NoError(t, err)
	require.Len(t, srv.deletes, 1)
	assert.Equal(t, "daily", srv.deletes[0].GetScheduleName())
	assert.Equal(t, []string{"alice"}, srv.gotUserIDs)
}

func TestPreviewSchedule_SendsTheCronAndTheTimezone(t *testing.T) {
	srv := &scheduleEditServer{previewResp: &runv1.PreviewScheduleResponse{CronWords: "At 01:00"}}
	c := dialScheduleEdit(t, srv)

	resp, err := c.PreviewSchedule(context.Background(), "0 1 * * *", "Europe/Paris")
	require.NoError(t, err)
	assert.Equal(t, "At 01:00", resp.GetCronWords())
	require.Len(t, srv.previews, 1)
	assert.Equal(t, "0 1 * * *", srv.previews[0].GetCron())
	assert.Equal(t, "Europe/Paris", srv.previews[0].GetTimezone())
}

// ABORTED on an upsert is a stale expected_version. Sending the same request
// again cannot settle it, so it is returned at once and the caller reads the
// schedule again.
func TestRunClient_DoesNotRetryAScheduleEditOnAborted(t *testing.T) {
	aborted := status.Error(codes.Aborted, "the schedule changed")
	srv := &scheduleEditServer{upsertErrs: []error{aborted, aborted, aborted}, deleteErrs: []error{aborted, aborted, aborted}}
	c := dialScheduleEdit(t, srv)

	_, err := c.UpsertSchedule(context.Background(), "", &runv1.UpsertScheduleRequest{ScheduleName: "daily", ExpectedVersion: 3})
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.Len(t, srv.upserts, 1, "a stale version is sent once")

	_, err = c.DeleteSchedule(context.Background(), "", "daily")
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.Len(t, srv.deletes, 1)
}

func TestRunClient_RetriesAScheduleEditOnUnavailable(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "connection reset")
	srv := &scheduleEditServer{upsertErrs: []error{unavailable, unavailable}, deleteErrs: []error{unavailable}}
	c := dialScheduleEdit(t, srv)

	_, err := c.UpsertSchedule(context.Background(), "", &runv1.UpsertScheduleRequest{ScheduleName: "daily"})
	require.NoError(t, err, "the third try succeeds")
	assert.Len(t, srv.upserts, 3)

	_, err = c.DeleteSchedule(context.Background(), "", "daily")
	require.NoError(t, err)
	assert.Len(t, srv.deletes, 2)
}

func TestRunClient_GivesUpAScheduleEditAfterThreeTriesOnUnavailable(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "down")
	srv := &scheduleEditServer{upsertErrs: []error{unavailable, unavailable, unavailable, unavailable}}
	c := dialScheduleEdit(t, srv)

	_, err := c.UpsertSchedule(context.Background(), "", &runv1.UpsertScheduleRequest{ScheduleName: "daily"})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Len(t, srv.upserts, 3)
}

func TestRunClient_DoesNotRetryAScheduleEditRefusedForMaintenance(t *testing.T) {
	srv := &scheduleEditServer{upsertErrs: []error{status.Error(codes.Unavailable, "continuo is in maintenance mode")}}
	c := dialScheduleEdit(t, srv, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if err != nil {
			_ = grpc.SetTrailer(ctx, metadata.Pairs(maintenanceTrailerKey, "true"))
		}
		return resp, err
	}))

	_, err := c.UpsertSchedule(context.Background(), "", &runv1.UpsertScheduleRequest{ScheduleName: "daily"})
	require.ErrorIs(t, err, output.ErrMaintenance)
	assert.Len(t, srv.upserts, 1, "maintenance lasts until an operator ends it; a retry cannot succeed")
}

func TestRunClient_DoesNotRetryAScheduleEditThatFailedForAnotherReason(t *testing.T) {
	srv := &scheduleEditServer{upsertErrs: []error{status.Error(codes.InvalidArgument, "cron: expected exactly 5 fields")}}
	c := dialScheduleEdit(t, srv)

	_, err := c.UpsertSchedule(context.Background(), "", &runv1.UpsertScheduleRequest{ScheduleName: "daily", Cron: "x"})
	require.Error(t, err)
	assert.Len(t, srv.upserts, 1)
}

// A preview is a read: it is sent once, whatever it fails with.
func TestRunClient_DoesNotRetryAPreview(t *testing.T) {
	srv := &scheduleEditServer{previewErr: status.Error(codes.Unavailable, "connection reset")}
	c := dialScheduleEdit(t, srv)

	_, err := c.PreviewSchedule(context.Background(), "0 1 * * *", "UTC")
	require.Error(t, err)
	assert.Len(t, srv.previews, 1)
}
