package grpc

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// The server serves run.v1.RunService on the same listener as StateService: a
// call to the RunService the server was given reaches it, and StateService is
// still served.
func TestServer_ServesRunServiceBesideStateService(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := NewServer(0, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		runv1.UnimplementedRunServiceServer{}, logger, true)
	require.NoError(t, err)
	go func() { _ = srv.Start() }()
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	conn, err := grpclib.NewClient(srv.Addr(), grpclib.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// The generated stub of an unimplemented method answers Unimplemented,
	// which only a registered RunService can.
	_, err = runv1.NewRunServiceClient(conn).GetRun(context.Background(), &runv1.GetRunRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "GetRun")
	// Maintenance mode refuses a run-starting StateService call before its
	// handler, which proves StateService is served too.
	_, err = statev1.NewStateServiceClient(conn).TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{ScheduleName: "daily"})
	require.Equal(t, codes.Unavailable, status.Code(err))
}
