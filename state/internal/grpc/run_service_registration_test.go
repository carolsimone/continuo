package grpc

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
)

// The server serves run.v1.RunService, and no other service beside gRPC
// reflection: a call to the RunService the server was given reaches it.
func TestServer_ServesRunServiceOnly(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := NewServer(0, runv1.UnimplementedRunServiceServer{}, logger, true)
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

	var served []string
	for name := range srv.grpcServer.GetServiceInfo() {
		if !strings.HasPrefix(name, "grpc.reflection.") {
			served = append(served, name)
		}
	}
	require.Equal(t, []string{runv1.RunService_ServiceDesc.ServiceName}, served)
}
