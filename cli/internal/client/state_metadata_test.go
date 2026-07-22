package client

import (
	"context"
	"net"
	"testing"

	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// capturingServer records the x-continuo-user-id metadata of the last
// TriggerSchedule call. It embeds Unimplemented so only the method we exercise
// is overridden.
type capturingServer struct {
	statev1.UnimplementedStateServiceServer
	gotUserID string
}

func (s *capturingServer) TriggerSchedule(ctx context.Context, _ *statev1.TriggerScheduleRequest) (*statev1.TriggerScheduleResponse, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-continuo-user-id"); len(v) > 0 {
			s.gotUserID = v[0]
		}
	}
	return &statev1.TriggerScheduleResponse{ScheduleId: "s1"}, nil
}

func dialCapturing(t *testing.T) (*stateGRPCClient, *capturingServer) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	cap := &capturingServer{}
	statev1.RegisterStateServiceServer(srv, cap)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &stateGRPCClient{conn: conn, api: statev1.NewStateServiceClient(conn)}, cap
}

func TestTriggerSchedule_SetsUserIDMetadata(t *testing.T) {
	c, cap := dialCapturing(t)
	_, err := c.TriggerSchedule(context.Background(), "daily", "okta.example.com|alice")
	require.NoError(t, err)
	assert.Equal(t, "okta.example.com|alice", cap.gotUserID)
}

func TestTriggerSchedule_EmptyActorSetsNoMetadata(t *testing.T) {
	c, cap := dialCapturing(t)
	_, err := c.TriggerSchedule(context.Background(), "daily", "")
	require.NoError(t, err)
	assert.Equal(t, "", cap.gotUserID)
}
