package grpcgate_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/carolsimone/continuo/pkg/maintenance"
	"github.com/carolsimone/continuo/pkg/maintenance/grpcgate"
)

const checkMethod = "/grpc.health.v1.Health/Check"

func healthClient(t *testing.T, ic grpc.UnaryServerInterceptor) healthpb.HealthClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(ic))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return healthpb.NewHealthClient(conn)
}

func TestGate_RefusesAGatedMethodWithTheTrailer(t *testing.T) {
	c := healthClient(t, grpcgate.UnaryServerInterceptor(true, checkMethod))
	var trailer metadata.MD
	_, err := c.Check(context.Background(), &healthpb.HealthCheckRequest{}, grpc.Trailer(&trailer))
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, maintenance.Message, status.Convert(err).Message())
	require.Equal(t, []string{"true"}, trailer.Get(maintenance.TrailerKey))
}

func TestGate_PassesAnUngatedMethod(t *testing.T) {
	c := healthClient(t, grpcgate.UnaryServerInterceptor(true, "/some.Other/Method"))
	_, err := c.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
}

func TestGate_PassesEverythingWhenOff(t *testing.T) {
	c := healthClient(t, grpcgate.UnaryServerInterceptor(false, checkMethod))
	var trailer metadata.MD
	_, err := c.Check(context.Background(), &healthpb.HealthCheckRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)
	require.Empty(t, trailer.Get(maintenance.TrailerKey))
}
