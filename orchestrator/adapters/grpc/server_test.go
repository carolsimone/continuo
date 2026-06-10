package grpc_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	grpcadapter "github.com/carolsimone/continuo/orchestrator/adapters/grpc"
	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestServer_RoutesEveryOrchestratorQueryRPC guards the failure mode where
// Server embeds UnimplementedOrchestratorQueryServer and a new RPC is added
// to the proto without a matching delegate on *Server — the binary then
// silently returns codes.Unimplemented at runtime, which only e2e catches.
//
// The test brings the gRPC server up on an ephemeral port, connects a real
// client, and invokes every RPC. Any RPC missing a direct delegate would
// return codes.Unimplemented from the embedded UnimplementedOrchestratorQueryServer.
func TestServer_RoutesEveryOrchestratorQueryRPC(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	handler := grpcadapter.NewQueryHandler(stubScheduleAndRunLists{}, stubDriftAwareRuns{}, logger)
	server, err := grpcadapter.NewServer(0, handler, logger) // 0 = ephemeral port
	require.NoError(t, err)
	go func() { _ = server.Start() }()
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(
		server.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := orchestratorv1.NewOrchestratorQueryClient(conn)
	ctx := context.Background()

	// Each call below would return codes.Unimplemented if its delegate were
	// missing on *Server. We don't care about response payloads here — the
	// stub returns empty values — only that the call dispatches to our handler.
	cases := []struct {
		name string
		call func() error
	}{
		{"GetScheduleGraph", func() error {
			_, err := client.GetScheduleGraph(ctx, &orchestratorv1.GetScheduleGraphRequest{ScheduleName: "x"})
			return err
		}},
		{"ListRuns", func() error {
			_, err := client.ListRuns(ctx, &orchestratorv1.ListRunsRequest{ScheduleName: "x"})
			return err
		}},
		{"GetRunGraph", func() error {
			_, err := client.GetRunGraph(ctx, &orchestratorv1.GetRunGraphRequest{RunId: "x"})
			return err
		}},
		{"ListActiveRunDrifts", func() error {
			_, err := client.ListActiveRunDrifts(ctx, &orchestratorv1.ListActiveRunDriftsRequest{})
			return err
		}},
		{"ListScheduleTopologies", func() error {
			_, err := client.ListScheduleTopologies(ctx, &orchestratorv1.ListScheduleTopologiesRequest{})
			return err
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.call()
			if err == nil {
				return // dispatched and stub succeeded — fine
			}
			st, _ := status.FromError(err)
			assert.NotEqualf(t, codes.Unimplemented, st.Code(),
				"%s returned Unimplemented — Server is missing a delegate for it",
				c.name)
		})
	}
}

// stubScheduleAndRunLists satisfies grpcadapter.ScheduleAndRunListReader.
type stubScheduleAndRunLists struct{}

func (stubScheduleAndRunLists) GetScheduleGraph(context.Context, string) (*domain.ScheduleGraph, error) {
	return &domain.ScheduleGraph{}, nil
}
func (stubScheduleAndRunLists) ListRuns(context.Context, string) ([]*domain.RunSummary, error) {
	return nil, nil
}
func (stubScheduleAndRunLists) ListScheduleTopologies(context.Context) ([]*domain.ScheduleTopologySummary, error) {
	return nil, nil
}

// stubDriftAwareRuns satisfies grpcadapter.DriftAwareRunReader.
type stubDriftAwareRuns struct{}

func (stubDriftAwareRuns) GetRunGraph(context.Context, string) (*queries.RunGraphView, error) {
	return &queries.RunGraphView{}, nil
}
func (stubDriftAwareRuns) ListActiveRunDrifts(context.Context) (*queries.ActiveRunDriftView, error) {
	return &queries.ActiveRunDriftView{}, nil
}
