package grpc_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	grpcadapter "github.com/carolsimone/continuo/orchestrator/adapters/grpc"
	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
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
	handler := grpcadapter.NewQueryHandler(stubScheduleAndRunLists{}, stubDriftAwareRuns{}, stubCodeVersionHistoryReader{}, stubPrecedentHistoryReader{}, logger)
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
		{"GetNodeVersions", func() error {
			_, err := client.GetNodeVersions(ctx, &orchestratorv1.GetNodeVersionsRequest{UniqueId: "x"})
			return err
		}},
		{"GetNodeVersionDiff", func() error {
			_, err := client.GetNodeVersionDiff(ctx, &orchestratorv1.GetNodeVersionDiffRequest{UniqueId: "x", FromSeq: 1, ToSeq: 2})
			return err
		}},
		{"GetUpstreamChanges", func() error {
			_, err := client.GetUpstreamChanges(ctx, &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "x"})
			return err
		}},
		{"GetCodeUnitVersions", func() error {
			_, err := client.GetCodeUnitVersions(ctx, &orchestratorv1.GetCodeUnitVersionsRequest{UnitId: "x"})
			return err
		}},
		{"GetNodeRunHistory", func() error {
			_, err := client.GetNodeRunHistory(ctx, &orchestratorv1.GetNodeRunHistoryRequest{UniqueId: "x"})
			return err
		}},
		{"GetPrecedents", func() error {
			_, err := client.GetPrecedents(ctx, &orchestratorv1.GetPrecedentsRequest{Signature: "x"})
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
func (stubScheduleAndRunLists) ListRuns(context.Context, string, int, int) ([]*domain.RunSummary, int, error) {
	return nil, 0, nil
}
func (stubScheduleAndRunLists) ListScheduleTopologies(context.Context) ([]*domain.ScheduleTopologySummary, error) {
	return nil, nil
}
func (stubScheduleAndRunLists) GetNodeAncestry(context.Context, string, int) ([]*domain.NodeAncestor, error) {
	return nil, nil
}
func (stubScheduleAndRunLists) GetNode(context.Context, string, string, string) (*domain.NodeMeta, error) {
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

// stubCodeVersionHistoryReader satisfies grpcadapter.CodeVersionHistoryReader.
type stubCodeVersionHistoryReader struct{}

func (stubCodeVersionHistoryReader) GetNodeVersions(context.Context, string, int32, bool) ([]codeversion.VersionView, error) {
	return nil, nil
}
func (stubCodeVersionHistoryReader) GetNodeVersionDiff(context.Context, string, int64, int64) (*codeversion.VersionDiff, error) {
	return &codeversion.VersionDiff{}, nil
}
func (stubCodeVersionHistoryReader) GetUpstreamChanges(context.Context, string, int32, time.Time) ([]codeversion.UpstreamChange, error) {
	return nil, nil
}
func (stubCodeVersionHistoryReader) GetCodeUnitVersions(context.Context, string, string, int32) ([]codeversion.UnitVersionView, error) {
	return nil, nil
}
func (stubCodeVersionHistoryReader) GetNodeRunHistory(context.Context, string, int32, string) ([]codeversion.RunExecution, error) {
	return nil, nil
}

// stubPrecedentHistoryReader satisfies grpcadapter.PrecedentHistoryReader.
type stubPrecedentHistoryReader struct{}

func (stubPrecedentHistoryReader) GetPrecedents(context.Context, string, string, string, int32, bool) ([]casebase.Precedent, error) {
	return nil, nil
}
