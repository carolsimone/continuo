package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeOrchestrator is an OrchestratorClient fake for graph_test.go.
type fakeOrchestrator struct {
	resp *orchestratorv1.GetScheduleGraphResponse
	err  error
}

func (f *fakeOrchestrator) GetScheduleGraph(_ context.Context, _ string) (*orchestratorv1.GetScheduleGraphResponse, error) {
	return f.resp, f.err
}

func (f *fakeOrchestrator) GetNodeVersions(context.Context, string, int32, bool) (*orchestratorv1.GetNodeVersionsResponse, error) {
	panic("GetNodeVersions should not be called in graph tests")
}

func (f *fakeOrchestrator) GetNodeVersionDiff(context.Context, string, int64, int64) (*orchestratorv1.GetNodeVersionDiffResponse, error) {
	panic("GetNodeVersionDiff should not be called in graph tests")
}

func (f *fakeOrchestrator) GetUpstreamChanges(context.Context, string, int32, string) (*orchestratorv1.GetUpstreamChangesResponse, error) {
	panic("GetUpstreamChanges should not be called in graph tests")
}

func (f *fakeOrchestrator) GetCodeUnitVersions(context.Context, string, string, int32) (*orchestratorv1.GetCodeUnitVersionsResponse, error) {
	panic("GetCodeUnitVersions should not be called in graph tests")
}

func (f *fakeOrchestrator) GetNodeRunHistory(context.Context, string, int32, string) (*orchestratorv1.GetNodeRunHistoryResponse, error) {
	panic("GetNodeRunHistory should not be called in graph tests")
}

func (f *fakeOrchestrator) GetPrecedents(context.Context, string, string, string, int32, bool) (*orchestratorv1.GetPrecedentsResponse, error) {
	panic("GetPrecedents should not be called in graph tests")
}

func (f *fakeOrchestrator) Close() error { return nil }

// runGraph invokes the graph command end-to-end with the provided fake client.
// It captures stdout/stderr and returns the exit code.
func runGraph(t *testing.T, fake client.OrchestratorClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewGraphCommand(
		func(_ context.Context, _ string) (client.OrchestratorClient, error) { return fake, nil },
		cfg,
		&outBuf,
		&errBuf,
	)
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	exit = 0
	if err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			exit = cliErr.ExitCode()
		} else {
			exit = 1
		}
	}
	return outBuf.String(), errBuf.String(), exit
}

// graphResponseJSON mirrors the expected JSON shape for assertions.
type graphNodeJSON struct {
	TableName     string `json:"table_name"`
	SchemaName    string `json:"schema_name"`
	ServiceName   string `json:"service_name"`
	Owner         string `json:"owner,omitempty"`
	Criticality   string `json:"criticality"`
	NodeType      string `json:"node_type"`
	Status        string `json:"status"`
	LastUpdatedAt string `json:"last_updated_at,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
}

type graphEdgeJSON struct {
	FromNodeId string `json:"from_node_id"`
	ToNodeId   string `json:"to_node_id"`
}

type graphResponseJSON struct {
	ScheduleName string          `json:"schedule_name"`
	Nodes        []graphNodeJSON `json:"nodes"`
	Edges        []graphEdgeJSON `json:"edges"`
}

func TestGraph_SuccessEmitsCorrectJSON(t *testing.T) {
	lastUpdated := time.Date(2026, 4, 20, 3, 1, 0, 0, time.UTC)
	createdAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	fake := &fakeOrchestrator{
		resp: &orchestratorv1.GetScheduleGraphResponse{
			Nodes: []*orchestratorv1.TableNode{
				{
					TableName:     "orders",
					SchemaName:    "public",
					ServiceName:   "warehouse",
					Owner:         "data-eng",
					Criticality:   orchestratorv1.Criticality_CRITICALITY_CORE,
					NodeType:      "table",
					Status:        "success",
					LastUpdatedAt: timestamppb.New(lastUpdated),
					CreatedAt:     timestamppb.New(createdAt),
				},
			},
			Edges: []*orchestratorv1.GraphEdge{
				{
					FromNodeId: "warehouse.public.orders",
					ToNodeId:   "warehouse.public.revenue",
				},
			},
		},
	}

	stdout, stderr, exit := runGraph(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)

	var payload graphResponseJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))

	assert.Equal(t, "daily_ingest", payload.ScheduleName)
	require.Len(t, payload.Nodes, 1)
	n := payload.Nodes[0]
	assert.Equal(t, "orders", n.TableName)
	assert.Equal(t, "public", n.SchemaName)
	assert.Equal(t, "warehouse", n.ServiceName)
	assert.Equal(t, "data-eng", n.Owner)
	assert.Equal(t, "core", n.Criticality)
	assert.Equal(t, "table", n.NodeType)
	assert.Equal(t, "success", n.Status)
	assert.Equal(t, "2026-04-20T03:01:00Z", n.LastUpdatedAt)
	assert.Equal(t, "2026-01-01T00:00:00Z", n.CreatedAt)

	require.Len(t, payload.Edges, 1)
	e := payload.Edges[0]
	assert.Equal(t, "warehouse.public.orders", e.FromNodeId)
	assert.Equal(t, "warehouse.public.revenue", e.ToNodeId)
}

func TestGraph_EmptyNodesAndEdgesAreArraysNotNull(t *testing.T) {
	fake := &fakeOrchestrator{
		resp: &orchestratorv1.GetScheduleGraphResponse{
			Nodes: []*orchestratorv1.TableNode{},
			Edges: []*orchestratorv1.GraphEdge{},
		},
	}

	stdout, _, exit := runGraph(t, fake, []string{"x"}, false)

	assert.Equal(t, 0, exit)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(stdout), &raw))

	assert.Equal(t, `"x"`, string(raw["schedule_name"]))

	var nodes []interface{}
	require.NoError(t, json.Unmarshal(raw["nodes"], &nodes))
	assert.Empty(t, nodes, "nodes must be an empty array, not null")

	var edges []interface{}
	require.NoError(t, json.Unmarshal(raw["edges"], &edges))
	assert.Empty(t, edges, "edges must be an empty array, not null")
}

func TestGraph_HumanModeSummaryLine(t *testing.T) {
	fake := &fakeOrchestrator{
		resp: &orchestratorv1.GetScheduleGraphResponse{
			Nodes: make([]*orchestratorv1.TableNode, 12),
			Edges: make([]*orchestratorv1.GraphEdge, 9),
		},
	}

	stdout, stderr, exit := runGraph(t, fake, []string{"daily_ingest"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "daily_ingest")
	assert.Contains(t, stderr, "12 nodes")
	assert.Contains(t, stderr, "9 edges")
}

func TestGraph_ZeroTimestampsOmittedFromJSON(t *testing.T) {
	fake := &fakeOrchestrator{
		resp: &orchestratorv1.GetScheduleGraphResponse{
			Nodes: []*orchestratorv1.TableNode{
				{
					TableName:     "orders",
					SchemaName:    "public",
					ServiceName:   "warehouse",
					Criticality:   orchestratorv1.Criticality_CRITICALITY_UNSPECIFIED,
					LastUpdatedAt: nil, // zero
					CreatedAt:     nil, // zero
				},
			},
			Edges: []*orchestratorv1.GraphEdge{},
		},
	}

	stdout, _, exit := runGraph(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)

	var payload struct {
		Nodes []map[string]json.RawMessage `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Nodes, 1)

	_, hasLastUpdatedAt := payload.Nodes[0]["last_updated_at"]
	assert.False(t, hasLastUpdatedAt, "last_updated_at must be omitted when nil")

	_, hasCreatedAt := payload.Nodes[0]["created_at"]
	assert.False(t, hasCreatedAt, "created_at must be omitted when nil")
}

func TestGraph_CriticalityStripping(t *testing.T) {
	cases := []struct {
		proto    orchestratorv1.Criticality
		expected string
	}{
		{orchestratorv1.Criticality_CRITICALITY_CORE, "core"},
		{orchestratorv1.Criticality_CRITICALITY_REGULATORY, "regulatory"},
		{orchestratorv1.Criticality_CRITICALITY_SECONDARY, "secondary"},
		{orchestratorv1.Criticality_CRITICALITY_UNSPECIFIED, "unspecified"},
	}

	for _, tc := range cases {
		t.Run(tc.expected, func(t *testing.T) {
			fake := &fakeOrchestrator{
				resp: &orchestratorv1.GetScheduleGraphResponse{
					Nodes: []*orchestratorv1.TableNode{
						{Criticality: tc.proto},
					},
					Edges: []*orchestratorv1.GraphEdge{},
				},
			}

			stdout, _, exit := runGraph(t, fake, []string{"sched"}, false)
			assert.Equal(t, 0, exit)

			var payload graphResponseJSON
			require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
			require.Len(t, payload.Nodes, 1)
			assert.Equal(t, tc.expected, payload.Nodes[0].Criticality)
		})
	}
}

func TestGraph_MissingArgumentExits2(t *testing.T) {
	fake := &fakeOrchestrator{}

	stdout, _, exit := runGraph(t, fake, []string{}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestGraph_NotFoundExits3(t *testing.T) {
	fake := &fakeOrchestrator{err: status.Error(codes.NotFound, "schedule not found")}

	stdout, _, exit := runGraph(t, fake, []string{"missing"}, false)

	assert.Equal(t, 3, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeNotFound, envelope.Error.Code)
	assert.False(t, envelope.Error.Retryable)
}

func TestGraph_UnavailableExits5(t *testing.T) {
	fake := &fakeOrchestrator{err: status.Error(codes.Unavailable, "server down")}

	_, _, exit := runGraph(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 5, exit)
}

func TestGraph_FactoryErrorExits5(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second}
	cmd := NewGraphCommand(
		func(_ context.Context, _ string) (client.OrchestratorClient, error) {
			return nil, status.Error(codes.Unavailable, "dial failed")
		},
		cfg,
		&outBuf,
		&errBuf,
	)
	cmd.SetArgs([]string{"daily_ingest"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()

	var cliErr output.CLIError
	require.True(t, errors.As(err, &cliErr))
	assert.Equal(t, 5, cliErr.ExitCode())
}
