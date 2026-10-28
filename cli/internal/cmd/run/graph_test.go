package run

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func graphFake() *fakeRunClient {
	return &fakeRunClient{graph: &runv1.GetRunGraphResponse{
		Nodes: []*runv1.RunGraphNode{
			{ServiceName: "svc", SchemaName: "raw", TableName: "a", ScheduleName: "daily", NodeType: "model",
				Status: "pending", TaskId: "t-a", WaitReason: "node_lease", WaitingOnRunId: "run-other",
				WaitingSince: "2026-10-27T09:00:00Z"},
			{ServiceName: "svc", SchemaName: "raw", TableName: "b", Status: "succeeded", TaskId: "t-b"},
		},
		Edges:              []*runv1.RunGraphEdge{{FromNodeId: "svc.raw.a", ToNodeId: "svc.raw.b"}},
		RunPromotionSeq:    4,
		LatestPromotionSeq: 6,
	}}
}

func TestRunGraph_PrintsNodesEdgesAndPromotionSeqs(t *testing.T) {
	fake := graphFake()

	stdout, stderr, exit := execute(t, NewGraphCommand, fake, testRunID)

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	var p struct {
		Nodes []map[string]any `json:"nodes"`
		Edges []map[string]any `json:"edges"`
		Run   int64            `json:"run_promotion_seq"`
		Late  int64            `json:"latest_promotion_seq"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	require.Len(t, p.Nodes, 2)
	assert.Equal(t, "svc", p.Nodes[0]["service_name"])
	assert.Equal(t, "node_lease", p.Nodes[0]["wait_reason"])
	assert.Equal(t, "2026-10-27T09:00:00Z", p.Nodes[0]["waiting_since"])
	assert.NotContains(t, p.Nodes[1], "wait_reason", "a task that waits for nothing omits the wait fields")
	assert.Equal(t, []map[string]any{{"from_node_id": "svc.raw.a", "to_node_id": "svc.raw.b"}}, p.Edges)
	assert.Equal(t, int64(4), p.Run)
	assert.Equal(t, int64(6), p.Late)
	assert.Equal(t, 1, fake.graphCalls, "the graph is one call")
}

func TestRunGraph_AnUnknownRunPrintsEmptyListsNotNull(t *testing.T) {
	fake := &fakeRunClient{graph: &runv1.GetRunGraphResponse{}}

	stdout, _, exit := execute(t, NewGraphCommand, fake, testRunID)

	require.Equal(t, 0, exit)
	assert.JSONEq(t, `{"nodes":[],"edges":[],"run_promotion_seq":0,"latest_promotion_seq":0}`, stdout)
}

func TestRunGraph_OutputMatchesItsSchema(t *testing.T) {
	stdout, _, exit := execute(t, NewGraphCommand, graphFake(), testRunID)
	require.Equal(t, 0, exit)

	cmd := NewGraphCommand(nil, &config.Config{}, nil, nil)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}

func TestRunGraph_AnInvalidRunIDIsAUsageError(t *testing.T) {
	fake := &fakeRunClient{graphErr: status.Error(codes.InvalidArgument, "run_id is not a UUID")}

	stdout, _, exit := execute(t, NewGraphCommand, fake, "nope")

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestRunGraph_WrongArgumentCountIsRefusedExit2(t *testing.T) {
	_, _, exit := execute(t, NewGraphCommand, &fakeRunClient{})
	assert.Equal(t, 2, exit)
}
