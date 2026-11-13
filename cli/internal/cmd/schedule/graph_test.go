package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeGraphRun is a RunClient fake for graph_test.go: it answers
// GetScheduleGraph and, embedding the interface, panics on any other call.
type fakeGraphRun struct {
	client.RunClient
	resp    *runv1.GetScheduleGraphResponse
	err     error
	gotName string
}

func (f *fakeGraphRun) GetScheduleGraph(_ context.Context, name string) (*runv1.GetScheduleGraphResponse, error) {
	f.gotName = name
	return f.resp, f.err
}

func (f *fakeGraphRun) Close() error { return nil }

// runGraph invokes the graph command end-to-end with the provided fake client.
// It captures stdout/stderr and returns the exit code.
func runGraph(t *testing.T, fake client.RunClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewGraphCommand(
		func(_ context.Context, _ string) (client.RunClient, error) { return fake, nil },
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

type graphNodeJSON struct {
	TableName   string `json:"table_name"`
	SchemaName  string `json:"schema_name"`
	ServiceName string `json:"service_name"`
	NodeType    string `json:"node_type"`
	// Dormant is a pointer so a test tells a node that says "dormant":false
	// from one that carries no such key.
	Dormant *bool `json:"dormant"`
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

func sampleGraph() *runv1.GetScheduleGraphResponse {
	return &runv1.GetScheduleGraphResponse{
		Nodes: []*runv1.ScheduleGraphNode{
			{
				TableName:    "orders",
				SchemaName:   "public",
				ServiceName:  "warehouse",
				NodeType:     "table",
				ScheduleName: "daily_ingest",
			},
		},
		Edges: []*runv1.ScheduleGraphEdge{
			{FromNodeId: "warehouse.public.orders", ToNodeId: "warehouse.public.revenue"},
		},
		PromotionSeq: 7,
	}
}

func TestGraph_SuccessEmitsCorrectJSON(t *testing.T) {
	fake := &fakeGraphRun{resp: sampleGraph()}

	stdout, stderr, exit := runGraph(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, "daily_ingest", fake.gotName)

	var payload graphResponseJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))

	assert.Equal(t, "daily_ingest", payload.ScheduleName)
	require.Len(t, payload.Nodes, 1)
	n := payload.Nodes[0]
	assert.Equal(t, "orders", n.TableName)
	assert.Equal(t, "public", n.SchemaName)
	assert.Equal(t, "warehouse", n.ServiceName)
	assert.Equal(t, "table", n.NodeType)

	require.Len(t, payload.Edges, 1)
	e := payload.Edges[0]
	assert.Equal(t, "warehouse.public.orders", e.FromNodeId)
	assert.Equal(t, "warehouse.public.revenue", e.ToNodeId)
}

// An unknown-external node is a table no node builds. The schedule graph carries it
// with node_type "unknown-external" and an empty service_name, and an edge from the
// node that reads it to its id (the empty service, then schema and table). The
// command emits all of it as it comes, and its help says what the type is and
// that the empty service makes its node id start with a dot, so a caller that
// reads the graph knows no node command can address it.
func TestGraph_UnknownExternalNodeKeepsItsTypeAndAnEmptyService(t *testing.T) {
	fake := &fakeGraphRun{resp: &runv1.GetScheduleGraphResponse{
		Nodes: []*runv1.ScheduleGraphNode{
			{
				SchemaName:  "raw",
				TableName:   "ingest_probe",
				ServiceName: "",
				NodeType:    "unknown-external",
			},
		},
		Edges: []*runv1.ScheduleGraphEdge{
			{FromNodeId: "a.m", ToNodeId: ".raw.ingest_probe"},
		},
	}}

	stdout, stderr, exit := runGraph(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)

	var payload graphResponseJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Nodes, 1)
	n := payload.Nodes[0]
	assert.Equal(t, "raw", n.SchemaName)
	assert.Equal(t, "ingest_probe", n.TableName)
	assert.Equal(t, "unknown-external", n.NodeType)
	assert.Empty(t, n.ServiceName)
	assert.Contains(t, stdout, `"service_name":""`, "the empty service is emitted, not omitted")
	require.Len(t, payload.Edges, 1)
	assert.Equal(t, "a.m", payload.Edges[0].FromNodeId)
	assert.Equal(t, ".raw.ingest_probe", payload.Edges[0].ToNodeId)

	cmd := NewGraphCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{})
	assert.Contains(t, cmd.Long, `A node of type "unknown-external" is a table continuo reads and does not build`, "the help says what an unknown-external node is")
	assert.Contains(t, cmd.Long, "its node id starts with a dot", "the help says what the empty service does to the node id")
}

// A dormant node is one no schedule tags: the graph carries it as an upstream
// of the nodes that read it, with "dormant": true. Every other node says
// "dormant": false, never leaves the key out. The help says what dormant means,
// how a dormant node runs and that every node carries the field, so a caller
// that reads the graph needs nothing else to tell the two apart.
func TestGraph_DormantNodeIsMarked(t *testing.T) {
	fake := &fakeGraphRun{resp: &runv1.GetScheduleGraphResponse{
		Nodes: []*runv1.ScheduleGraphNode{
			{
				TableName: "revenue", SchemaName: "public", ServiceName: "warehouse",
				NodeType: "dbt-model", ScheduleName: "daily_ingest",
			},
			{
				TableName: "scratch_rates", SchemaName: "staging", ServiceName: "warehouse",
				NodeType: "dbt-model", Dormant: true,
			},
			{
				TableName: "ingest_probe", SchemaName: "raw", NodeType: "unknown-external",
			},
		},
		Edges: []*runv1.ScheduleGraphEdge{
			{FromNodeId: "warehouse.public.revenue", ToNodeId: "warehouse.staging.scratch_rates"},
		},
	}}

	stdout, stderr, exit := runGraph(t, fake, []string{"daily_ingest"}, false)

	require.Equal(t, 0, exit, "stderr: %s", stderr)
	var payload graphResponseJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Nodes, 3)
	for _, n := range payload.Nodes {
		require.NotNil(t, n.Dormant, "every node carries dormant, %s.%s does not", n.SchemaName, n.TableName)
	}
	assert.False(t, *payload.Nodes[0].Dormant, "a node of a schedule is not dormant")
	assert.True(t, *payload.Nodes[1].Dormant, "a node of no schedule is dormant")
	assert.Equal(t, "dbt-model", payload.Nodes[1].NodeType, "a dormant node keeps its own type")
	assert.False(t, *payload.Nodes[2].Dormant, "a table nothing builds is never dormant")
	assert.Contains(t, stdout, `"dormant":true`)
	assert.Contains(t, stdout, `"dormant":false`)

	cmd := NewGraphCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{})
	long := strings.Join(strings.Fields(cmd.Long), " ")
	assert.Contains(t, long, `"dormant":boolean`, "the help shows the field in the output shape")
	assert.Contains(t, long, `"dormant" is true for a node that has no schedule`, "the help says what dormant means")
	assert.Contains(t, long, "No cron or schedule run includes it, except to rebuild a dormant view or materialized view below a seed",
		"the help says a run includes a dormant node only to rebuild a view below a seed whose load drops the seed's table")
	assert.Contains(t, long, "a rerun or rebase of that run rebuilds it again if the rebuild did not finish", "the help says a rerun or rebase finishes the rebuild")
	assert.Contains(t, long, `a run started by hand executes it ("continuo node trigger <service> <schema> <table>")`, "the help names the command that runs it")
	assert.Contains(t, cmd.Annotations["output_schema"], `"dormant":"boolean"`, "the output schema declares the field")
}

// TestScheduleGraph_OutputMatchesItsSchema holds the output_schema annotation
// to the JSON the command emits: the same keys at the top level and in every
// node and edge, and none of the fields the topology no longer carries.
func TestScheduleGraph_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeGraphRun{resp: sampleGraph()}
	stdout, _, exit := runGraph(t, fake, []string{"daily_ingest"}, false)
	require.Equal(t, 0, exit)

	cmd := NewGraphCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{})
	var schema map[string]any
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema))
	var emitted map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &emitted))

	assertSameKeys(t, "", schema, emitted)
	for _, gone := range []string{"owner", "criticality", "created_at", "last_updated_at", "status"} {
		assert.NotContains(t, cmd.Annotations["output_schema"], `"`+gone+`"`)
		assert.NotContains(t, cmd.Long, `"`+gone+`"`)
		assert.NotContains(t, stdout, `"`+gone+`"`)
	}
}

// assertSameKeys walks a schema and a value together and fails where an
// object has other keys than its schema. A schema leaf (a string) stands for
// any scalar or array; a one-element schema array describes every element.
func assertSameKeys(t *testing.T, path string, schema, value any) {
	t.Helper()
	switch sv := schema.(type) {
	case map[string]any:
		vv, ok := value.(map[string]any)
		require.True(t, ok, "%s: schema has an object, output does not", path)
		for k := range sv {
			assert.Contains(t, vv, k, "%s: schema key missing from the output", path)
		}
		for k := range vv {
			assert.Contains(t, sv, k, "%s: output key missing from the schema", path)
		}
		for k, child := range sv {
			if cv, ok := vv[k]; ok {
				assertSameKeys(t, path+"."+k, child, cv)
			}
		}
	case []any:
		vv, ok := value.([]any)
		require.True(t, ok, "%s: schema has an array, output does not", path)
		require.Len(t, sv, 1, "%s: an array schema describes one element", path)
		require.NotEmpty(t, vv, "%s: the fixture must carry an element to compare", path)
		for i, el := range vv {
			assertSameKeys(t, path+"[]"+string(rune('0'+i)), sv[0], el)
		}
	}
}

func TestGraph_EmptyNodesAndEdgesAreArraysNotNull(t *testing.T) {
	fake := &fakeGraphRun{resp: &runv1.GetScheduleGraphResponse{}}

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
	fake := &fakeGraphRun{
		resp: &runv1.GetScheduleGraphResponse{
			Nodes: make([]*runv1.ScheduleGraphNode, 12),
			Edges: make([]*runv1.ScheduleGraphEdge, 9),
		},
	}

	stdout, stderr, exit := runGraph(t, fake, []string{"daily_ingest"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "daily_ingest")
	assert.Contains(t, stderr, "12 nodes")
	assert.Contains(t, stderr, "9 edges")
}

func TestGraph_MissingArgumentExits2(t *testing.T) {
	fake := &fakeGraphRun{}

	stdout, _, exit := runGraph(t, fake, []string{}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestGraph_UnavailableExits5(t *testing.T) {
	fake := &fakeGraphRun{err: status.Error(codes.Unavailable, "server down")}

	_, _, exit := runGraph(t, fake, []string{"daily_ingest"}, false)

	assert.Equal(t, 5, exit)
}

func TestGraph_FactoryErrorExits5(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second}
	cmd := NewGraphCommand(
		func(_ context.Context, _ string) (client.RunClient, error) {
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
