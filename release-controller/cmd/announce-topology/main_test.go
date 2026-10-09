package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAnnouncer records what the command asked for and answers with the
// configured result or error.
type fakeAnnouncer struct {
	releaseID string
	topo      release.Topology
	current   release.Topology
	result    handlers.AnnounceResult
	err       error
}

func (f *fakeAnnouncer) Announce(_ context.Context, releaseID string, topo release.Topology) (handlers.AnnounceResult, error) {
	f.releaseID, f.topo = releaseID, topo
	return f.result, f.err
}

func (f *fakeAnnouncer) ReannounceCurrent(context.Context) (handlers.AnnounceResult, error) {
	return f.result, f.err
}

func (f *fakeAnnouncer) CurrentTopology(context.Context) (release.Topology, error) {
	return f.current, f.err
}

func runWith(t *testing.T, a *fakeAnnouncer, stdin string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	connect := func(context.Context) (topologyAnnouncer, func(), error) { return a, func() {}, nil }
	code := run(context.Background(), args, strings.NewReader(stdin), &stdout, &stderr, connect)
	return code, stdout.String(), stderr.String()
}

const twoNodes = `[{"unique_id":"bench.n1","service_name":"bench","node_type":"dbt-model","upstream_unique_ids":[]},
 {"unique_id":"bench.n2","service_name":"bench","node_type":"dbt-model","upstream_unique_ids":["bench.n1"]}]`

func TestRun_AnnouncesATopologyFromStdin(t *testing.T) {
	a := &fakeAnnouncer{result: handlers.AnnounceResult{ReleaseID: "bench-1", PromotionSeq: 4, TopologyURI: "s3://b/k", TopologySHA256: "f00d"}}
	code, stdout, _ := runWith(t, a, twoNodes, "--release-id", "bench-1", "--topology", "-")

	require.Equal(t, exitOK, code)
	assert.Equal(t, "bench-1", a.releaseID)
	require.Len(t, a.topo, 2)
	assert.Equal(t, []string{"bench.n1"}, a.topo[1].UpstreamUniqueIDs)
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &out))
	assert.Equal(t, map[string]any{"release_id": "bench-1", "promotion_seq": float64(4), "topology_uri": "s3://b/k", "topology_sha256": "f00d"}, out)
}

func TestRun_UsageErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stdin string
		args  []string
	}{
		{name: "no mode", args: nil},
		{name: "two modes", args: []string{"--reannounce-current", "--print-current"}},
		{name: "topology without a release id", stdin: twoNodes, args: []string{"--topology", "-"}},
		{name: "release id without a topology", args: []string{"--release-id", "bench-1"}},
		{name: "not JSON", stdin: "{", args: []string{"--release-id", "bench-1", "--topology", "-"}},
		{name: "unknown node field", stdin: `[{"unique_id":"a","changed":false}]`, args: []string{"--release-id", "bench-1", "--topology", "-"}},
		{name: "no nodes", stdin: `[]`, args: []string{"--release-id", "bench-1", "--topology", "-"}},
		{name: "a node without unique_id", stdin: `[{"service_name":"bench"}]`, args: []string{"--release-id", "bench-1", "--topology", "-"}},
		{name: "a duplicate unique_id", stdin: `[{"unique_id":"a"},{"unique_id":"a"}]`, args: []string{"--release-id", "bench-1", "--topology", "-"}},
		{name: "an extra argument", args: []string{"--reannounce-current", "extra"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runWith(t, &fakeAnnouncer{}, tc.stdin, tc.args...)
			assert.Equal(t, exitUsage, code)
			assert.Empty(t, stdout, "stdout carries only the result")
			assert.NotEmpty(t, stderr)
		})
	}
}

func TestRun_ErrorExitCodes(t *testing.T) {
	code, _, _ := runWith(t, &fakeAnnouncer{err: handlers.ErrNoCurrentProd}, "", "--reannounce-current")
	assert.Equal(t, exitNoCurrentProd, code)

	code, _, _ = runWith(t, &fakeAnnouncer{err: handlers.ErrNoCurrentProd}, "", "--print-current")
	assert.Equal(t, exitNoCurrentProd, code)

	code, _, _ = runWith(t, &fakeAnnouncer{err: handlers.ErrReleaseIDTaken}, twoNodes, "--release-id", "rA", "--topology", "-")
	assert.Equal(t, exitUsage, code)

	code, stdout, _ := runWith(t, &fakeAnnouncer{err: errors.New("connection refused")}, twoNodes, "--release-id", "bench-1", "--topology", "-")
	assert.Equal(t, exitError, code)
	assert.Empty(t, stdout)
}

func TestRun_PrintsTheCurrentTopology(t *testing.T) {
	a := &fakeAnnouncer{current: release.Topology{
		{UniqueID: "a", ServiceName: "svc", NodeType: "dbt-model", ContentHash: "h", UpstreamUniqueIDs: []string{}},
	}}
	code, stdout, _ := runWith(t, a, "", "--print-current")
	require.Equal(t, exitOK, code)
	var nodes []topologyartifact.Node
	require.NoError(t, json.Unmarshal([]byte(stdout), &nodes))
	require.Len(t, nodes, 1)
	assert.Equal(t, "a", nodes[0].UniqueID)
	assert.Equal(t, "h", nodes[0].ContentHash)
}

// What --print-current writes, --topology reads back unchanged: the benchmark
// builds its union from one and announces it with the other.
func TestTopologyRoundTrip(t *testing.T) {
	topo := release.Topology{{ //nolint:gosec // G101: SecretRef names a Kubernetes Secret; it is not a credential
		UniqueID: "a", SchemaName: "s", TableName: "t", ResolvedRelationID: "s.t", ServiceName: "svc",
		NodeType: "python-api", ContentHash: "h", TestCount: 2, ImageTag: "img",
		UpstreamUniqueIDs: []string{"b"}, Schedule: "daily", OriginalFilePath: "p.py", SecretRef: "continuo-api-x",
	}}
	raw, err := json.Marshal(fromTopology(topo))
	require.NoError(t, err)
	got, err := readTopology(bytes.NewReader(raw))
	require.NoError(t, err)
	assert.Equal(t, topo, got)
}

// A topology of only dbt-test nodes is refused: orchestrator strips every
// dbt-test when it applies the promotion, so announcing one would leave an
// empty swap set that retires every live node. One non-test node is enough.
func TestReadTopology_RefusesAnAllDbtTestTopology(t *testing.T) {
	onlyTests := `[{"unique_id":"test.p.not_null_a.1","service_name":"svc","node_type":"dbt-test","upstream_unique_ids":["a"]}]`
	_, err := readTopology(strings.NewReader(onlyTests))
	require.Error(t, err)

	withModel := `[{"unique_id":"a","service_name":"svc","node_type":"dbt-model","upstream_unique_ids":[]},
	 {"unique_id":"test.p.not_null_a.1","service_name":"svc","node_type":"dbt-test","upstream_unique_ids":["a"]}]`
	topo, err := readTopology(strings.NewReader(withModel))
	require.NoError(t, err)
	require.Len(t, topo, 2)
}

func TestLoadConfig_NamesEveryMissingKey(t *testing.T) {
	for _, key := range []string{"POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION"} {
		t.Setenv(key, "")
	}
	_, _, err := loadConfig()
	require.Error(t, err)
	for _, key := range []string{"POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION"} {
		assert.Contains(t, err.Error(), key)
	}
}
