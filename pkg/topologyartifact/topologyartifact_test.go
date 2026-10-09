package topologyartifact_test

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
)

type topologyFixture struct {
	Input     topologyartifact.Document `json:"input"`
	Canonical string                    `json:"canonical"`
}

func loadTopologyFixture(t *testing.T) topologyFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/topology_v1.json")
	require.NoError(t, err)
	var fx topologyFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	require.NotEmpty(t, fx.Canonical)
	return fx
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func gz(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(raw)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestCanonicalJSON_MatchesGoldenFixture(t *testing.T) {
	fx := loadTopologyFixture(t)
	got, err := topologyartifact.CanonicalJSON(fx.Input)
	require.NoError(t, err)
	assert.Equal(t, fx.Canonical, string(got))
}

func TestCanonicalJSON_DoesNotDependOnInputOrder(t *testing.T) {
	fx := loadTopologyFixture(t)
	reordered := fx.Input
	reordered.Nodes = slices.Clone(fx.Input.Nodes)
	slices.Reverse(reordered.Nodes)
	for i := range reordered.Nodes {
		ups := slices.Clone(reordered.Nodes[i].UpstreamUniqueIDs)
		slices.Reverse(ups)
		reordered.Nodes[i].UpstreamUniqueIDs = ups
	}
	got, err := topologyartifact.CanonicalJSON(reordered)
	require.NoError(t, err)
	assert.Equal(t, fx.Canonical, string(got))
}

func TestCanonicalJSON_WritesTheBuildsSchemaVersion(t *testing.T) {
	fx := loadTopologyFixture(t)
	fx.Input.SchemaVersion = 0
	got, err := topologyartifact.CanonicalJSON(fx.Input)
	require.NoError(t, err)
	assert.Equal(t, fx.Canonical, string(got))
}

func TestEncode_IsDeterministicAndDecodesBack(t *testing.T) {
	fx := loadTopologyFixture(t)
	first, firstSum, err := topologyartifact.Encode(fx.Input)
	require.NoError(t, err)
	second, secondSum, err := topologyartifact.Encode(fx.Input)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, firstSum, secondSum)
	assert.Equal(t, sha(first), firstSum)
	assert.Equal(t, []byte{0, 0, 0, 0}, first[4:8], "the gzip header carries no modification time")

	doc, err := topologyartifact.Decode(first, firstSum)
	require.NoError(t, err)
	again, err := topologyartifact.CanonicalJSON(doc)
	require.NoError(t, err)
	assert.Equal(t, fx.Canonical, string(again))
}

func TestDecode_AcceptsAnUppercaseChecksum(t *testing.T) {
	fx := loadTopologyFixture(t)
	obj, sum, err := topologyartifact.Encode(fx.Input)
	require.NoError(t, err)
	upper := []byte(sum)
	for i, c := range upper {
		if c >= 'a' && c <= 'f' {
			upper[i] = c - 'a' + 'A'
		}
	}
	_, err = topologyartifact.Decode(obj, string(upper))
	assert.NoError(t, err)
}

func TestDecode_RejectsAChecksumMismatch(t *testing.T) {
	fx := loadTopologyFixture(t)
	obj, _, err := topologyartifact.Encode(fx.Input)
	require.NoError(t, err)
	_, err = topologyartifact.Decode(obj, sha([]byte("another object")))
	assert.ErrorIs(t, err, topologyartifact.ErrChecksumMismatch)
	assert.NotErrorIs(t, err, topologyartifact.ErrMalformed)
}

func TestDecode_RejectsMalformedObjects(t *testing.T) {
	notGzip := []byte("plain text, not gzip")
	notJSON := gz(t, []byte("not json"))
	wrongVersion := gz(t, []byte(`{"schema_version":2,"tenant_id":"default","release_id":"r","nodes":[]}`))
	noRelease := gz(t, []byte(`{"schema_version":1,"tenant_id":"default","release_id":"","nodes":[]}`))
	noUniqueID := gz(t, []byte(`{"schema_version":1,"tenant_id":"default","release_id":"r","nodes":[{"unique_id":""}]}`))
	oversized := make([]byte, topologyartifact.MaxObjectBytes+1)

	for name, obj := range map[string][]byte{
		"not gzip":         notGzip,
		"not json":         notJSON,
		"schema_version 2": wrongVersion,
		"empty release_id": noRelease,
		"node without id":  noUniqueID,
		"oversized object": oversized,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := topologyartifact.Decode(obj, sha(obj))
			assert.ErrorIs(t, err, topologyartifact.ErrMalformed)
		})
	}
}

// Two nodes sharing a unique_id are a release-level collision that the domain
// duplicate-claims gate reports to the operator; the codec keeps both nodes in
// input order and leaves the verdict to that gate.
func TestDecode_KeepsNodesThatShareAUniqueID(t *testing.T) {
	obj := gz(t, []byte(`{"schema_version":1,"tenant_id":"default","release_id":"r","nodes":[{"unique_id":"a.b","service_name":"one"},{"unique_id":"a.b","service_name":"two"}]}`))
	doc, err := topologyartifact.Decode(obj, sha(obj))
	require.NoError(t, err)
	require.Len(t, doc.Nodes, 2)
	assert.Equal(t, "one", doc.Nodes[0].ServiceName)
	assert.Equal(t, "two", doc.Nodes[1].ServiceName)
}

// The input holds far more nodes than the insertion-sort threshold of an
// unstable sort (12), with a few unique ids repeated and interleaved, so only a
// stable sort keeps the input order inside each group of equal ids.
func TestCanonicalJSON_KeepsInputOrderForEqualUniqueIDs(t *testing.T) {
	ids := []string{"c.c", "a.a", "d.d", "b.b"}
	const total = 40
	d := topologyartifact.Document{TenantID: "default", ReleaseID: "r"}
	wantByID := map[string][]string{}
	for i := 0; i < total; i++ {
		id := ids[(i*7+i/3)%len(ids)]
		svc := fmt.Sprintf("svc-%02d", i)
		d.Nodes = append(d.Nodes, topologyartifact.Node{UniqueID: id, ServiceName: svc})
		wantByID[id] = append(wantByID[id], svc)
	}
	var want []string
	for _, id := range []string{"a.a", "b.b", "c.c", "d.d"} {
		require.Greater(t, len(wantByID[id]), 1, "the input must repeat %s", id)
		want = append(want, wantByID[id]...)
	}
	require.Len(t, want, total)

	got, err := topologyartifact.CanonicalJSON(d)
	require.NoError(t, err)
	var parsed topologyartifact.Document
	require.NoError(t, json.Unmarshal(got, &parsed))
	require.Len(t, parsed.Nodes, total)
	gotServices := make([]string, 0, total)
	for _, n := range parsed.Nodes {
		gotServices = append(gotServices, n.ServiceName)
	}
	assert.Equal(t, want, gotServices)
}

func TestKey(t *testing.T) {
	assert.Equal(t, "tenants/default/topologies/rel-1/topology.json.gz", topologyartifact.Key("default", "rel-1"))
}

type candidateKeyCase struct {
	ReleaseID string `json:"release_id"`
	UniqueID  string `json:"unique_id"`
	NodeType  string `json:"node_type"`
	Key       string `json:"key"`
}

func loadCandidateKeyCases(t *testing.T) []candidateKeyCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/candidate_object_keys_v1.json")
	require.NoError(t, err)
	var cases []candidateKeyCase
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.NotEmpty(t, cases)
	return cases
}

func TestCandidateObjectKey_MatchesGoldenFixture(t *testing.T) {
	for _, c := range loadCandidateKeyCases(t) {
		assert.Equal(t, c.Key, topologyartifact.CandidateObjectKey(c.ReleaseID, c.UniqueID, c.NodeType), "%s %s", c.NodeType, c.UniqueID)
	}
}

// A node type added to the contract must be added to the fixture too, so the
// Python writer of the candidate objects is pinned to the same answer.
func TestCandidateObjectKey_FixtureCoversEveryNodeType(t *testing.T) {
	covered := map[string]bool{}
	for _, c := range loadCandidateKeyCases(t) {
		covered[c.NodeType] = true
	}
	for _, nt := range model.NodeTypes() {
		assert.True(t, covered[string(nt)], "candidate_object_keys_v1.json has no case for %s", nt)
	}
}
