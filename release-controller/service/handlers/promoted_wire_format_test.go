package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleValidationResult_Promote_TopologyWireFormat pins the exact JSON of
// the per-node topology in release.promoted:v1, byte for byte. The published
// shape is a cross-service contract: orchestrator projects these keys, so a
// renamed, reordered, added or dropped field is a breaking change that must
// fail here rather than in a consumer.
//
// Two nodes cover both runtime-manifest cases:
//   - public.a pins a complete reference and emits all four runtime fields.
//   - public.legacy has no reference — as nodes from releases made before
//     runtime manifests do — and must emit none of them.
func TestHandleValidationResult_Promote_TopologyWireFormat(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "sha-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: "rA", Status: "ok",
	}))
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA", Status: "ok",
		Topology: release.Topology{
			{UniqueID: "public.a", DBTUniqueID: "model.svc_a.a", SchemaName: "public", TableName: "a",
				ServiceName: "svc-a", NodeType: "model", ContentHash: "h-a", TestCount: 2,
				UpstreamUniqueIDs: []string{}, Schedule: "@daily", OriginalFilePath: "models/a.sql"},
			{UniqueID: "public.legacy", DBTUniqueID: "model.svc_legacy.legacy", SchemaName: "public",
				TableName: "legacy", ServiceName: "svc-legacy", NodeType: "model", ContentHash: "h-l",
				UpstreamUniqueIDs: []string{}, OriginalFilePath: "models/legacy.sql"},
		},
		RuntimeManifests: map[string]pkgmodel.RuntimeManifestRef{"svc-a": runtimeRef("a1")},
	}))
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "public.a", Status: "ok"},
			{NodeID: "public.legacy", Status: "ok"},
		},
		AggregateStatus: "ok",
	}))

	entries := outboxEntries(store)
	last := entries[len(entries)-1]

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(last.Payload, &payload))

	const wantTopology = `[` +
		`{"unique_id":"public.a","dbt_unique_id":"model.svc_a.a","schema_name":"public","table_name":"a",` +
		`"service_name":"svc-a","node_type":"model","content_hash":"h-a","test_count":2,"image_tag":"sha-a",` +
		`"upstream_unique_ids":[],"schedule":"@daily","changed":true,"original_file_path":"models/a.sql",` +
		`"runtime_manifest_uri":"s3://continuo/a1/manifest.msgpack",` +
		`"runtime_manifest_sha256":"a100000000000000000000000000000000000000000000000000000000000000",` +
		`"runtime_manifest_dbt_version":"1.12.0b1",` +
		`"runtime_manifest_parse_context_sha256":"a111111111111111111111111111111111111111111111111111111111111111"},` +
		`{"unique_id":"public.legacy","dbt_unique_id":"model.svc_legacy.legacy","schema_name":"public",` +
		`"table_name":"legacy","service_name":"svc-legacy","node_type":"model","content_hash":"h-l",` +
		`"test_count":0,"image_tag":"","upstream_unique_ids":[],"schedule":"","changed":true,` +
		`"original_file_path":"models/legacy.sql"}` +
		`]`

	assert.Equal(t, wantTopology, string(payload["topology"]),
		"the promoted topology's wire format must not drift: it is consumed by orchestrator")

	// Stated separately from the byte comparison above so the intent survives any
	// future edit to the expected string: a node with no runtime manifest emits
	// no runtime keys at all, rather than empty ones.
	assert.NotContains(t, string(payload["topology"]), `"runtime_manifest_uri":""`,
		"a node without a runtime manifest must omit the runtime fields entirely")
}
