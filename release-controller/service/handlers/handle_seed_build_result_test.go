package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers for seed-build result tests ---

// newTestDeps returns a Deps and fakeStore wired for seed-build result tests.
func newTestDeps(t *testing.T) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "continuo"
	return deps, store
}

// ctx returns a background context for test calls.
func ctx(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

// topoSeedPlusModel returns a topology with one new dbt-seed and one model that
// depends on it. Both are treated as new (no prod snapshot → both are in the
// changed-closure).
func topoSeedPlusModel() release.Topology {
	return release.Topology{
		{UniqueID: "seed.core.fx", ServiceName: "svc-fin", NodeType: "dbt-seed",
			SchemaName: "schema_fin", TableName: "fx"},
		{UniqueID: "model.fin.report", ServiceName: "svc-fin", NodeType: "dbt-model",
			SchemaName: "schema_fin", TableName: "report",
			UpstreamUniqueIDs:    []string{"seed.core.fx"},
			CandidateArtifactURI: "s3://continuo/svc-fin/rel-seed-ok/candidate_report.sql"},
	}
}

// topoSeedOnly returns a topology with a single new dbt-seed and no downstream
// models — used to exercise the empty-after-exclusion promote edge case.
func topoSeedOnly() release.Topology {
	return release.Topology{
		{UniqueID: "seed.core.fx", ServiceName: "svc-fin", NodeType: "dbt-seed",
			SchemaName: "schema_fin", TableName: "fx"},
	}
}

// putSeedBuildingRelease advances a release through ReceiveCandidate →
// AdvanceQueue → HandleCompileResult(ok) → HandleParsedManifest (which must
// emit seed.build.requested, transitioning the release to SeedBuilding) and
// stores it in the fakeStore. The topology must contain at least one dbt-seed
// (new, no prod snapshot) so HandleParsedManifest routes to the seed-build leg.
func putSeedBuildingRelease(t *testing.T, store *fakeStore, deps *handlers.Deps, releaseID string, topo release.Topology) {
	t.Helper()

	require.NoError(t, handlers.ReceiveCandidate(ctx(t), deps, handlers.ReceiveCandidateInput{
		Service:   "svc-fin",
		ReleaseID: releaseID,
		ImageTag:  "sha-fin",
		Repo:      "acme/demo",
		CommitSHA: "cafebabe",
	}))
	require.NoError(t, handlers.AdvanceQueue(ctx(t), deps))

	// Simulate the compile leg completing successfully (Compiling → Parsing).
	require.NoError(t, handlers.HandleCompileResult(ctx(t), deps, handlers.HandleCompileResultInput{
		ReleaseID: releaseID,
		Status:    "ok",
	}))

	require.NoError(t, handlers.HandleParsedManifest(ctx(t), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     releaseID,
		Status:        "ok",
		CodeBundleURI: "s3://continuo/code-bundles/" + releaseID + "/bundle.json",
		Topology:      topo,
	}))

	r, err := store.GetRelease(releaseID)
	require.NoError(t, err)
	require.Equal(t, release.StatusSeedBuilding, r.Status(),
		"topology must have a new dbt-seed for putSeedBuildingRelease to leave release in SeedBuilding")
}

// mustGetRelease fetches a release from the fakeStore or fails the test.
func mustGetRelease(t *testing.T, store *fakeStore, releaseID string) *release.Release {
	t.Helper()
	r, err := store.GetRelease(releaseID)
	require.NoError(t, err)
	require.NotNil(t, r)
	return r
}

// lastOutbox returns the most-recently written outbox entry, failing if empty.
func lastOutbox(t *testing.T, store *fakeStore) *pkgoutbox.Entry {
	t.Helper()
	entries := store.OutboxEntries()
	require.NotEmpty(t, entries, "outbox must not be empty")
	return entries[len(entries)-1]
}

// outboxCount returns the number of outbox entries written so far.
func outboxCount(t *testing.T, store *fakeStore) int {
	t.Helper()
	return len(store.OutboxEntries())
}

// decodeValidationNodes decodes the "nodes" array from a
// validation.requested:v1 outbox payload entry directly from the entry bytes.
func decodeValidationNodes(t *testing.T, entry *pkgoutbox.Entry) []map[string]any {
	t.Helper()
	var payload struct {
		Nodes []map[string]any `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	return payload.Nodes
}

// nodeUniqueIDs extracts unique_id values from a decoded nodes slice.
func nodeUniqueIDs(nodes []map[string]any) []string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if id, ok := n["unique_id"].(string); ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// --- tests ---

func TestHandleSeedBuildResult_OKEmitsValidationRequestedExcludingBuiltSeeds(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-ok"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedPlusModel())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

	r := mustGetRelease(t, store, releaseID)
	assert.Equal(t, release.StatusValidating, r.Status())

	entry := lastOutbox(t, store)
	assert.Equal(t, streams.ValidationRequestedV1, entry.StreamName)
	nodes := decodeValidationNodes(t, entry)
	ids := nodeUniqueIDs(nodes)
	assert.Contains(t, ids, "model.fin.report", "model still validated")
	assert.NotContains(t, ids, "seed.core.fx", "built seed excluded from validation set")
}

func TestHandleSeedBuildResult_OKValidationPayloadCarriesExpectedFields(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-fields"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedPlusModel())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

	entry := lastOutbox(t, store)
	assert.Equal(t, streams.ValidationRequestedV1, entry.StreamName)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, releaseID, payload["release_id"])
	assert.Equal(t, "validation", payload["mode"])

	// node_ids_in_order must not contain the seed
	rawIDs, ok := payload["node_ids_in_order"].([]any)
	require.True(t, ok, "node_ids_in_order must be an array")
	var idsInOrder []string
	for _, v := range rawIDs {
		if s, ok := v.(string); ok {
			idsInOrder = append(idsInOrder, s)
		}
	}
	assert.Contains(t, idsInOrder, "model.fin.report")
	assert.NotContains(t, idsInOrder, "seed.core.fx")
}

func TestHandleSeedBuildResult_FailedRejects(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-fail"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedPlusModel())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID, Status: "failed", ErrorClass: "seed_error", ErrorDetail: "csv parse",
	}))

	r := mustGetRelease(t, store, releaseID)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "seed_build_failed", r.RejectReason())
	assert.Equal(t, streams.ReleaseRejectedV1, lastOutbox(t, store).StreamName)
}

func TestHandleSeedBuildResult_FailedPayloadCarriesCandidateSchema(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-fail-schema"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedPlusModel())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID, Status: "failed", ErrorClass: "seed_error", ErrorDetail: "csv parse",
	}))

	entry := lastOutbox(t, store)
	assert.Equal(t, streams.ReleaseRejectedV1, entry.StreamName)

	// Use map[string]any so the uniform payload (which includes array fields
	// like per_node and failing_nodes) unmarshals without error.
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, "_candidate_rel_seed_fail_schema", payload["candidate_schema"],
		"release.rejected:v1 must carry candidate_schema so executor can tear down the schema")
}

// topoTwoSeeds returns a topology with two dbt-seeds so we can test the
// per_node seed-build failure path with multiple entries.
func topoTwoSeeds() release.Topology {
	return release.Topology{
		{UniqueID: "analytics.seed_fx_rates_eur", ServiceName: "svc-fin", NodeType: "dbt-seed",
			SchemaName: "schema_fin", TableName: "seed_fx_rates_eur"},
		{UniqueID: "analytics.seed_equities", ServiceName: "svc-fin", NodeType: "dbt-seed",
			SchemaName: "schema_fin", TableName: "seed_equities"},
	}
}

// TestHandleSeedBuildResult_Failed_EmitsUniformRejected verifies that a
// seed-build failure emits a release.rejected:v1 outbox payload with the
// canonical uniform shape: stage="seed_build", reason="seed_build_failed",
// repo, commit_sha, code_bundle_uri, failing_nodes, per_node with dbt_log_uri
// per entry, and candidate_schema. It also verifies that the release
// aggregate records a seed_build-stage PerNodeResult with Stage=="seed_build".
func TestHandleSeedBuildResult_Failed_EmitsUniformRejected(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-uniform"
	putSeedBuildingRelease(t, store, deps, releaseID, topoTwoSeeds())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID,
		Status:    "failed",
		PerNode: []handlers.NodeResult{
			{NodeID: "analytics.seed_fx_rates_eur", Status: "failed", DBTLogURI: "s3://logs/seed_eur.log"},
			{NodeID: "analytics.seed_equities", Status: "ok", DBTLogURI: "s3://logs/seed_eq.log"},
		},
		ErrorClass:  "seed_error",
		ErrorDetail: "csv parse failure",
	}))

	r := mustGetRelease(t, store, releaseID)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "seed_build_failed", r.RejectReason())

	// The aggregate must record per-node seed-build stage results.
	require.NotEmpty(t, r.PerNodeResults())
	assert.Equal(t, "seed_build", r.PerNodeResults()[0].Stage)

	// The outbox payload must match the canonical uniform rejected shape.
	e := lastOutbox(t, store)
	assert.Equal(t, streams.ReleaseRejectedV1, e.StreamName)

	var topLevel map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(e.Payload, &topLevel))

	var stage string
	require.NoError(t, json.Unmarshal(topLevel["stage"], &stage))
	assert.Equal(t, "seed_build", stage)

	var reason string
	require.NoError(t, json.Unmarshal(topLevel["reason"], &reason))
	assert.Equal(t, "seed_build_failed", reason)

	var repo string
	require.NoError(t, json.Unmarshal(topLevel["repo"], &repo))
	assert.Equal(t, "acme/demo", repo)

	var commitSHA string
	require.NoError(t, json.Unmarshal(topLevel["commit_sha"], &commitSHA))
	assert.Equal(t, "cafebabe", commitSHA)

	var codeBundleURI string
	require.NoError(t, json.Unmarshal(topLevel["code_bundle_uri"], &codeBundleURI))
	assert.Equal(t, "s3://continuo/code-bundles/rel-seed-uniform/bundle.json", codeBundleURI,
		"code_bundle_uri must come from the release aggregate, set at parse time")

	var failingNodes []string
	require.NoError(t, json.Unmarshal(topLevel["failing_nodes"], &failingNodes))
	assert.Equal(t, []string{"analytics.seed_fx_rates_eur"}, failingNodes)

	var perNode []struct {
		NodeID    string `json:"node_id"`
		Status    string `json:"status"`
		DBTLogURI string `json:"dbt_log_uri"`
	}
	require.NoError(t, json.Unmarshal(topLevel["per_node"], &perNode))
	require.Len(t, perNode, 2)
	// Collect statuses from all per_node entries to verify both are present.
	statuses := make([]string, 0, len(perNode))
	for _, n := range perNode {
		statuses = append(statuses, n.Status)
		assert.NotEmpty(t, n.DBTLogURI, "each per_node entry must carry its dbt_log_uri")
	}
	assert.Contains(t, statuses, "failed")
	assert.Contains(t, statuses, "ok")

	// candidate_schema must be preserved alongside the new uniform fields.
	var candidateSchema string
	require.NoError(t, json.Unmarshal(topLevel["candidate_schema"], &candidateSchema))
	assert.Equal(t, "_candidate_rel_seed_uniform", candidateSchema)
}

// TestHandleSeedBuildResult_OKThenValidationCompletePromotes proves the
// seed-build leg leaves the persisted validation set equal to what the executor
// actually emits per-node events for (the non-seed models). After the seed
// builds ok, only the downstream model is validated, so only that node's per-node
// result lands. The validation terminal (kind=complete) must then PROMOTE, not stall
// by rejecting over the seed, which is not part of the release's validation set.
func TestHandleSeedBuildResult_OKThenValidationCompletePromotes(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-then-validate"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedPlusModel())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

	// The persisted validation set must exclude the built seed so the barrier's
	// expected set matches the executor's per-node emissions.
	r := mustGetRelease(t, store, releaseID)
	require.Equal(t, release.StatusValidating, r.Status())
	assert.Equal(t, []string{"model.fin.report"}, r.ValidationNodeIDs(),
		"persisted validation set must drop the just-built seed")

	// Executor validates only the non-seed model and emits one per-node result;
	// the seed gets no validation event.
	seedValidationNodes(t, deps, releaseID, []handlers.NodeResult{
		{NodeID: "model.fin.report", Status: "ok"},
	})

	require.NoError(t, handlers.HandleValidationResult(ctx(t), deps, handlers.HandleValidationResultInput{
		ReleaseID:       releaseID,
		AggregateStatus: "ok",
	}))

	r = mustGetRelease(t, store, releaseID)
	assert.Equal(t, release.StatusPromoted, r.Status(),
		"seed-build then per-node validation-ok must promote, not hang on the barrier")
	assert.Equal(t, streams.ReleasePromotedV1, lastOutbox(t, store).StreamName)
}

func TestHandleSeedBuildResult_UnknownReleaseDropped(t *testing.T) {
	deps, store := newTestDeps(t)
	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: "nope", Status: "ok",
	}))
	// no panic, no outbox
	assert.Equal(t, 0, outboxCount(t, store))
}

// TestHandleSeedBuildResult_EmptyAfterExclusionPromotesDirect covers the edge
// case where the ONLY change in the release was a new/changed seed, so after
// excluding the just-built seeds from the validation set, no nodes remain to
// validate. The handler must promote directly (mirror the nothing-to-validate
// path) rather than emit an empty validation.requested.
func TestHandleSeedBuildResult_EmptyAfterExclusionPromotesDirect(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-only"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedOnly())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

	r := mustGetRelease(t, store, releaseID)
	assert.Equal(t, release.StatusPromoted, r.Status(), "seed-only release promotes directly after seed build")

	// No validation.requested must be emitted; last event is release.promoted:v1
	entry := lastOutbox(t, store)
	assert.Equal(t, streams.ReleasePromotedV1, entry.StreamName)

	// Verify current_prod was updated
	cp := store.GetCurrentProd()
	assert.Equal(t, releaseID, cp.ReleaseID())
}

func TestHandleSeedBuildResult_EmptyAfterExclusionPromotePayloadCarriesCandidateSchema(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-only-schema"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedOnly())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

	entry := lastOutbox(t, store)
	assert.Equal(t, streams.ReleasePromotedV1, entry.StreamName)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, "_candidate_rel_seed_only_schema", payload["candidate_schema"],
		"release.promoted:v1 on seed-only path must carry candidate_schema so executor can tear down the schema")
}

// topoSeedsWithFilePath returns a topology with two dbt-seeds that each carry
// an OriginalFilePath and ServiceName, so the rejection payload can include
// the source location for the remediation agent.
func topoSeedsWithFilePath() release.Topology {
	return release.Topology{
		{UniqueID: "analytics.seed_fx_rates_eur", ServiceName: "svc-fin", NodeType: "dbt-seed",
			SchemaName: "schema_fin", TableName: "seed_fx_rates_eur",
			OriginalFilePath: "seeds/seed_fx_rates_eur.csv"},
		{UniqueID: "analytics.seed_equities", ServiceName: "svc-fin", NodeType: "dbt-seed",
			SchemaName: "schema_fin", TableName: "seed_equities",
			OriginalFilePath: "seeds/seed_equities.csv"},
	}
}

// TestHandleSeedBuildResult_Failed_PerNodeCarriesFilePathAndService verifies
// that when the candidate topology carries OriginalFilePath and ServiceName for
// a seed node, the release.rejected:v1 per_node entry includes those values so
// the remediation agent can locate the source file without querying Ancestry
// (which only has promoted topology and cannot find newly-added seeds).
func TestHandleSeedBuildResult_Failed_PerNodeCarriesFilePathAndService(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-fp"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedsWithFilePath())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID,
		Status:    "failed",
		PerNode: []handlers.NodeResult{
			{NodeID: "analytics.seed_fx_rates_eur", Status: "failed", DBTLogURI: "s3://logs/seed_eur.log"},
		},
		ErrorClass:  "seed_error",
		ErrorDetail: "csv parse failure",
	}))

	entry := lastOutbox(t, store)
	assert.Equal(t, streams.ReleaseRejectedV1, entry.StreamName)

	var topLevel map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entry.Payload, &topLevel))

	var perNode []struct {
		NodeID   string `json:"node_id"`
		FilePath string `json:"file_path"`
		Service  string `json:"service"`
	}
	require.NoError(t, json.Unmarshal(topLevel["per_node"], &perNode))
	require.Len(t, perNode, 1)
	assert.Equal(t, "seeds/seed_fx_rates_eur.csv", perNode[0].FilePath,
		"file_path must be taken from the candidate topology's OriginalFilePath")
	assert.Equal(t, "svc-fin", perNode[0].Service,
		"service must be taken from the candidate topology's ServiceName")
}
