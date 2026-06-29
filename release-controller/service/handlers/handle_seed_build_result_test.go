package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
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
			UpstreamUniqueIDs: []string{"seed.core.fx"},
			CandidateSQLURI:   "s3://continuo/svc-fin/rel-seed-ok/candidate_report.sql"},
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
		Service:           "svc-fin",
		ReleaseID:         releaseID,
		ImageTag:          "sha-fin",
		Repo:              "acme/demo",
		CommitSHA:         "cafebabe",
		CompileInContinuo: true,
	}))
	require.NoError(t, handlers.AdvanceQueue(ctx(t), deps))

	// Simulate the compile leg completing successfully (Compiling → Parsing).
	require.NoError(t, handlers.HandleCompileResult(ctx(t), deps, handlers.HandleCompileResultInput{
		ReleaseID: releaseID,
		Status:    "ok",
	}))

	require.NoError(t, handlers.HandleParsedManifest(ctx(t), deps, handlers.HandleParsedManifestInput{
		ReleaseID: releaseID,
		Status:    "ok",
		Topology:  topo,
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
