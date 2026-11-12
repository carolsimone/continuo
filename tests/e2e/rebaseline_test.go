package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// rebaselineService is the dbt service the rebaseline release re-baselines.
	// It is the only e2e service with seeds.
	rebaselineService = "service-1"
	// rebaselineRefusal is release-controller's answer to a rebaseline release
	// that is not a bootstrap one, which the public API passes on in its 400.
	rebaselineRefusal = "rebaseline is accepted only together with bootstrap"
)

// TestE2E_RebaselineRelease_AdoptsSeedsAndValidatesNothing proves that a
// release posted with "bootstrap": true and "rebaseline": true re-baselines
// production without validating or loading anything:
//
//  1. production holds every baseline node and every service's baseline
//     pointer, and a release of service-1 with its current image is posted
//     through the public API with both flags;
//  2. it promotes, and no validation Job is ever created for it;
//  3. run-controller applies it with no promote-seed run, and records each of
//     service-1's seeds as adopted (node_materializations source 'adopted',
//     release_id the release) at the content_hash and seed_columns_hash the
//     release stores for it, a non-empty seed_columns_hash since the compile
//     wrote the release's compile extras;
//  4. every :NodeVersion catalog records for the release is healed;
//  5. GET /api/v1/releases/<id> answers "rebaseline": true.
//
// The release moves the live topology, so a deferred restore announces the
// default e2e DAG again. It plans no promote-seed run, so the restore's wait for
// them returns at once.
func TestE2E_RebaselineRelease_AdoptsSeedsAndValidatesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: the wait for earlier promote-seed runs (5m) + the
	// promotion (10m) + run-controller applying it (2m) + catalog's topology
	// swap (2m) and versions (2m) + the no-validation grace window (20s). The
	// restore has a context of its own.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rebaseline-" + uuid.NewString()[:8]
	t.Logf("release_id=%s service=%s", releaseID, rebaselineService)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	imageTag := allServices[rebaselineService].imageTag
	require.NotEmpty(t, imageTag, "image_tag missing for %s", rebaselineService)
	var prodNodes []map[string]string
	for svc, si := range allServices {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "service": svc, "content_hash": n.contentHash,
			})
		}
	}

	// ── 1. production holds the baseline; the rebaseline release is posted ──
	// A promote-seed run an earlier test's release planned would record its
	// seeds as built after this release adopts them.
	waitForPromoteSeedRunsToEnd(t, ctx, clients, "", 5*time.Minute)
	defer restoreDefaultTopologyAfter(t, clients, releaseID)
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allServices)

	token := mintCIToken(t, e2eDbtRepositoryID, "carolsimone/continuo-demo", imageTag)
	status, body := submitPublicRelease(t, clients, token, map[string]any{
		"service":    rebaselineService,
		"release_id": releaseID,
		"image_tag":  imageTag,
		"bootstrap":  true,
		"rebaseline": true,
	})
	require.Equal(t, http.StatusAccepted, status, "POST /api/v1/releases: expected 202, got %d: %v", status, body)

	// ── 2. it promotes and validates nothing ─────────────────────────────────
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)
	assertNoValidationJobsDispatched(t, ctx, clients, releaseID)

	// ── 3. run-controller adopts every seed and loads none ───────────────────
	// The adoption commits in the transaction that moves the live release, so
	// it is there once the live release is this one.
	waitForRunControllerToApply(t, ctx, clients, releaseID, "", 2*time.Minute)
	var promoteSeedRuns int
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &promoteSeedRuns, `
		SELECT count(*) FROM runs WHERE tenant_id = $1 AND kind = 'promote_seed' AND release_id = $2`,
		events.DefaultTenantID, releaseID))
	assert.Zero(t, promoteSeedRuns, "a rebaseline release loads no seed: it adopts every one")
	assertSeedsAdopted(t, ctx, clients, releaseID)

	// ── 4. catalog marks the release's versions healed ───────────────────────
	waitForTopologySwap(t, ctx, clients, releaseID, probeUniqueID, 2*time.Minute)
	promoted := readLivePointer(ctx, clients)
	require.Equal(t, releaseID, promoted.releaseID, "catalog's live release must be the rebaseline release")
	waitForVersionsOfPromotion(t, ctx, clients, probeUniqueID, promoted.promotionSeq, 2*time.Minute)
	// catalog writes a version once per (unique_id, content_hash), so a stack
	// where an earlier service-1 release already wrote these hashes records none
	// for this release; what it records must be healed.
	// TestPlanVersionWrite_HealedProvenance pins the rule itself.
	versions := queryNeo4jRows(t, ctx, clients,
		`MATCH (v:NodeVersion {tenant_id: 'default', release_id: $rid})
		 RETURN v.unique_id AS uid, coalesce(v.healed, false) AS healed`,
		map[string]any{"rid": releaseID})
	for _, v := range versions {
		assert.Equal(t, true, v["healed"], "the version of %v that rebaseline release %s recorded is healed", v["uid"], releaseID)
	}
	t.Logf("✅ catalog recorded %d version(s) for %s, every one healed", len(versions), releaseID)

	// ── 5. the public API reports the flag ───────────────────────────────────
	read := getPublicRelease(t, clients, token, releaseID)
	assert.Equal(t, true, read["rebaseline"], "GET /api/v1/releases/%s reports the rebaseline flag: %v", releaseID, read)
	assert.Equal(t, true, read["bootstrap"], "GET /api/v1/releases/%s reports the bootstrap flag: %v", releaseID, read)
	t.Logf("✅ rebaseline release %s promoted without validation, adopted service-1's seeds and planned no seed load", releaseID)
}

// TestE2E_RebaselineWithoutBootstrap_Is400 proves the public API refuses a
// release that asks to re-baseline without bootstrapping, and records no
// release for it.
func TestE2E_RebaselineWithoutBootstrap_Is400(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)

	releaseID := "e2e-rebaseline-nb-" + uuid.NewString()[:8]
	token := mintCIToken(t, e2eDbtRepositoryID, "carolsimone/continuo-demo", "abc")
	status, body := submitPublicRelease(t, clients, token, map[string]any{
		"service":    rebaselineService,
		"release_id": releaseID,
		"image_tag":  "unused",
		"rebaseline": true,
	})
	require.Equal(t, http.StatusBadRequest, status, "rebaseline without bootstrap must be 400: %v", body)
	assert.Equal(t, "bad_request", body["code"])
	errText, _ := body["error"].(string)
	assert.Contains(t, errText, rebaselineRefusal, "the 400 says why")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "GET release-controller /releases/%s", releaseID)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "a refused release is never recorded")
	t.Logf("✅ rebaseline without bootstrap answered 400 (%s) and recorded nothing", errText)
}

// releaseSeed is one seed of a release in run-controller's copy, with the
// version node_materializations records for it.
type releaseSeed struct {
	NodeKey             string `db:"node_key"`
	ReleaseHash         string `db:"release_hash"`
	ReleaseColumnsHash  string `db:"release_columns_hash"`
	BuiltHash           string `db:"built_hash"`
	BuiltColumnsHash    string `db:"built_columns_hash"`
	BuiltSource         string `db:"built_source"`
	BuiltReleaseID      string `db:"built_release_id"`
	MaterializationSeen bool   `db:"materialization_seen"`
}

// assertSeedsAdopted asserts that each of rebaselineService's seeds in
// releaseID is recorded as adopted by that release, at the content_hash and
// seed_columns_hash the release stores for it.
func assertSeedsAdopted(t *testing.T, ctx context.Context, clients *testClients, releaseID string) {
	t.Helper()
	var seeds []releaseSeed
	require.NoError(t, clients.runControllerDB.SelectContext(ctx, &seeds, `
		SELECT n.node_key,
		       n.content_hash                  AS release_hash,
		       n.seed_columns_hash             AS release_columns_hash,
		       coalesce(m.content_hash, '')      AS built_hash,
		       coalesce(m.seed_columns_hash, '') AS built_columns_hash,
		       coalesce(m.source, '')            AS built_source,
		       coalesce(m.release_id, '')        AS built_release_id,
		       m.node_key IS NOT NULL            AS materialization_seen
		  FROM topology_nodes n
		  LEFT JOIN node_materializations m ON m.tenant_id = n.tenant_id AND m.node_key = n.node_key
		 WHERE n.tenant_id = $1 AND n.release_id = $2 AND n.node_type = $3 AND n.service_name = $4
		 ORDER BY n.node_key`,
		events.DefaultTenantID, releaseID, string(model.NodeTypeDbtSeed), rebaselineService))
	keys := make([]string, 0, len(seeds))
	for _, s := range seeds {
		keys = append(keys, s.NodeKey)
	}
	require.ElementsMatch(t,
		[]string{seedSchemaName + ".seed_table_1", seedSchemaName + ".seed_table_2", seedSchemaName + ".seed_table_3"},
		keys, "run-controller's copy of %s holds %s's three seeds", releaseID, rebaselineService)
	for _, s := range seeds {
		require.True(t, s.MaterializationSeen, "seed %s has no node_materializations row", s.NodeKey)
		assert.Equal(t, "adopted", s.BuiltSource, "seed %s is recorded as adopted, not built", s.NodeKey)
		assert.Equal(t, releaseID, s.BuiltReleaseID, "seed %s is adopted by the rebaseline release", s.NodeKey)
		assert.True(t, strings.HasPrefix(s.ReleaseHash, "sha256:"), "seed %s has a content_hash in the release: %q", s.NodeKey, s.ReleaseHash)
		assert.Equal(t, s.ReleaseHash, s.BuiltHash, "seed %s is adopted at the release's content_hash", s.NodeKey)
		assert.NotEmpty(t, s.ReleaseColumnsHash,
			"seed %s has a seed_columns_hash in the release: the compile wrote the release's compile extras", s.NodeKey)
		assert.Equal(t, s.ReleaseColumnsHash, s.BuiltColumnsHash, "seed %s is adopted at the release's seed_columns_hash", s.NodeKey)
	}
	t.Logf("✅ %s's seeds %v are adopted by %s at the release's hashes", rebaselineService, keys, releaseID)
}

// getPublicRelease reads a release through the public API with a bearer token
// and returns its decoded JSON body.
func getPublicRelease(t *testing.T, clients *testClients, token, releaseID string) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, clients.uiBase+"/api/v1/releases/"+releaseID, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "GET /api/v1/releases/%s", releaseID)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET /api/v1/releases/%s", releaseID)
	out := map[string]any{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out), "decode GET /api/v1/releases/%s", releaseID)
	return out
}
