package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetReleaseResponse_PerNodeResultsIncludeStage verifies that each entry in
// per_node_results exposes its stage (and file_path when present) so the UI can
// group results into Compilation / Seed / Validation sections.
func TestGetReleaseResponse_PerNodeResultsIncludeStage(t *testing.T) {
	rel := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:     "rSTAGE",
		Status: pipeline.StatusRejected,
		PerNodeResults: []pipeline.NodeValidationResult{
			{Stage: "compile", NodeID: "model.svc.node_a", Status: "failed", FilePath: "models/node_a.sql"},
			{Stage: "validation", NodeID: "model.svc.node_b", Status: "ok"},
		},
	})

	body, err := json.Marshal(getReleaseResponse(rel))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))

	raw, ok := decoded["per_node_results"]
	require.True(t, ok, "per_node_results must be present in response")
	entries, ok := raw.([]any)
	require.True(t, ok, "per_node_results must be a JSON array")
	require.Len(t, entries, 2)

	first := entries[0].(map[string]any)
	assert.Equal(t, "compile", first["stage"], "first entry must carry stage=compile")
	assert.Equal(t, "models/node_a.sql", first["file_path"], "compile entry must carry file_path")

	second := entries[1].(map[string]any)
	assert.Equal(t, "validation", second["stage"], "second entry must carry stage=validation")
	_, hasFilePath := second["file_path"]
	assert.False(t, hasFilePath, "validation entry with no file_path must omit the field (omitempty)")
}

func TestGetReleaseResponse_IncludesProvenance(t *testing.T) {
	rel := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:        "rPROV",
		Status:    pipeline.StatusRejected,
		Repo:      "acme/demo",
		CommitSHA: "deadbeefcafe1234",
	})

	body, err := json.Marshal(getReleaseResponse(rel))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "acme/demo", decoded["repo"])
	assert.Equal(t, "deadbeefcafe1234", decoded["commit_sha"])
	// Guard the full response shape so an accidental field drop in the extracted
	// helper is caught: 11 pre-existing keys plus repo + commit_sha +
	// remediation_round + rebaseline + notices + continuo_contracts +
	// contract_bundle_uri + contract_bundle_sha256. There is no shadow key: a
	// candidate response never carries one, since GET /releases/{id} only ever
	// returns a candidate.
	assert.Len(t, decoded, 20)
	assert.Equal(t, "rPROV", decoded["release_id"])
	_, hasShadow := decoded["shadow"]
	assert.False(t, hasShadow, "a candidate response must not carry a shadow key")
}

// TestHandleGetRelease_VerificationIDAnswers404 verifies that GET
// /releases/{id} treats a verification run's id as unknown: a verification
// is not a release and is read through GET /verification-runs instead.
func TestHandleGetRelease_VerificationIDAnswers404(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	v := pipeline.NewVerification("verify-1", "finance", "tag", "rel-orig", 1, "", release.ManifestKindDbt, now)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["verify-1"] = v

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/releases/verify-1", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetReleaseResponse_IncludesRejectDetail(t *testing.T) {
	r := pipeline.NewCandidate("rel-1", "finance", "tag", false, false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.Fail("duplicate_table",
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		[]string{"analytics.orders"}, time.Unix(3, 0).UTC()))

	body := getReleaseResponse(r)

	assert.Equal(t, "duplicate_table", body["reject_reason"])
	assert.Equal(t,
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		body["reject_detail"])
}

// TestGetReleaseResponse_IncludesManifestKind verifies the response names how
// the release's artifact is parsed, so the UI can preview the stages that
// apply to its kind (a python release has no compile leg).
func TestGetReleaseResponse_IncludesManifestKind(t *testing.T) {
	rel := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:           "rKIND",
		Status:       pipeline.StatusReceived,
		ManifestKind: release.ManifestKindPythonODCS,
	})

	body, err := json.Marshal(getReleaseResponse(rel))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "python-odcs", decoded["manifest_kind"])
}

// A release stored with the python kind still reads: rehydration takes the
// stored kind as it is and never parses it, so the refusal of that kind on new
// submissions does not reach a stored row.
func TestGetReleaseResponse_AStoredPythonKindStillReads(t *testing.T) {
	rel := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:           "rOLD",
		Status:       pipeline.StatusPromoted,
		ManifestKind: release.ManifestKindPython,
	})

	body, err := json.Marshal(getReleaseResponse(rel))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "python", decoded["manifest_kind"])
}

func TestGetReleaseResponse_IncludesRebaseline(t *testing.T) {
	rel := pipeline.Rehydrate(pipeline.RehydrateInput{ID: "rREBASE", Status: pipeline.StatusPromoted, Bootstrap: true, Rebaseline: true})
	body, err := json.Marshal(getReleaseResponse(rel))
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, true, decoded["bootstrap"])
	assert.Equal(t, true, decoded["rebaseline"])
}

func TestGetRelease_ReturnsTheNotices(t *testing.T) {
	yes := true
	prodSeed := release.Node{UniqueID: "analytics.seed_fx_rates_eur", ServiceName: "finance", NodeType: "dbt-seed", Materialized: "seed", FullRefresh: &yes,
		ContentHash: "sha256:9c2b", SeedColumnsHash: "sha256:e5a0", Schedule: "seed",
		SeedColumns: []release.SeedColumn{{Name: "currency"}, {Name: "rate_date"}, {Name: "rate_to_eur"}}}
	candSeed := prodSeed
	candSeed.ContentHash, candSeed.SeedColumnsHash = "sha256:71fe", "sha256:0d3c"
	candSeed.SeedColumns = append(append([]release.SeedColumn{}, prodSeed.SeedColumns...), release.SeedColumn{Name: "source"})
	candidate := release.Topology{
		candSeed,
		{UniqueID: "analytics.fx_rates_current", NodeType: "dbt-model", Materialized: "view", Schedule: "hourly", UpstreamUniqueIDs: []string{"analytics.seed_fx_rates_eur"}},
		{UniqueID: "analytics.fx_rates_latest_eur", NodeType: "dbt-model", Materialized: "view", Schedule: "hourly", UpstreamUniqueIDs: []string{"analytics.fx_rates_current"}},
	}
	notices, _ := release.BuildNotices(candidate, release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", false)

	now := time.Unix(1, 0).UTC()
	withNotices := pipeline.NewCandidate("rel-notices", "finance", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, now)
	withNotices.SetNotices(notices)
	without := pipeline.NewCandidate("rel-none", "finance", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, now)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-notices"] = withNotices
	releases.releases["rel-none"] = without

	get := func(id string) map[string]any {
		rec := httptest.NewRecorder()
		newTestServer(deps).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/releases/"+id, nil))
		require.Equal(t, http.StatusOK, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		return body
	}

	body := get("rel-notices")
	got, ok := body["notices"].([]any)
	require.True(t, ok, "notices must be a JSON array")
	require.Len(t, got, 1)
	n := got[0].(map[string]any)
	assert.Equal(t, "seed_reload", n["kind"])
	assert.Equal(t, "run", n["load"].(map[string]any)["operation"])
	assert.Equal(t, []any{"source"}, n["columns"].(map[string]any)["added"])
	assert.Equal(t, "analytics.fx_rates_latest_eur", n["rebuilds"].([]any)[1].(map[string]any)["node_id"])

	assert.Equal(t, []any{}, get("rel-none")["notices"])
}

// GET /releases/{id} names the continuo-contracts version that read the
// release's contracts and the contract bundle the parse wrote: both empty until
// the parse ends, and the version stays on a release the parse rejected.
func TestGetRelease_ReturnsContinuoContracts(t *testing.T) {
	bundle := release.ContractBundleRef{
		URI:    "s3://continuo/tenants/default/topologies/rel-1/contracts.json.gz",
		SHA256: "9b0c4d1e7a52f3860be1d9c4a7f08e35b216d0a94c7e53f1a8b20d6e49c71f3a",
	}
	cases := map[string]struct {
		in         pipeline.RehydrateInput
		wantLib    string
		wantBundle release.ContractBundleRef
	}{
		"before the parse": {
			in: pipeline.RehydrateInput{ID: "rel-1", Status: pipeline.StatusParsing, ManifestKind: release.ManifestKindPythonODCS},
		},
		"a promoted release": {
			in: pipeline.RehydrateInput{
				ID: "rel-1", Status: pipeline.StatusPromoted, ManifestKind: release.ManifestKindPythonODCS,
				CandidateContractBundle: bundle, ContinuoContracts: "0.2.0",
			},
			wantLib: "0.2.0", wantBundle: bundle,
		},
		"a rejected release": {
			in: pipeline.RehydrateInput{
				ID: "rel-1", Status: pipeline.StatusRejected, ManifestKind: release.ManifestKindPythonODCS,
				ContinuoContracts: "0.2.0",
			},
			wantLib: "0.2.0",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			deps, releases := newRetryRemediationDeps(time.Unix(1, 0).UTC())
			tc.in.Kind = pipeline.KindCandidate
			releases.releases["rel-1"] = pipeline.Rehydrate(tc.in)

			rec := httptest.NewRecorder()
			newTestServer(deps).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/releases/rel-1", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))

			assert.Equal(t, tc.wantLib, decoded["continuo_contracts"])
			assert.Equal(t, tc.wantBundle.URI, decoded["contract_bundle_uri"])
			assert.Equal(t, tc.wantBundle.SHA256, decoded["contract_bundle_sha256"])
		})
	}
}

// GET /releases/{id} returns the parse timings a run recorded: how long the
// parse took, how long its contracts took, and how many contracts the contract
// bundle holds by family. A rejected release has the durations and no count.
func TestGetRelease_ReturnsTheParseTimings(t *testing.T) {
	parseMs, contractsMs := int64(4180), int64(1260)
	rejectedParseMs, rejectedContractsMs := int64(310), int64(45)
	cases := map[string]struct {
		timings release.ParseTimings
		want    map[string]any
	}{
		"a parsed release": {
			timings: release.ParseTimings{ParseMs: &parseMs, ContractsMs: &contractsMs, ContractsByFamily: map[string]int{"dbt": 3, "python": 2}},
			want:    map[string]any{"parse_ms": 4180.0, "contracts_ms": 1260.0, "contracts_by_family": map[string]any{"dbt": 3.0, "python": 2.0}},
		},
		"a rejected release": {
			timings: release.ParseTimings{ParseMs: &rejectedParseMs, ContractsMs: &rejectedContractsMs},
			want:    map[string]any{"parse_ms": 310.0, "contracts_ms": 45.0},
		},
		"a release with no contracts": {
			timings: release.ParseTimings{ParseMs: &parseMs, ContractsMs: &contractsMs, ContractsByFamily: map[string]int{}},
			want:    map[string]any{"parse_ms": 4180.0, "contracts_ms": 1260.0, "contracts_by_family": map[string]any{}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			deps, releases := newRetryRemediationDeps(time.Unix(1, 0).UTC())
			releases.releases["rel-1"] = pipeline.Rehydrate(pipeline.RehydrateInput{
				ID: "rel-1", Kind: pipeline.KindCandidate, Status: pipeline.StatusValidating, ParseTimings: tc.timings,
			})

			rec := httptest.NewRecorder()
			newTestServer(deps).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/releases/rel-1", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))

			for _, key := range []string{"parse_ms", "contracts_ms", "contracts_by_family"} {
				if want, ok := tc.want[key]; ok {
					assert.Equal(t, want, decoded[key], key)
				} else {
					assert.NotContains(t, decoded, key)
				}
			}
		})
	}
}

// A run the parse has not reached, and one parsed by a topology-controller that
// reports no timings, leave all three keys out rather than answering zero.
func TestGetRelease_OmitsTimingsARunNeverRecorded(t *testing.T) {
	for name, status := range map[string]pipeline.Status{
		"before the parse":                     pipeline.StatusParsing,
		"parsed by a producer without timings": pipeline.StatusPromoted,
	} {
		t.Run(name, func(t *testing.T) {
			deps, releases := newRetryRemediationDeps(time.Unix(1, 0).UTC())
			releases.releases["rel-1"] = pipeline.Rehydrate(pipeline.RehydrateInput{
				ID: "rel-1", Kind: pipeline.KindCandidate, Status: status,
				ParseTimings: release.NewParseTimings(0, 0, nil),
			})

			rec := httptest.NewRecorder()
			newTestServer(deps).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/releases/rel-1", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))

			assert.NotContains(t, decoded, "parse_ms")
			assert.NotContains(t, decoded, "contracts_ms")
			assert.NotContains(t, decoded, "contracts_by_family")
		})
	}
}
