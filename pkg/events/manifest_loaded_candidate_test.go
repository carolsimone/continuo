package events_test

import (
	"encoding/json"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/events"
)

type manifestLoadedCandidateFixture struct {
	Input struct {
		TenantID   string                         `json:"tenant_id"`
		Producer   string                         `json:"producer"`
		OccurredAt time.Time                      `json:"occurred_at"`
		Payload    events.ManifestLoadedCandidate `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

var manifestLoadedCandidateFixtures = []string{
	"testdata/manifest_loaded_candidate_v2_ok.json",
	"testdata/manifest_loaded_candidate_v2_failed.json",
}

func loadManifestLoadedCandidateFixture(t *testing.T, path string) manifestLoadedCandidateFixture {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is one of the fixed fixture paths above
	require.NoError(t, err)
	var fx manifestLoadedCandidateFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

// fixtureFields turns a golden fixture's fields object into the string map a
// stream entry is read as: every header field a string, the payload the raw
// JSON text of the fixture's payload object.
func fixtureFields(t *testing.T, fields map[string]json.RawMessage) map[string]string {
	t.Helper()
	out := make(map[string]string, len(fields))
	for key, raw := range fields {
		if key == "payload" {
			out[key] = string(raw)
			continue
		}
		var s string
		require.NoError(t, json.Unmarshal(raw, &s), key)
		out[key] = s
	}
	return out
}

func assertFieldsMatchFixture(t *testing.T, want map[string]json.RawMessage, got map[string]any) {
	t.Helper()
	require.Len(t, got, len(want), "the entry must carry exactly the fixture's fields")
	for _, key := range []string{"event_id", "tenant_id", "occurred_at", "producer", "schema_version"} {
		var w string
		require.NoError(t, json.Unmarshal(want[key], &w), key)
		assert.Equal(t, w, got[key], key)
	}
	var wantPayload, gotPayload any
	require.NoError(t, json.Unmarshal(want["payload"], &wantPayload))
	require.NoError(t, json.Unmarshal([]byte(got["payload"].(string)), &gotPayload))
	assert.Equal(t, wantPayload, gotPayload)
}

func TestManifestLoadedCandidateFields_MatchGoldenFixtures(t *testing.T) {
	for _, path := range manifestLoadedCandidateFixtures {
		t.Run(path, func(t *testing.T) {
			fx := loadManifestLoadedCandidateFixture(t, path)
			got, err := events.ManifestLoadedCandidateFields(fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
			require.NoError(t, err)
			assertFieldsMatchFixture(t, fx.Fields, got)
		})
	}
}

func TestDecodeManifestLoadedCandidate_ReadsGoldenFixtures(t *testing.T) {
	for _, path := range manifestLoadedCandidateFixtures {
		t.Run(path, func(t *testing.T) {
			fx := loadManifestLoadedCandidateFixture(t, path)
			env, p, err := events.DecodeManifestLoadedCandidate(fixtureFields(t, fx.Fields))
			require.NoError(t, err)
			assert.Equal(t, fx.Input.Payload, p)
			assert.Equal(t, fx.Input.TenantID, env.TenantID)
			assert.Equal(t, fx.Input.Producer, env.Producer)
			assert.True(t, fx.Input.OccurredAt.Equal(env.OccurredAt))
			assert.Equal(t, events.ManifestLoadedCandidateEventID(fx.Input.TenantID, fx.Input.Payload.ReleaseID), env.EventID)
		})
	}
}

func TestManifestLoadedCandidateEventID_IsStablePerRelease(t *testing.T) {
	a := events.ManifestLoadedCandidateEventID("default", "rel-1")
	assert.Equal(t, a, events.ManifestLoadedCandidateEventID("default", "rel-1"))
	assert.NotEqual(t, a, events.ManifestLoadedCandidateEventID("default", "rel-2"))
	assert.NotEqual(t, a, events.ManifestLoadedCandidateEventID("acme", "rel-1"), "the tenant is part of the natural key")
}

func TestManifestLoadedCandidateFields_AFailureWithoutNodesCarriesAnEmptyList(t *testing.T) {
	got, err := events.ManifestLoadedCandidateFields("default", "topology-controller", time.Unix(0, 0), events.ManifestLoadedCandidate{
		ReleaseID: "rel-1", Status: events.ManifestStatusFailed, FailureKind: "invalid_artifact", Detail: "empty manifest",
	})
	require.NoError(t, err)
	assert.Contains(t, got["payload"], `"failed_nodes":[]`)
}

func TestManifestLoadedCandidateFields_OKCarriesTheContractBundleAndTheLibraryVersion(t *testing.T) {
	fx := loadManifestLoadedCandidateFixture(t, "testdata/manifest_loaded_candidate_v2_ok.json")
	p := fx.Input.Payload
	require.Equal(t, "s3://continuo/tenants/default/topologies/rel-2026-10-08-0001/contracts.json.gz", p.ContractBundleURI)
	require.Equal(t, "9b0c4d1e7a52f3860be1d9c4a7f08e35b216d0a94c7e53f1a8b20d6e49c71f3a", p.ContractBundleSHA256)
	require.Equal(t, "0.2.0", p.ContinuoContracts)

	got, err := events.ManifestLoadedCandidateFields(fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, p)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(got["payload"].(string)), &payload))
	assert.Equal(t, p.ContractBundleURI, payload["contract_bundle_uri"])
	assert.Equal(t, p.ContractBundleSHA256, payload["contract_bundle_sha256"])
	assert.Equal(t, p.ContinuoContracts, payload["continuo_contracts"])
}

func TestManifestLoadedCandidateFields_FailedCarriesTheLibraryVersion(t *testing.T) {
	fx := loadManifestLoadedCandidateFixture(t, "testdata/manifest_loaded_candidate_v2_failed.json")
	p := fx.Input.Payload
	require.Equal(t, "invalid_contract", p.FailureKind)
	require.Equal(t, "0.2.0", p.ContinuoContracts)
	require.Len(t, p.FailedNodes, 1)
	require.Equal(t, "analytics.py_daily_kpis", p.FailedNodes[0].NodeID)

	got, err := events.ManifestLoadedCandidateFields(fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, p)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(got["payload"].(string)), &payload))
	assert.Equal(t, "0.2.0", payload["continuo_contracts"])
	assert.NotContains(t, payload, "contract_bundle_uri", "a failed release writes no contract bundle")
	assert.NotContains(t, payload, "contract_bundle_sha256", "a failed release writes no contract bundle")
}

func TestManifestLoadedCandidate_DecodesTheParseTimings(t *testing.T) {
	ok := loadManifestLoadedCandidateFixture(t, "testdata/manifest_loaded_candidate_v2_ok.json")
	_, p, err := events.DecodeManifestLoadedCandidate(fixtureFields(t, ok.Fields))
	require.NoError(t, err)
	assert.Equal(t, int64(4180), p.ParseMs)
	assert.Equal(t, int64(1260), p.ContractsMs)
	assert.Equal(t, map[model.ContractFamily]int{"dbt": 3, "python": 2}, p.ContractsByFamily)

	failed := loadManifestLoadedCandidateFixture(t, "testdata/manifest_loaded_candidate_v2_failed.json")
	_, p, err = events.DecodeManifestLoadedCandidate(fixtureFields(t, failed.Fields))
	require.NoError(t, err)
	assert.Equal(t, int64(310), p.ParseMs)
	assert.Equal(t, int64(45), p.ContractsMs)
	assert.Nil(t, p.ContractsByFamily, "a failed parse counts no contracts by family")
}

func TestManifestLoadedCandidateFields_BothOutcomesCarryTheDurationsAndOnlyOKTheCounts(t *testing.T) {
	render := func(p events.ManifestLoadedCandidate) map[string]any {
		t.Helper()
		got, err := events.ManifestLoadedCandidateFields("default", "topology-controller", time.Unix(0, 0), p)
		require.NoError(t, err)
		var payload map[string]any
		require.NoError(t, json.Unmarshal([]byte(got["payload"].(string)), &payload))
		return payload
	}

	ok := render(events.ManifestLoadedCandidate{
		ReleaseID: "rel-1", Status: events.ManifestStatusOK, TopologyURI: "s3://b/k", TopologySHA256: "abc",
		ParseMs: 7, ContractsMs: 3, ContractsByFamily: map[model.ContractFamily]int{"dbt": 2},
	})
	assert.EqualValues(t, 7, ok["parse_ms"])
	assert.EqualValues(t, 3, ok["contracts_ms"])
	assert.Equal(t, map[string]any{"dbt": float64(2)}, ok["contracts_by_family"])

	failed := render(events.ManifestLoadedCandidate{
		ReleaseID: "rel-2", Status: events.ManifestStatusFailed, FailureKind: "invalid_sql", Detail: "d",
		ParseMs: 9, ContractsMs: 4, ContractsByFamily: map[model.ContractFamily]int{"dbt": 2},
	})
	assert.EqualValues(t, 9, failed["parse_ms"])
	assert.EqualValues(t, 4, failed["contracts_ms"])
	assert.NotContains(t, failed, "contracts_by_family", "only an ok parse counts the contracts it wrote")
}

func TestManifestLoadedCandidateFields_AnOKReleaseWithNoContractsCarriesAnEmptyCount(t *testing.T) {
	got, err := events.ManifestLoadedCandidateFields("default", "topology-controller", time.Unix(0, 0), events.ManifestLoadedCandidate{
		ReleaseID: "rel-1", Status: events.ManifestStatusOK, TopologyURI: "s3://b/k", TopologySHA256: "abc",
	})
	require.NoError(t, err)
	assert.Contains(t, got["payload"], `"contracts_by_family":{}`)
}

func TestDecodeManifestLoadedCandidate_AnEntryWithoutTheNewFieldsDecodes(t *testing.T) {
	for name, payload := range map[string]string{
		"ok":     `{"release_id":"rel-1","status":"ok","topology_uri":"s3://b/k","topology_sha256":"abc","node_count":1,"code_bundle_uri":"s3://b/c"}`,
		"failed": `{"release_id":"rel-1","status":"failed","failure_kind":"invalid_sql","detail":"d","failed_nodes":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, p, err := events.DecodeManifestLoadedCandidate(map[string]string{
				"event_id":       "e",
				"tenant_id":      "default",
				"occurred_at":    "2026-10-08T09:30:00.000000Z",
				"producer":       "topology-controller",
				"schema_version": "1",
				"payload":        payload,
			})
			require.NoError(t, err)
			assert.Empty(t, p.ContractBundleURI)
			assert.Empty(t, p.ContractBundleSHA256)
			assert.Empty(t, p.ContinuoContracts)
			assert.Zero(t, p.ParseMs)
			assert.Zero(t, p.ContractsMs)
			// A nil map is how an absent key decodes: the event of an older producer.
			assert.Nil(t, p.ContractsByFamily)
		})
	}
}

// TestDecodeManifestLoadedCandidate_AnEmptyCountByFamilyIsNotNil pins the other
// half of that rule: a producer that reports the count writes {} for a release
// with no contract, which decodes as an empty map that is not nil.
func TestDecodeManifestLoadedCandidate_AnEmptyCountByFamilyIsNotNil(t *testing.T) {
	_, p, err := events.DecodeManifestLoadedCandidate(map[string]string{
		"event_id":       "e",
		"tenant_id":      "default",
		"occurred_at":    "2026-10-08T09:30:00.000000Z",
		"producer":       "topology-controller",
		"schema_version": "1",
		"payload": `{"release_id":"rel-1","status":"ok","topology_uri":"s3://b/k","topology_sha256":"abc",` +
			`"node_count":0,"code_bundle_uri":"","parse_ms":12,"contracts_ms":0,"contracts_by_family":{}}`,
	})
	require.NoError(t, err)
	assert.NotNil(t, p.ContractsByFamily)
	assert.Empty(t, p.ContractsByFamily)
}

func TestDecodeManifestLoadedCandidate_RejectsMalformedEntries(t *testing.T) {
	valid := func() map[string]string {
		return map[string]string{
			"event_id":       "e",
			"tenant_id":      "default",
			"occurred_at":    "2026-10-08T09:30:00.000000Z",
			"producer":       "topology-controller",
			"schema_version": "1",
			"payload":        `{"release_id":"rel-1","status":"ok","topology_uri":"s3://b/k","topology_sha256":"abc","node_count":1}`,
		}
	}
	_, _, err := events.DecodeManifestLoadedCandidate(valid())
	require.NoError(t, err, "the base case must decode")

	for name, mutate := range map[string]func(map[string]string){
		"no payload":       func(f map[string]string) { delete(f, "payload") },
		"schema_version 2": func(f map[string]string) { f["schema_version"] = "2" },
		"payload not json": func(f map[string]string) { f["payload"] = "{" },
		"empty release_id": func(f map[string]string) {
			f["payload"] = `{"release_id":"","status":"ok","topology_uri":"s3://b/k","topology_sha256":"abc"}`
		},
		"unknown status": func(f map[string]string) { f["payload"] = `{"release_id":"rel-1","status":"maybe"}` },
		"ok without uri": func(f map[string]string) {
			f["payload"] = `{"release_id":"rel-1","status":"ok","topology_sha256":"abc"}`
		},
		"ok without sha": func(f map[string]string) {
			f["payload"] = `{"release_id":"rel-1","status":"ok","topology_uri":"s3://b/k"}`
		},
		"bad occurred_at": func(f map[string]string) { f["occurred_at"] = "yesterday" },
	} {
		t.Run(name, func(t *testing.T) {
			f := valid()
			mutate(f)
			_, _, err := events.DecodeManifestLoadedCandidate(f)
			assert.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}
