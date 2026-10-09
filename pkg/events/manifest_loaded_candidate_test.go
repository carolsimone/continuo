package events_test

import (
	"encoding/json"
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
