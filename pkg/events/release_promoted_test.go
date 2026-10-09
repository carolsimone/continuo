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

type releasePromotedFixture struct {
	Input struct {
		TenantID   string                 `json:"tenant_id"`
		Producer   string                 `json:"producer"`
		OccurredAt time.Time              `json:"occurred_at"`
		Payload    events.ReleasePromoted `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

func loadReleasePromotedFixture(t *testing.T) releasePromotedFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/release_promoted_v2.json")
	require.NoError(t, err)
	var fx releasePromotedFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func TestReleasePromotedFields_MatchGoldenFixture(t *testing.T) {
	fx := loadReleasePromotedFixture(t)
	got, err := events.ReleasePromotedFields(fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
	require.NoError(t, err)
	assertFieldsMatchFixture(t, fx.Fields, got)
}

func TestDecodeReleasePromoted_ReadsGoldenFixture(t *testing.T) {
	fx := loadReleasePromotedFixture(t)
	env, p, err := events.DecodeReleasePromoted(fixtureFields(t, fx.Fields))
	require.NoError(t, err)
	want := fx.Input.Payload
	assert.True(t, want.PromotedAt.Equal(p.PromotedAt))
	want.PromotedAt, p.PromotedAt = time.Time{}, time.Time{}
	assert.Equal(t, want, p)
	assert.Equal(t, fx.Input.TenantID, env.TenantID)
	assert.Equal(t, fx.Input.Producer, env.Producer)
	assert.Equal(t, events.ReleasePromotedEventID(fx.Input.TenantID, fx.Input.Payload.PromotionSeq), env.EventID)
}

func TestReleasePromotedEventID_IsStablePerSeq(t *testing.T) {
	a := events.ReleasePromotedEventID("default", 7)
	assert.Equal(t, a, events.ReleasePromotedEventID("default", 7))
	assert.NotEqual(t, a, events.ReleasePromotedEventID("default", 8))
	assert.NotEqual(t, a, events.ReleasePromotedEventID("acme", 7), "the tenant is part of the natural key")
}

func TestReleasePromotedFields_NilChangedNodeIDsAreAnEmptyList(t *testing.T) {
	cest := time.FixedZone("CEST", 2*60*60)
	got, err := events.ReleasePromotedFields("default", "release-controller", time.Unix(0, 0), events.ReleasePromoted{
		ReleaseID: "rel-1", PromotionSeq: 1, TopologyURI: "s3://b/k", TopologySHA256: "abc", Bootstrap: true,
		PromotedAt: time.Date(2026, 10, 8, 11, 0, 0, 0, cest),
	})
	require.NoError(t, err)
	assert.Contains(t, got["payload"], `"changed_node_ids":[]`)
	assert.Contains(t, got["payload"], `"promoted_at":"2026-10-08T09:00:00Z"`, "promoted_at is written in UTC")
}

func TestDecodeReleasePromoted_RejectsMalformedEntries(t *testing.T) {
	valid := func() map[string]string {
		return map[string]string{
			"event_id":       "e",
			"tenant_id":      "default",
			"occurred_at":    "2026-10-08T09:31:00.000000Z",
			"producer":       "release-controller",
			"schema_version": "1",
			"payload":        `{"release_id":"rel-1","promotion_seq":3,"topology_uri":"s3://b/k","topology_sha256":"abc","changed_node_ids":[]}`,
		}
	}
	_, _, err := events.DecodeReleasePromoted(valid())
	require.NoError(t, err, "the base case must decode")

	for name, mutate := range map[string]func(map[string]string){
		"no payload":       func(f map[string]string) { delete(f, "payload") },
		"schema_version 2": func(f map[string]string) { f["schema_version"] = "2" },
		"payload not json": func(f map[string]string) { f["payload"] = "[" },
		"empty release_id": func(f map[string]string) {
			f["payload"] = `{"release_id":"","promotion_seq":3,"topology_uri":"s3://b/k","topology_sha256":"abc"}`
		},
		"seq zero": func(f map[string]string) {
			f["payload"] = `{"release_id":"rel-1","promotion_seq":0,"topology_uri":"s3://b/k","topology_sha256":"abc"}`
		},
		"no topology_uri": func(f map[string]string) {
			f["payload"] = `{"release_id":"rel-1","promotion_seq":3,"topology_sha256":"abc"}`
		},
		"no sha256": func(f map[string]string) {
			f["payload"] = `{"release_id":"rel-1","promotion_seq":3,"topology_uri":"s3://b/k"}`
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := valid()
			mutate(f)
			_, _, err := events.DecodeReleasePromoted(f)
			assert.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}
