package event

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRemediationEventIDDeterministic(t *testing.T) {
	a := RemediationEventID("rel-1", "schema.node", 1)
	b := RemediationEventID("rel-1", "schema.node", 1)
	c := RemediationEventID("rel-2", "schema.node", 1)
	if a != b {
		t.Fatal("same (release,node,round) must yield same event id")
	}
	if a == c {
		t.Fatal("different release must yield different event id")
	}
}

// TestRemediationEventID_RoundOneKeepsTheOriginalIdentity verifies that round
// 0 and round 1 both reproduce the id the round-less RemediationEventID used
// to produce, so every trigger emitted before rounds existed keeps its
// identity, and only round >= 2 mints a distinct id.
func TestRemediationEventID_RoundOneKeepsTheOriginalIdentity(t *testing.T) {
	legacy := uuid.NewSHA1(remediationEventNamespace, []byte("rel-1|n1")) // formula the round-less id used
	assert.Equal(t, legacy, RemediationEventID("rel-1", "n1", 1))
	assert.Equal(t, legacy, RemediationEventID("rel-1", "n1", 0))
	assert.NotEqual(t, legacy, RemediationEventID("rel-1", "n1", 2))
	assert.NotEqual(t, RemediationEventID("rel-1", "n1", 2), RemediationEventID("rel-1", "n1", 3))
}

func TestRemediationRequestedJSON(t *testing.T) {
	p := RemediationRequested{
		EventID:          "id",
		Source:           "validation",
		ReleaseID:        "rel-1",
		RemediationRound: 2,
		NodeID:           "schema.node",
		Category:         "logic",
		ErrorSignature:   "sig",
		DBTLogURI:        "s3://b/log",
		Repo:             "owner/repo",
		CommitSHA:        "abc",
		ClassifiedAt:     "2026-06-23T00:00:00Z",
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"event_id", "source", "release_id", "remediation_round", "node_id", "category", "error_signature", "dbt_log_uri", "repo", "commit_sha", "classified_at"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing json key %q", k)
		}
	}
	if m["remediation_round"] != float64(2) {
		t.Errorf("remediation_round = %v, want 2", m["remediation_round"])
	}
}
