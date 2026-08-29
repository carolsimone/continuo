package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation/domain/failure"
)

// TestEvidenceFromRejected_RemediationRound verifies that a
// remediation.retry_requested:v1 message — a compile rejection payload plus a
// top-level remediation_round — decodes to a FailureEvidence carrying that
// round, and that a payload with no remediation_round field at all (the
// original release.rejected:v1 shape) decodes to the zero value, which the
// FailureEvidence.RemediationRound convention treats as round 1.
func TestEvidenceFromRejected_RemediationRound(t *testing.T) {
	t.Run("remediation_round threaded onto evidence", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-1","stage":"compile","reason":"compile_failed","remediation_round":2,
		  "repo":"o/r","commit_sha":"sha","per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		require.NoError(t, err)
		require.Len(t, evs, 1)
		assert.Equal(t, failure.SourceCompile, evs[0].Source)
		assert.Equal(t, 2, evs[0].RemediationRound)
	})

	t.Run("absent remediation_round decodes to the zero value, which means round 1", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-1","stage":"compile","reason":"compile_failed",
		  "repo":"o/r","commit_sha":"sha","per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		require.NoError(t, err)
		require.Len(t, evs, 1)
		assert.Equal(t, failure.SourceCompile, evs[0].Source)
		assert.Equal(t, 0, evs[0].RemediationRound, "0 and 1 both mean round 1; ClassifyRejection normalises it")
	})
}
