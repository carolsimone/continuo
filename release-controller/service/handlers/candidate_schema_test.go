package handlers_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

// TestCandidateSchemaForAddsAFixedElevenBytes pins the one property callers
// that mint a release id rely on: the candidate schema is the release id plus a
// constant 11-byte prefix, with no other length change, because
// SanitizeSchemaSuffix substitutes characters one-for-one.
//
// PostgreSQL truncates an identifier past 63 bytes instead of rejecting it, so
// a release id longer than 52 bytes yields a schema whose discriminating tail
// is silently cut and can collide with another release's. agent-remediation
// bounds the shadow release ids it mints against exactly that budget; widening
// this prefix without widening that bound would reintroduce the collision, so
// the width is asserted here rather than left implicit.
func TestCandidateSchemaForAddsAFixedElevenBytes(t *testing.T) {
	for _, releaseID := range []string{
		"",
		"rel-abc1234-1",
		"shadow-rel-abc1234-1-analytics.py_daily_kpis-a2",
	} {
		schema := handlers.CandidateSchemaFor(releaseID)
		require.Equal(t, len(releaseID)+11, len(schema),
			"candidate schema for %q must be exactly 11 bytes longer than the release id", releaseID)
	}
	require.Equal(t, "_candidate_", handlers.CandidateSchemaFor(""))
}
