package handlers

// CandidateSchemaFor returns the candidate schema name for a release: the
// `_candidate_` prefix followed by the release ID sanitized into a dbt
// schema-name suffix (SanitizeSchemaSuffix). It is the single definition of
// this naming rule — every leg that emits or consumes a release's candidate
// schema (compile.requested, validation.requested, seed.build.requested,
// release.rejected, release.promoted) must derive it from here so the name
// the parse rehearsal validates the runtime legs actually use never drifts.
func CandidateSchemaFor(releaseID string) string {
	return "_candidate_" + SanitizeSchemaSuffix(releaseID)
}
