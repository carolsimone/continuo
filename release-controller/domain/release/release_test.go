package release_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelease_NewIsReceived(t *testing.T) {
	r := release.New("sha-abc", "svc1", "sha-abc", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Equal(t, release.StatusReceived, r.Status())
}

func TestRelease_NewInitialisesImageTags(t *testing.T) {
	r := release.New("sha-abc", "svc1", "sha-abc", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Equal(t, map[string]string{"svc1": "sha-abc"}, r.ImageTags())
	assert.Equal(t, "svc1", r.ChangedService())
}

func TestRelease_TransitionReceivedToParsing(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	assert.Equal(t, release.StatusParsing, r.Status())
}

func TestRelease_TransitionParsingToValidating(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	assert.Equal(t, release.StatusValidating, r.Status())
	assert.Equal(t, []string{"n"}, r.ValidationNodeIDs())
}

func TestRelease_TransitionValidatingToPromoted(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToPromoted(time.Unix(3, 0)))
	assert.Equal(t, release.StatusPromoted, r.Status())
}

func TestRelease_TransitionValidatingToRejected(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToRejected("validation_failed", "", []string{"n"}, time.Unix(3, 0)))
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
}

func TestRelease_CannotSkipStates(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Error(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(1, 0)))
	assert.Error(t, r.TransitionToPromoted(time.Unix(1, 0)))
}

func TestRelease_CannotDoublePromote(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToPromoted(time.Unix(3, 0)))
	assert.Error(t, r.TransitionToPromoted(time.Unix(4, 0)))
}

func TestRelease_TransitionsAreRecorded(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	transitions := r.Transitions()
	require.Len(t, transitions, 3)
	assert.Equal(t, release.StatusReceived, transitions[0].To)
	assert.Equal(t, release.StatusParsing, transitions[1].To)
	assert.Equal(t, release.StatusValidating, transitions[2].To)
}

func TestRelease_SetAssembledImageTags(t *testing.T) {
	r := release.New("sha-abc", "svc-a", "tag-a", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assembled := map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"}
	r.SetAssembledImageTags(assembled)
	assert.Equal(t, assembled, r.ImageTags())
}

func TestRecordValidationResults_RetainedAcrossReject(t *testing.T) {
	r := release.New("rX", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.Topology{{UniqueID: "a"}}, []string{"a"}, time.Unix(3, 0).UTC()))

	r.RecordValidationResults([]release.NodeValidationResult{
		{NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 12},
	})
	require.NoError(t, r.TransitionToRejected("validation_failed", "", []string{"a"}, time.Unix(4, 0).UTC()))

	assert.Equal(t, []release.NodeValidationResult{
		{Stage: "validation", NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 12},
	}, r.PerNodeResults())
	assert.Equal(t, release.StatusRejected, r.Status())
}

func TestRecordValidationResults_RetainedAcrossPromote(t *testing.T) {
	r := release.New("rY", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.Topology{{UniqueID: "a"}}, []string{"a"}, time.Unix(3, 0).UTC()))
	r.RecordValidationResults([]release.NodeValidationResult{{NodeID: "a", Status: "ok"}})
	require.NoError(t, r.TransitionToPromoted(time.Unix(4, 0).UTC()))
	assert.Equal(t, []release.NodeValidationResult{{Stage: "validation", NodeID: "a", Status: "ok"}}, r.PerNodeResults())
}

func TestUpsertStageResult_AddsThenReplacesByStageAndNode(t *testing.T) {
	r := release.New("rel-1", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0).UTC())
	r.UpsertStageResult("validation", release.NodeValidationResult{NodeID: "a", Status: "ok"})
	r.UpsertStageResult("validation", release.NodeValidationResult{NodeID: "b", Status: "failed"})
	// Replace a's outcome; b and stage untouched.
	r.UpsertStageResult("validation", release.NodeValidationResult{NodeID: "a", Status: "failed", DBTLogURI: "s3://l"})

	got := r.PerNodeResults()
	require.Len(t, got, 2)
	byNode := map[string]release.NodeValidationResult{}
	for _, n := range got {
		require.Equal(t, "validation", n.Stage)
		byNode[n.NodeID] = n
	}
	assert.Equal(t, "failed", byNode["a"].Status)
	assert.Equal(t, "s3://l", byNode["a"].DBTLogURI)
	assert.Equal(t, "failed", byNode["b"].Status)
}

func TestRelease_NewCarriesProvenance(t *testing.T) {
	r := release.New("sha-abc", "svc1", "sha-abc", false, false, "acme/demo", "deadbeefcafe", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Equal(t, "acme/demo", r.Repo())
	assert.Equal(t, "deadbeefcafe", r.CommitSHA())
}

func TestRelease_RehydrateRoundTripsProvenance(t *testing.T) {
	r := release.Rehydrate(release.RehydrateInput{
		ID:        "r1",
		Status:    release.StatusRejected,
		Repo:      "acme/demo",
		CommitSHA: "deadbeefcafe",
	})
	assert.Equal(t, "acme/demo", r.Repo())
	assert.Equal(t, "deadbeefcafe", r.CommitSHA())
}

func TestRelease_SetCodeBundleURIRoundTrips(t *testing.T) {
	r := release.New("sha-abc", "svc1", "sha-abc", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Equal(t, "", r.CodeBundleURI(), "unset code_bundle_uri defaults to empty")
	r.SetCodeBundleURI("s3://b/code-bundles/r/bundle.json")
	assert.Equal(t, "s3://b/code-bundles/r/bundle.json", r.CodeBundleURI())
}

func TestRelease_RehydrateRoundTripsCodeBundleURI(t *testing.T) {
	r := release.Rehydrate(release.RehydrateInput{
		ID:            "r1",
		Status:        release.StatusValidating,
		CodeBundleURI: "s3://b/code-bundles/r/bundle.json",
	})
	assert.Equal(t, "s3://b/code-bundles/r/bundle.json", r.CodeBundleURI())
}

func TestNode_CandidateArtifactURIRoundTrips(t *testing.T) {
	// Node.CandidateArtifactURI must round-trip via JSON under the key "candidate_artifact_uri".
	n := release.Node{
		UniqueID:             "a",
		CandidateArtifactURI: "s3://continuo/svc-a/rA/candidate_a.sql",
	}
	b, err := json.Marshal(n)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	assert.Equal(t, "s3://continuo/svc-a/rA/candidate_a.sql", m["candidate_artifact_uri"],
		"JSON key must be candidate_artifact_uri")
	_, hasCandidateSQL := m["candidate_sql"]
	assert.False(t, hasCandidateSQL, "old candidate_sql key must not appear")

	var n2 release.Node
	require.NoError(t, json.Unmarshal(b, &n2))
	assert.Equal(t, n.CandidateArtifactURI, n2.CandidateArtifactURI, "round-trip preserves value")
}

func TestNodeMarshalsCandidateArtifactURI(t *testing.T) {
	n := release.Node{UniqueID: "analytics.orders", CandidateArtifactURI: "s3://b/candidate-sql/rel-1/candidate_analytics.orders.sql"}
	b, err := json.Marshal(n)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"candidate_artifact_uri":"s3://b/candidate-sql/rel-1/candidate_analytics.orders.sql"`)
	assert.NotContains(t, string(b), "candidate_sql_uri",
		"the legacy key must not be emitted — no compatibility alias is written")
}

func TestNode_TestCountRoundTrips(t *testing.T) {
	in := release.Node{UniqueID: "model.svc_a.orders", NodeType: "dbt-model", TestCount: 3}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out release.Node
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.TestCount != 3 {
		t.Fatalf("TestCount = %d, want 3", out.TestCount)
	}
}

func TestTopology_WithoutCandidateArtifactURI_ClearsField(t *testing.T) {
	// WithoutCandidateArtifactURI must return a copy with every node's CandidateArtifactURI
	// cleared, leaving other fields intact.
	topo := release.Topology{
		{UniqueID: "a", CandidateArtifactURI: "s3://continuo/svc-a/rA/a.sql", TableName: "tbl_a"},
		{UniqueID: "b", CandidateArtifactURI: "s3://continuo/svc-a/rA/b.sql", TableName: "tbl_b"},
	}
	stripped := topo.WithoutCandidateArtifactURI()
	for _, n := range stripped {
		assert.Empty(t, n.CandidateArtifactURI, "CandidateArtifactURI must be cleared in stripped topology")
	}
	assert.Equal(t, "tbl_a", stripped[0].TableName, "other fields must be preserved")
	// original must be unmodified
	assert.NotEmpty(t, topo[0].CandidateArtifactURI, "original topology must not be mutated")
}

var (
	t0 = time.Unix(1, 0)
	t1 = time.Unix(2, 0)
)

func newReceivedRelease(t *testing.T) *release.Release {
	t.Helper()
	return release.New("sha-abc", "svc", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
}

func newParsingRelease(t *testing.T) *release.Release {
	t.Helper()
	r := newReceivedRelease(t)
	require.NoError(t, r.TransitionToParsing(t0))
	return r
}

func TestTransitionToSeedBuilding_FromParsing(t *testing.T) {
	r := newParsingRelease(t)
	topo := release.Topology{{UniqueID: "seed.core.fx"}}
	require.NoError(t, r.TransitionToSeedBuilding(topo, []string{"seed.core.fx"}, t0))
	assert.Equal(t, release.StatusSeedBuilding, r.Status())
	assert.Equal(t, topo, r.CandidateTopology())
	assert.Equal(t, []string{"seed.core.fx"}, r.ValidationNodeIDs())
}

func TestTransitionFromSeedBuilding_ToValidating(t *testing.T) {
	r := newParsingRelease(t)
	require.NoError(t, r.TransitionToSeedBuilding(release.Topology{}, []string{"seed.core.fx", "model.fin.report"}, t0))
	require.NoError(t, r.TransitionFromSeedBuilding([]string{"model.fin.report"}, t1))
	assert.Equal(t, release.StatusValidating, r.Status())
	assert.Equal(t, []string{"model.fin.report"}, r.ValidationNodeIDs(),
		"TransitionFromSeedBuilding narrows the persisted validation set to the filtered IDs")
}

func TestTransitionToSeedBuilding_RejectsWrongSource(t *testing.T) {
	r := newReceivedRelease(t) // status=received
	require.Error(t, r.TransitionToSeedBuilding(release.Topology{}, nil, t0))
}

func TestTransitionToRejected_FromSeedBuilding(t *testing.T) {
	r := newParsingRelease(t)
	require.NoError(t, r.TransitionToSeedBuilding(release.Topology{}, nil, t0))
	require.NoError(t, r.TransitionToRejected("seed_build_failed", "", nil, t1))
	assert.Equal(t, release.StatusRejected, r.Status())
}

func TestTransitionToCompiling_FromReceived(t *testing.T) {
	r := newReceivedRelease(t)
	require.NoError(t, r.TransitionToCompiling(t0))
	assert.Equal(t, release.StatusCompiling, r.Status())
}

func TestTransitionFromCompiling_ToParsing(t *testing.T) {
	r := newReceivedRelease(t)
	require.NoError(t, r.TransitionToCompiling(t0))
	require.NoError(t, r.TransitionFromCompiling(t1))
	assert.Equal(t, release.StatusParsing, r.Status())
}

func TestTransitionToCompiling_RejectsWrongSource(t *testing.T) {
	r := newParsingRelease(t) // already parsing
	require.Error(t, r.TransitionToCompiling(t0))
}

func TestTransitionToRejected_FromCompiling(t *testing.T) {
	r := newReceivedRelease(t)
	require.NoError(t, r.TransitionToCompiling(t0))
	require.NoError(t, r.TransitionToRejected("compile_failed", "", nil, t1))
	assert.Equal(t, release.StatusRejected, r.Status())
}

func TestTransitionToRejected_PersistsDetail(t *testing.T) {
	r := release.New("rel-1", "finance", "tag", false, false, "owner/repo", "abc123", release.ManifestKindDbt, time.Unix(1, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0)))

	require.NoError(t, r.TransitionToRejected(
		"duplicate_table",
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		[]string{"analytics.orders"},
		time.Unix(3, 0),
	))

	assert.Equal(t, "duplicate_table", r.RejectReason())
	assert.Equal(t,
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		r.RejectDetail())
	assert.Equal(t, []string{"analytics.orders"}, r.FailingNodes())
}

func TestRecordStageResults_ReplacesPerStage(t *testing.T) {
	r := release.New("rel-1", "core", "tag", false, false, "o/r", "sha", release.ManifestKindDbt, time.Now())
	r.RecordStageResults("compile", []release.NodeValidationResult{
		{Stage: "compile", NodeID: "core", Status: "failed", DBTLogURI: "s3://c.log"},
	})
	r.RecordStageResults("validation", []release.NodeValidationResult{
		{Stage: "validation", NodeID: "analytics.x", Status: "ok"},
	})
	// re-deliver compile: must replace, not duplicate
	r.RecordStageResults("compile", []release.NodeValidationResult{
		{Stage: "compile", NodeID: "core", Status: "failed", DBTLogURI: "s3://c2.log"},
	})
	got := r.PerNodeResults()
	if len(got) != 2 {
		t.Fatalf("want 2 results (1 compile + 1 validation), got %d", len(got))
	}
	var compile release.NodeValidationResult
	for _, n := range got {
		if n.Stage == "compile" {
			compile = n
		}
	}
	if compile.DBTLogURI != "s3://c2.log" {
		t.Fatalf("compile result not replaced: %q", compile.DBTLogURI)
	}
}

// newValidatingRelease builds a release with the given shadow flag and drives
// it through Parsing into Validating.
func newValidatingRelease(t *testing.T, shadow bool) *release.Release {
	t.Helper()
	r := release.New("sha-abc", "svc", "tag", false, shadow, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(t0))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, t1))
	return r
}

func TestTransitionToValidated_OnlyFromValidating(t *testing.T) {
	r := newValidatingRelease(t, true)
	require.NoError(t, r.TransitionToValidated(time.Unix(3, 0)))
	assert.Equal(t, release.StatusValidated, r.Status())

	r2 := newReceivedRelease(t)
	assert.Error(t, r2.TransitionToValidated(time.Unix(1, 0)))
}

func TestNewRelease_CarriesShadow(t *testing.T) {
	shadow := release.New("sha-abc", "svc1", "sha-abc", false, true, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.True(t, shadow.IsShadow())

	plain := release.New("sha-abc", "svc1", "sha-abc", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.False(t, plain.IsShadow())
}
