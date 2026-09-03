package pipeline_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCandidate_IsReceived(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc1", "sha-abc", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Equal(t, pipeline.StatusReceived, r.Status())
}

func TestNewCandidate_InitialisesImageTags(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc1", "sha-abc", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Equal(t, map[string]string{"svc1": "sha-abc"}, r.ImageTags())
	assert.Equal(t, "svc1", r.ChangedService())
}

func TestRun_TransitionReceivedToParsing(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	assert.Equal(t, pipeline.StatusParsing, r.Status())
}

func TestRun_TransitionParsingToValidating(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	assert.Equal(t, pipeline.StatusValidating, r.Status())
	assert.Equal(t, []string{"n"}, r.ValidationNodeIDs())
}

func TestRun_TransitionValidatingToPromoted(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.Promote(time.Unix(3, 0)))
	assert.Equal(t, pipeline.StatusPromoted, r.Status())
}

func TestRun_TransitionValidatingToRejected(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.Fail("validation_failed", "", []string{"n"}, time.Unix(3, 0)))
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.FailReason())
}

func TestRun_CannotSkipStates(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Error(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(1, 0)))
	assert.Error(t, r.Promote(time.Unix(1, 0)))
}

func TestRun_CannotDoublePromote(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.Promote(time.Unix(3, 0)))
	assert.Error(t, r.Promote(time.Unix(4, 0)))
}

func TestRun_TransitionsAreRecorded(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	transitions := r.Transitions()
	require.Len(t, transitions, 3)
	assert.Equal(t, pipeline.StatusReceived, transitions[0].To)
	assert.Equal(t, pipeline.StatusParsing, transitions[1].To)
	assert.Equal(t, pipeline.StatusValidating, transitions[2].To)
}

func TestRun_SetAssembledImageTags(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc-a", "tag-a", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assembled := map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"}
	r.SetAssembledImageTags(assembled)
	assert.Equal(t, assembled, r.ImageTags())
}

func TestRecordValidationResults_RetainedAcrossReject(t *testing.T) {
	r := pipeline.NewCandidate("rX", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.Topology{{UniqueID: "a"}}, []string{"a"}, time.Unix(3, 0).UTC()))

	r.RecordValidationResults([]pipeline.NodeValidationResult{
		{NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 12},
	})
	require.NoError(t, r.Fail("validation_failed", "", []string{"a"}, time.Unix(4, 0).UTC()))

	assert.Equal(t, []pipeline.NodeValidationResult{
		{Stage: "validation", NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 12},
	}, r.PerNodeResults())
	assert.Equal(t, pipeline.StatusRejected, r.Status())
}

func TestRecordValidationResults_RetainedAcrossPromote(t *testing.T) {
	r := pipeline.NewCandidate("rY", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.Topology{{UniqueID: "a"}}, []string{"a"}, time.Unix(3, 0).UTC()))
	r.RecordValidationResults([]pipeline.NodeValidationResult{{NodeID: "a", Status: "ok"}})
	require.NoError(t, r.Promote(time.Unix(4, 0).UTC()))
	assert.Equal(t, []pipeline.NodeValidationResult{{Stage: "validation", NodeID: "a", Status: "ok"}}, r.PerNodeResults())
}

func TestUpsertStageResult_AddsThenReplacesByStageAndNode(t *testing.T) {
	r := pipeline.NewCandidate("rel-1", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0).UTC())
	r.UpsertStageResult("validation", pipeline.NodeValidationResult{NodeID: "a", Status: "ok"})
	r.UpsertStageResult("validation", pipeline.NodeValidationResult{NodeID: "b", Status: "failed"})
	// Replace a's outcome; b and stage untouched.
	r.UpsertStageResult("validation", pipeline.NodeValidationResult{NodeID: "a", Status: "failed", DBTLogURI: "s3://l"})

	got := r.PerNodeResults()
	require.Len(t, got, 2)
	byNode := map[string]pipeline.NodeValidationResult{}
	for _, n := range got {
		require.Equal(t, "validation", n.Stage)
		byNode[n.NodeID] = n
	}
	assert.Equal(t, "failed", byNode["a"].Status)
	assert.Equal(t, "s3://l", byNode["a"].DBTLogURI)
	assert.Equal(t, "failed", byNode["b"].Status)
}

func TestNewCandidate_CarriesProvenance(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc1", "sha-abc", false, "acme/demo", "deadbeefcafe", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Equal(t, "acme/demo", r.Repo())
	assert.Equal(t, "deadbeefcafe", r.CommitSHA())
}

func TestRun_RehydrateRoundTripsProvenance(t *testing.T) {
	r := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:        "r1",
		Kind:      pipeline.KindCandidate,
		Status:    pipeline.StatusRejected,
		Repo:      "acme/demo",
		CommitSHA: "deadbeefcafe",
	})
	assert.Equal(t, "acme/demo", r.Repo())
	assert.Equal(t, "deadbeefcafe", r.CommitSHA())
}

func TestRun_SetCodeBundleURIRoundTrips(t *testing.T) {
	r := pipeline.NewCandidate("sha-abc", "svc1", "sha-abc", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Equal(t, "", r.CodeBundleURI(), "unset code_bundle_uri defaults to empty")
	r.SetCodeBundleURI("s3://b/code-bundles/r/bundle.json")
	assert.Equal(t, "s3://b/code-bundles/r/bundle.json", r.CodeBundleURI())
}

func TestRun_RehydrateRoundTripsCodeBundleURI(t *testing.T) {
	r := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:            "r1",
		Kind:          pipeline.KindCandidate,
		Status:        pipeline.StatusValidating,
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
	lifecycleT0 = time.Unix(1, 0)
	lifecycleT1 = time.Unix(2, 0)
)

func newReceivedCandidate(t *testing.T) *pipeline.Run {
	t.Helper()
	return pipeline.NewCandidate("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
}

func newParsingCandidate(t *testing.T) *pipeline.Run {
	t.Helper()
	r := newReceivedCandidate(t)
	require.NoError(t, r.TransitionToParsing(lifecycleT0))
	return r
}

func TestTransitionToSeedBuilding_FromParsing(t *testing.T) {
	r := newParsingCandidate(t)
	topo := release.Topology{{UniqueID: "seed.core.fx"}}
	require.NoError(t, r.TransitionToSeedBuilding(topo, []string{"seed.core.fx"}, lifecycleT0))
	assert.Equal(t, pipeline.StatusSeedBuilding, r.Status())
	assert.Equal(t, topo, r.CandidateTopology())
	assert.Equal(t, []string{"seed.core.fx"}, r.ValidationNodeIDs())
}

func TestTransitionFromSeedBuilding_ToValidating(t *testing.T) {
	r := newParsingCandidate(t)
	require.NoError(t, r.TransitionToSeedBuilding(release.Topology{}, []string{"seed.core.fx", "model.fin.report"}, lifecycleT0))
	require.NoError(t, r.TransitionFromSeedBuilding([]string{"model.fin.report"}, lifecycleT1))
	assert.Equal(t, pipeline.StatusValidating, r.Status())
	assert.Equal(t, []string{"model.fin.report"}, r.ValidationNodeIDs(),
		"TransitionFromSeedBuilding narrows the persisted validation set to the filtered IDs")
}

func TestTransitionToSeedBuilding_RejectsWrongSource(t *testing.T) {
	r := newReceivedCandidate(t) // status=received
	require.Error(t, r.TransitionToSeedBuilding(release.Topology{}, nil, lifecycleT0))
}

func TestTransitionToRejected_FromSeedBuilding(t *testing.T) {
	r := newParsingCandidate(t)
	require.NoError(t, r.TransitionToSeedBuilding(release.Topology{}, nil, lifecycleT0))
	require.NoError(t, r.Fail("seed_build_failed", "", nil, lifecycleT1))
	assert.Equal(t, pipeline.StatusRejected, r.Status())
}

func TestTransitionToCompiling_FromReceived(t *testing.T) {
	r := newReceivedCandidate(t)
	require.NoError(t, r.TransitionToCompiling(lifecycleT0))
	assert.Equal(t, pipeline.StatusCompiling, r.Status())
}

func TestTransitionFromCompiling_ToParsing(t *testing.T) {
	r := newReceivedCandidate(t)
	require.NoError(t, r.TransitionToCompiling(lifecycleT0))
	require.NoError(t, r.TransitionFromCompiling(lifecycleT1))
	assert.Equal(t, pipeline.StatusParsing, r.Status())
}

func TestTransitionToCompiling_RejectsWrongSource(t *testing.T) {
	r := newParsingCandidate(t) // already parsing
	require.Error(t, r.TransitionToCompiling(lifecycleT0))
}

func TestTransitionToRejected_FromCompiling(t *testing.T) {
	r := newReceivedCandidate(t)
	require.NoError(t, r.TransitionToCompiling(lifecycleT0))
	require.NoError(t, r.Fail("compile_failed", "", nil, lifecycleT1))
	assert.Equal(t, pipeline.StatusRejected, r.Status())
}

func TestTransitionToRejected_PersistsDetail(t *testing.T) {
	r := pipeline.NewCandidate("rel-1", "finance", "tag", false, "owner/repo", "abc123", release.ManifestKindDbt, time.Unix(1, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0)))

	require.NoError(t, r.Fail(
		"duplicate_table",
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		[]string{"analytics.orders"},
		time.Unix(3, 0),
	))

	assert.Equal(t, "duplicate_table", r.FailReason())
	assert.Equal(t,
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		r.FailDetail())
	assert.Equal(t, []string{"analytics.orders"}, r.FailingNodes())
}

func TestRecordStageResults_ReplacesPerStage(t *testing.T) {
	r := pipeline.NewCandidate("rel-1", "core", "tag", false, "o/r", "sha", release.ManifestKindDbt, time.Now())
	r.RecordStageResults("compile", []pipeline.NodeValidationResult{
		{Stage: "compile", NodeID: "core", Status: "failed", DBTLogURI: "s3://c.log"},
	})
	r.RecordStageResults("validation", []pipeline.NodeValidationResult{
		{Stage: "validation", NodeID: "analytics.x", Status: "ok"},
	})
	// re-deliver compile: must replace, not duplicate
	r.RecordStageResults("compile", []pipeline.NodeValidationResult{
		{Stage: "compile", NodeID: "core", Status: "failed", DBTLogURI: "s3://c2.log"},
	})
	got := r.PerNodeResults()
	if len(got) != 2 {
		t.Fatalf("want 2 results (1 compile + 1 validation), got %d", len(got))
	}
	var compile pipeline.NodeValidationResult
	for _, n := range got {
		if n.Stage == "compile" {
			compile = n
		}
	}
	if compile.DBTLogURI != "s3://c2.log" {
		t.Fatalf("compile result not replaced: %q", compile.DBTLogURI)
	}
}

func TestStartRemediationRound_IncrementsUpToCap(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	r := pipeline.NewCandidate("rel-1", "finance", "abc", false, "o/r", "sha", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToCompiling(now))
	require.NoError(t, r.Fail("compile_failed", "", []string{"finance"}, now))
	require.Equal(t, 1, r.RemediationRound())

	n, err := r.StartRemediationRound(now)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	n, err = r.StartRemediationRound(now)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	_, err = r.StartRemediationRound(now)
	require.ErrorIs(t, err, pipeline.ErrRoundsExhausted)

	last := r.Transitions()[len(r.Transitions())-1]
	require.Equal(t, "remediation_retry", string(last.To))
}

func TestStartRemediationRound_RequiresRejected(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	r := pipeline.NewCandidate("rel-1", "finance", "abc", false, "o/r", "sha", release.ManifestKindDbt, now)
	_, err := r.StartRemediationRound(now)
	require.ErrorIs(t, err, pipeline.ErrNotRejected)
}

func TestRehydrate_CarriesRoundAndPayload(t *testing.T) {
	r := pipeline.Rehydrate(pipeline.RehydrateInput{ID: "rel-1", Kind: pipeline.KindCandidate, Status: pipeline.StatusRejected, RemediationRound: 2, RejectionPayload: []byte(`{"a":1}`)})
	require.Equal(t, 2, r.RemediationRound())
	require.JSONEq(t, `{"a":1}`, string(r.RejectionPayload()))
	require.Equal(t, 1, pipeline.Rehydrate(pipeline.RehydrateInput{ID: "rel-2", Kind: pipeline.KindCandidate, Status: pipeline.StatusRejected}).RemediationRound(), "zero rehydrates as round 1")
}

func TestNewCandidate_BootstrapFlag(t *testing.T) {
	now := time.Unix(100, 0).UTC()

	plain := pipeline.NewCandidate("r1", "svc-a", "sha-a", false, "acme/demo", "deadbeef", release.ManifestKindDbt, now)
	assert.False(t, plain.IsBootstrap())

	boot := pipeline.NewCandidate("r2", "svc-a", "sha-a", true, "acme/demo", "deadbeef", release.ManifestKindDbt, now)
	assert.True(t, boot.IsBootstrap())
}

func TestRehydrate_PreservesBootstrap(t *testing.T) {
	r := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:        "r3",
		Kind:      pipeline.KindCandidate,
		Status:    pipeline.StatusReceived,
		Bootstrap: true,
	})
	assert.True(t, r.IsBootstrap())
}
