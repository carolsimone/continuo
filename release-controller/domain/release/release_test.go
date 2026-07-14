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
	r := release.New("sha-abc", "svc1", "sha-abc", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	assert.Equal(t, release.StatusReceived, r.Status())
}

func TestRelease_NewInitialisesImageTags(t *testing.T) {
	r := release.New("sha-abc", "svc1", "sha-abc", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	assert.Equal(t, map[string]string{"svc1": "sha-abc"}, r.ImageTags())
	assert.Equal(t, "svc1", r.ChangedService())
}

func TestRelease_TransitionReceivedToParsing(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	assert.Equal(t, release.StatusParsing, r.Status())
}

func TestRelease_TransitionParsingToValidating(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	assert.Equal(t, release.StatusValidating, r.Status())
	assert.Equal(t, []string{"n"}, r.ValidationNodeIDs())
}

func TestRelease_TransitionValidatingToPromoted(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToPromoted(time.Unix(3, 0)))
	assert.Equal(t, release.StatusPromoted, r.Status())
}

func TestRelease_TransitionValidatingToRejected(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToRejected("validation_failed", []string{"n"}, time.Unix(3, 0)))
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
}

func TestRelease_CannotSkipStates(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	assert.Error(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(1, 0)))
	assert.Error(t, r.TransitionToPromoted(time.Unix(1, 0)))
}

func TestRelease_CannotDoublePromote(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToPromoted(time.Unix(3, 0)))
	assert.Error(t, r.TransitionToPromoted(time.Unix(4, 0)))
}

func TestRelease_TransitionsAreRecorded(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	transitions := r.Transitions()
	require.Len(t, transitions, 3)
	assert.Equal(t, release.StatusReceived, transitions[0].To)
	assert.Equal(t, release.StatusParsing, transitions[1].To)
	assert.Equal(t, release.StatusValidating, transitions[2].To)
}

func TestRelease_SetAssembledImageTags(t *testing.T) {
	r := release.New("sha-abc", "svc-a", "tag-a", false, "acme/demo", "deadbeef", time.Unix(0, 0))
	assembled := map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"}
	r.SetAssembledImageTags(assembled)
	assert.Equal(t, assembled, r.ImageTags())
}

func TestRecordValidationResults_RetainedAcrossReject(t *testing.T) {
	r := release.New("rX", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.Topology{{UniqueID: "a"}}, []string{"a"}, time.Unix(3, 0).UTC()))

	r.RecordValidationResults([]release.NodeValidationResult{
		{NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 12},
	})
	require.NoError(t, r.TransitionToRejected("validation_failed", []string{"a"}, time.Unix(4, 0).UTC()))

	assert.Equal(t, []release.NodeValidationResult{
		{Stage: "validation", NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 12},
	}, r.PerNodeResults())
	assert.Equal(t, release.StatusRejected, r.Status())
}

func TestRecordValidationResults_RetainedAcrossPromote(t *testing.T) {
	r := release.New("rY", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.Topology{{UniqueID: "a"}}, []string{"a"}, time.Unix(3, 0).UTC()))
	r.RecordValidationResults([]release.NodeValidationResult{{NodeID: "a", Status: "ok"}})
	require.NoError(t, r.TransitionToPromoted(time.Unix(4, 0).UTC()))
	assert.Equal(t, []release.NodeValidationResult{{Stage: "validation", NodeID: "a", Status: "ok"}}, r.PerNodeResults())
}

func TestRelease_NewCarriesProvenance(t *testing.T) {
	r := release.New("sha-abc", "svc1", "sha-abc", false, "acme/demo", "deadbeefcafe", time.Unix(0, 0))
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

func TestNode_CandidateSQLURIRoundTrips(t *testing.T) {
	// Node.CandidateSQLURI must round-trip via JSON under the key "candidate_sql_uri".
	n := release.Node{
		UniqueID:        "a",
		CandidateSQLURI: "s3://continuo/svc-a/rA/candidate_a.sql",
	}
	b, err := json.Marshal(n)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	assert.Equal(t, "s3://continuo/svc-a/rA/candidate_a.sql", m["candidate_sql_uri"],
		"JSON key must be candidate_sql_uri")
	_, hasCandidateSQL := m["candidate_sql"]
	assert.False(t, hasCandidateSQL, "old candidate_sql key must not appear")

	var n2 release.Node
	require.NoError(t, json.Unmarshal(b, &n2))
	assert.Equal(t, n.CandidateSQLURI, n2.CandidateSQLURI, "round-trip preserves value")
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

func TestTopology_WithoutCandidateSQLURI_ClearsField(t *testing.T) {
	// WithoutCandidateSQLURI must return a copy with every node's CandidateSQLURI
	// cleared, leaving other fields intact.
	topo := release.Topology{
		{UniqueID: "a", CandidateSQLURI: "s3://continuo/svc-a/rA/a.sql", TableName: "tbl_a"},
		{UniqueID: "b", CandidateSQLURI: "s3://continuo/svc-a/rA/b.sql", TableName: "tbl_b"},
	}
	stripped := topo.WithoutCandidateSQLURI()
	for _, n := range stripped {
		assert.Empty(t, n.CandidateSQLURI, "CandidateSQLURI must be cleared in stripped topology")
	}
	assert.Equal(t, "tbl_a", stripped[0].TableName, "other fields must be preserved")
	// original must be unmodified
	assert.NotEmpty(t, topo[0].CandidateSQLURI, "original topology must not be mutated")
}

var (
	t0 = time.Unix(1, 0)
	t1 = time.Unix(2, 0)
)

func newReceivedRelease(t *testing.T) *release.Release {
	t.Helper()
	return release.New("sha-abc", "svc", "tag", false, "acme/demo", "deadbeef", time.Unix(0, 0))
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
	require.NoError(t, r.TransitionToSeedBuilding(release.Topology{}, nil, t0))
	require.NoError(t, r.TransitionFromSeedBuilding(t1))
	assert.Equal(t, release.StatusValidating, r.Status())
}

func TestTransitionToSeedBuilding_RejectsWrongSource(t *testing.T) {
	r := newReceivedRelease(t) // status=received
	require.Error(t, r.TransitionToSeedBuilding(release.Topology{}, nil, t0))
}

func TestTransitionToRejected_FromSeedBuilding(t *testing.T) {
	r := newParsingRelease(t)
	require.NoError(t, r.TransitionToSeedBuilding(release.Topology{}, nil, t0))
	require.NoError(t, r.TransitionToRejected("seed_build_failed", nil, t1))
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
	require.NoError(t, r.TransitionToRejected("compile_failed", nil, t1))
	assert.Equal(t, release.StatusRejected, r.Status())
}

func TestRecordStageResults_ReplacesPerStage(t *testing.T) {
	r := release.New("rel-1", "core", "tag", false, "o/r", "sha", time.Now())
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
