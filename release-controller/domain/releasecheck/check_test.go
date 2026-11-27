package releasecheck_test

import (
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)

func candidate(t *testing.T) *releasecheck.Check {
	t.Helper()
	return releasecheck.NewCandidate("rel-1", "core", "img:1", false, false, "org/repo", "abc123", release.ManifestKindDbt, t0)
}

func fix(t *testing.T) *releasecheck.Check {
	t.Helper()
	return releasecheck.NewFix("verify-rel-1-core-r2-t1", "core", "img:1", "rel-1", 2, 1, "s3://b/core/verify-rel-1-core-r2-t1/source-overlay.tar.gz", release.ManifestKindDbt, t0)
}

// validating drives r from received to validating with an empty topology, the
// shortest legal path to the state Promote and Pass require.
func validating(t *testing.T, r *releasecheck.Check) {
	t.Helper()
	require.NoError(t, r.TransitionToParsing(t0))
	require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, t0))
}

func TestNewCandidate_HasNoFixFacts(t *testing.T) {
	r := candidate(t)
	assert.Equal(t, releasecheck.KindCandidate, r.Kind())
	require.NotNil(t, r.Candidate())
	assert.Nil(t, r.Fix())
	assert.Equal(t, "org/repo", r.Repo())
	assert.Equal(t, 1, r.Round())
	assert.Equal(t, "", r.VerifiesReleaseID())
	assert.Equal(t, 0, r.Try())
	assert.Equal(t, "", r.SourceOverlayURI())
}

func TestNewFix_HasNoCandidateFacts(t *testing.T) {
	r := fix(t)
	assert.Equal(t, releasecheck.KindFix, r.Kind())
	require.NotNil(t, r.Fix())
	assert.Nil(t, r.Candidate())
	assert.Equal(t, "rel-1", r.VerifiesReleaseID())
	assert.Equal(t, 2, r.Round(), "a fix's round is the round of the try it proves")
	assert.Equal(t, 1, r.Try())
	assert.Equal(t, "", r.Repo())
	assert.False(t, r.IsBootstrap())
	assert.Nil(t, r.RejectionPayload())
}

func TestPromote_RefusesFix(t *testing.T) {
	r := fix(t)
	validating(t, r)
	err := r.Promote(t0)
	require.True(t, errors.Is(err, releasecheck.ErrWrongKind), "got %v", err)
	assert.Equal(t, releasecheck.StatusValidating, r.Status())
}

func TestPass_RefusesCandidate(t *testing.T) {
	r := candidate(t)
	validating(t, r)
	err := r.Pass(t0)
	require.True(t, errors.Is(err, releasecheck.ErrWrongKind), "got %v", err)
	assert.Equal(t, releasecheck.StatusValidating, r.Status())
}

func TestPass_EndsFixPassed(t *testing.T) {
	r := fix(t)
	validating(t, r)
	require.NoError(t, r.Pass(t0.Add(time.Minute)))
	assert.Equal(t, releasecheck.StatusPassed, r.Status())
	finished, ok := r.FinishedAt()
	require.True(t, ok)
	assert.Equal(t, t0.Add(time.Minute), finished)
}

func TestFail_LandsOnRejectedForCandidate(t *testing.T) {
	r := candidate(t)
	require.NoError(t, r.Fail("compile_failed", "boom", []string{"core"}, t0))
	assert.Equal(t, releasecheck.StatusRejected, r.Status())
	assert.Equal(t, "compile_failed", r.FailReason())
	assert.Equal(t, []string{"core"}, r.FailingNodes())
}

func TestFail_LandsOnFailedForFix(t *testing.T) {
	r := fix(t)
	require.NoError(t, r.Fail("validation_failed", "", []string{"model.core.orders"}, t0))
	assert.Equal(t, releasecheck.StatusFailed, r.Status())
	assert.True(t, r.Status().IsTerminal())
}

func TestStartRound_RefusesFix(t *testing.T) {
	r := fix(t)
	require.NoError(t, r.Fail("validation_failed", "", nil, t0))
	_, err := r.StartRound(t0)
	require.True(t, errors.Is(err, releasecheck.ErrWrongKind), "got %v", err)
}

func TestSetRejectionPayload_IsIgnoredOnFix(t *testing.T) {
	r := fix(t)
	r.SetRejectionPayload([]byte(`{}`))
	assert.Nil(t, r.RejectionPayload())
}

func TestActivatedAt_IsFirstTransitionOffReceived(t *testing.T) {
	r := candidate(t)
	_, ok := r.ActivatedAt()
	assert.False(t, ok, "a queued check has not activated")
	require.NoError(t, r.TransitionToCompiling(t0.Add(30*time.Second)))
	at, ok := r.ActivatedAt()
	require.True(t, ok)
	assert.Equal(t, t0.Add(30*time.Second), at)
}

func TestCheck_StageDurationIsTheTimeUntilTheNextTransition(t *testing.T) {
	r := candidate(t)
	require.NoError(t, r.TransitionToCompiling(t0.Add(1*time.Second)))
	require.NoError(t, r.TransitionFromCompiling(t0.Add(31*time.Second)))
	require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, t0.Add(40*time.Second)))
	require.NoError(t, r.Promote(t0.Add(100*time.Second)))
	assert.Equal(t, 30*time.Second, r.StageDuration(releasecheck.StatusCompiling))
	assert.Equal(t, 9*time.Second, r.StageDuration(releasecheck.StatusParsing))
	assert.Equal(t, 60*time.Second, r.StageDuration(releasecheck.StatusValidating))
	assert.Equal(t, time.Duration(0), r.StageDuration(releasecheck.StatusSeedBuilding))
}

func TestCheck_StageDurationOfTheStageInProgressIsZero(t *testing.T) {
	r := candidate(t)
	require.NoError(t, r.TransitionToParsing(t0.Add(5*time.Second)))
	assert.Equal(t, time.Duration(0), r.StageDuration(releasecheck.StatusParsing))
}

func TestRehydrate_BuildsTheRightKind(t *testing.T) {
	v := releasecheck.Rehydrate(releasecheck.RehydrateInput{
		ID: "verify-x", Kind: releasecheck.KindFix, Status: releasecheck.StatusPassed,
		ImageTags: map[string]string{"core": "img"}, ChangedProject: "core",
		CreatedAt: t0, ManifestKind: release.ManifestKindDbt,
		VerifiesReleaseID: "rel-1", Round: 3, Try: 2, SourceOverlayURI: "s3://o",
	})
	require.NotNil(t, v.Fix())
	assert.Nil(t, v.Candidate())
	assert.Equal(t, 2, v.Try())
	assert.Equal(t, 3, v.Round(), "a fix's round rehydrates from the round column")

	c := releasecheck.Rehydrate(releasecheck.RehydrateInput{
		ID: "rel-1", Kind: releasecheck.KindCandidate, Status: releasecheck.StatusRejected,
		ImageTags: map[string]string{"core": "img"}, ChangedProject: "core",
		CreatedAt: t0, ManifestKind: release.ManifestKindDbt, Repo: "org/repo", CommitSHA: "abc",
	})
	require.NotNil(t, c.Candidate())
	assert.Equal(t, 1, c.Round(), "a stored round below 1 reads as round 1")
}

func candidateSubmission() releasecheck.CandidateSubmission {
	return releasecheck.CandidateSubmission{
		Project: "core", ImageTag: "img:1", Kind: release.ManifestKindDbt,
		Bootstrap: false, Repo: "org/repo", CommitSHA: "abc123",
	}
}

func TestMatchesSubmission_SameFactsMatch(t *testing.T) {
	assert.True(t, candidate(t).MatchesSubmission(candidateSubmission()))
}

func TestMatchesSubmission_AnyDifferingFactDoesNotMatch(t *testing.T) {
	for name, mutate := range map[string]func(*releasecheck.CandidateSubmission){
		"project":    func(s *releasecheck.CandidateSubmission) { s.Project = "other" },
		"image tag":  func(s *releasecheck.CandidateSubmission) { s.ImageTag = "img:2" },
		"kind":       func(s *releasecheck.CandidateSubmission) { s.Kind = release.ManifestKindPython },
		"bootstrap":  func(s *releasecheck.CandidateSubmission) { s.Bootstrap = true },
		"repo":       func(s *releasecheck.CandidateSubmission) { s.Repo = "org/fork" },
		"commit sha": func(s *releasecheck.CandidateSubmission) { s.CommitSHA = "def456" },
	} {
		t.Run(name, func(t *testing.T) {
			s := candidateSubmission()
			mutate(&s)
			assert.False(t, candidate(t).MatchesSubmission(s))
		})
	}
}

// Activation replaces the image-tag map with every project's tag; the
// candidate still matches on the tag it holds for its own project.
func TestMatchesSubmission_HoldsAfterActivationAssemblesImageTags(t *testing.T) {
	r := candidate(t)
	r.SetAssembledImageTags(map[string]string{"core": "img:1", "billing": "img:9"})
	require.NoError(t, r.TransitionToParsing(t0))
	assert.True(t, r.MatchesSubmission(candidateSubmission()))
}

func TestMatchesSubmission_AFixMatchesNoSubmission(t *testing.T) {
	assert.False(t, fix(t).MatchesSubmission(candidateSubmission()))
}

func TestCheck_CandidateTopologyRefRehydratesAndIsRecordedByTheTransition(t *testing.T) {
	ref := release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", SHA256: "ab12", NodeCount: 7}
	r := releasecheck.Rehydrate(releasecheck.RehydrateInput{ID: "r1", Kind: releasecheck.KindCandidate, Status: releasecheck.StatusValidating, CandidateTopology: ref})
	assert.Equal(t, ref, r.CandidateTopologyRef())

	fresh := candidate(t)
	assert.True(t, fresh.CandidateTopologyRef().IsZero(), "a check that was never parsed names no artifact")
	require.NoError(t, fresh.TransitionToParsing(t0))
	require.NoError(t, fresh.TransitionToValidating(ref, []string{"n"}, t0))
	assert.Equal(t, ref, fresh.CandidateTopologyRef())
}

func TestMatchesSubmission_ComparesRebaseline(t *testing.T) {
	r := releasecheck.NewCandidate("rel-1", "core", "abc", true, true, "o/r", "sha", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	same := releasecheck.CandidateSubmission{Project: "core", ImageTag: "abc", Kind: release.ManifestKindDbt, Bootstrap: true, Rebaseline: true, Repo: "o/r", CommitSHA: "sha"}
	assert.True(t, r.MatchesSubmission(same))
	other := same
	other.Rebaseline = false
	assert.False(t, r.MatchesSubmission(other))
}

func TestCheck_NoticesRoundTripAndTakeTheWarehouseFindings(t *testing.T) {
	r := releasecheck.NewCandidate("rel-n", "finance", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	assert.Empty(t, r.Notices())
	r.SetNotices([]release.Notice{
		{Kind: "seed_reload", NodeID: "analytics.seed_a", EngineFamily: warehouse.SchemaBound, Prediction: "reloads"},
		{Kind: "seed_reload", NodeID: "analytics.seed_b", EngineFamily: warehouse.SchemaBound, Prediction: "reloads"},
	})

	outside := seedreload.Findings{Checked: true, Completeness: "exact",
		Dependents: []seedreload.Dependent{{Schema: "bi", Name: "report", Kind: "view", Owner: "bi", Depth: 1}}}
	assert.True(t, r.ApplyWarehouseFindings("analytics.seed_b", outside))
	assert.False(t, r.ApplyWarehouseFindings("analytics.seed_none", outside))

	got := r.Notices()
	require.Len(t, got, 2)
	assert.Equal(t, "reloads", got[0].Prediction)
	assert.Equal(t, "stops", got[1].Prediction)
	assert.Equal(t, []release.OutsideObject{{Object: "bi.report", Kind: "view", Owner: "bi"}}, got[1].Warehouse.OutsideContinuo)

	rehydrated := releasecheck.Rehydrate(releasecheck.RehydrateInput{ID: "rel-n", Kind: releasecheck.KindCandidate, Notices: got})
	assert.Equal(t, got, rehydrated.Notices())
}

// SetNotices keeps its own copy, so findings applied to the check never change
// the caller's slice.
func TestCheck_SetNoticesCopiesTheSlice(t *testing.T) {
	r := releasecheck.NewCandidate("rel-c", "finance", "tag", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	in := []release.Notice{{Kind: "seed_reload", NodeID: "analytics.seed_a", EngineFamily: warehouse.SchemaBound, Prediction: "reloads"}}
	r.SetNotices(in)
	require.True(t, r.ApplyWarehouseFindings("analytics.seed_a", seedreload.Findings{Checked: true, Completeness: "none"}))
	assert.Equal(t, "stops", r.Notices()[0].Prediction)
	assert.Equal(t, "reloads", in[0].Prediction)
}
