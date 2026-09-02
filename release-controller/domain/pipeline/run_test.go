package pipeline_test

import (
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)

func candidate(t *testing.T) *pipeline.Run {
	t.Helper()
	return pipeline.NewCandidate("rel-1", "core", "img:1", false, "org/repo", "abc123", release.ManifestKindDbt, t0)
}

func verification(t *testing.T) *pipeline.Run {
	t.Helper()
	return pipeline.NewVerification("verify-rel-1-core-a1", "core", "img:1", "rel-1", 1, "s3://b/core/verify-rel-1-core-a1/source-overlay.tar.gz", release.ManifestKindDbt, t0)
}

// validating drives r from received to validating with an empty topology, the
// shortest legal path to the state Promote and Pass require.
func validating(t *testing.T, r *pipeline.Run) {
	t.Helper()
	require.NoError(t, r.TransitionToParsing(t0))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, nil, t0))
}

func TestNewCandidate_HasNoVerificationFacts(t *testing.T) {
	r := candidate(t)
	assert.Equal(t, pipeline.KindCandidate, r.Kind())
	require.NotNil(t, r.Candidate())
	assert.Nil(t, r.Verification())
	assert.Equal(t, "org/repo", r.Repo())
	assert.Equal(t, 1, r.RemediationRound())
	assert.Equal(t, "", r.VerifiesReleaseID())
	assert.Equal(t, 0, r.Attempt())
	assert.Equal(t, "", r.SourceOverlayURI())
}

func TestNewVerification_HasNoCandidateFacts(t *testing.T) {
	r := verification(t)
	assert.Equal(t, pipeline.KindVerification, r.Kind())
	require.NotNil(t, r.Verification())
	assert.Nil(t, r.Candidate())
	assert.Equal(t, "rel-1", r.VerifiesReleaseID())
	assert.Equal(t, 1, r.Attempt())
	assert.Equal(t, "", r.Repo())
	assert.False(t, r.IsBootstrap())
	assert.Nil(t, r.RejectionPayload())
}

func TestPromote_RefusesVerification(t *testing.T) {
	r := verification(t)
	validating(t, r)
	err := r.Promote(t0)
	require.True(t, errors.Is(err, pipeline.ErrWrongKind), "got %v", err)
	assert.Equal(t, pipeline.StatusValidating, r.Status())
}

func TestPass_RefusesCandidate(t *testing.T) {
	r := candidate(t)
	validating(t, r)
	err := r.Pass(t0)
	require.True(t, errors.Is(err, pipeline.ErrWrongKind), "got %v", err)
	assert.Equal(t, pipeline.StatusValidating, r.Status())
}

func TestPass_EndsVerificationPassed(t *testing.T) {
	r := verification(t)
	validating(t, r)
	require.NoError(t, r.Pass(t0.Add(time.Minute)))
	assert.Equal(t, pipeline.StatusPassed, r.Status())
	finished, ok := r.FinishedAt()
	require.True(t, ok)
	assert.Equal(t, t0.Add(time.Minute), finished)
}

func TestFail_LandsOnRejectedForCandidate(t *testing.T) {
	r := candidate(t)
	require.NoError(t, r.Fail("compile_failed", "boom", []string{"core"}, t0))
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "compile_failed", r.FailReason())
	assert.Equal(t, []string{"core"}, r.FailingNodes())
}

func TestFail_LandsOnFailedForVerification(t *testing.T) {
	r := verification(t)
	require.NoError(t, r.Fail("validation_failed", "", []string{"model.core.orders"}, t0))
	assert.Equal(t, pipeline.StatusFailed, r.Status())
	assert.True(t, r.Status().IsTerminal())
}

func TestStartRemediationRound_RefusesVerification(t *testing.T) {
	r := verification(t)
	require.NoError(t, r.Fail("validation_failed", "", nil, t0))
	_, err := r.StartRemediationRound(t0)
	require.True(t, errors.Is(err, pipeline.ErrWrongKind), "got %v", err)
}

func TestSetRejectionPayload_IsIgnoredOnVerification(t *testing.T) {
	r := verification(t)
	r.SetRejectionPayload([]byte(`{}`))
	assert.Nil(t, r.RejectionPayload())
}

func TestActivatedAt_IsFirstTransitionOffReceived(t *testing.T) {
	r := candidate(t)
	_, ok := r.ActivatedAt()
	assert.False(t, ok, "a queued run has not activated")
	require.NoError(t, r.TransitionToCompiling(t0.Add(30*time.Second)))
	at, ok := r.ActivatedAt()
	require.True(t, ok)
	assert.Equal(t, t0.Add(30*time.Second), at)
}

func TestRehydrate_BuildsTheRightKind(t *testing.T) {
	v := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID: "verify-x", Kind: pipeline.KindVerification, Status: pipeline.StatusPassed,
		ImageTags: map[string]string{"core": "img"}, ChangedService: "core",
		CreatedAt: t0, ManifestKind: release.ManifestKindDbt,
		VerifiesReleaseID: "rel-1", Attempt: 2, SourceOverlayURI: "s3://o",
	})
	require.NotNil(t, v.Verification())
	assert.Nil(t, v.Candidate())
	assert.Equal(t, 2, v.Attempt())

	c := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID: "rel-1", Kind: pipeline.KindCandidate, Status: pipeline.StatusRejected,
		ImageTags: map[string]string{"core": "img"}, ChangedService: "core",
		CreatedAt: t0, ManifestKind: release.ManifestKindDbt, Repo: "org/repo", CommitSHA: "abc",
	})
	require.NotNil(t, c.Candidate())
	assert.Equal(t, 1, c.RemediationRound(), "a stored round below 1 reads as round 1")
}
