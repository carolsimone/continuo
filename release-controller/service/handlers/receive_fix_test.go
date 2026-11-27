package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validFix() handlers.ReceiveFixInput {
	return handlers.ReceiveFixInput{
		CheckID: "verify-rel-1-core-r2-t1", Project: "core", ImageTag: "img:1", Kind: "dbt",
		VerifiesReleaseID: "rel-1", Round: 2, Try: 1, SourceOverlayURI: "s3://b/core/verify-rel-1-core-r2-t1/source-overlay.tar.gz",
	}
}

func TestReceiveFix_PersistsAReceivedFix(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	require.NoError(t, handlers.ReceiveFix(context.Background(), deps, validFix()))
	got, err := store.GetRelease("verify-rel-1-core-r2-t1")
	require.NoError(t, err)
	assert.Equal(t, releasecheck.KindFix, got.Kind())
	assert.Equal(t, releasecheck.StatusReceived, got.Status())
	assert.Equal(t, "rel-1", got.VerifiesReleaseID())
	assert.Equal(t, 2, got.RemediationRound())
	assert.Equal(t, 1, got.Try())
	assert.Equal(t, release.ManifestKindDbt, got.ManifestKind())
}

func TestReceiveFix_Validation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, _ := newDeps(now)
	for name, mutate := range map[string]func(*handlers.ReceiveFixInput){
		"missing check_id":  func(in *handlers.ReceiveFixInput) { in.CheckID = "" },
		"missing project":   func(in *handlers.ReceiveFixInput) { in.Project = "" },
		"missing image_tag": func(in *handlers.ReceiveFixInput) { in.ImageTag = "" },
		"missing kind":      func(in *handlers.ReceiveFixInput) { in.Kind = "" },
		"bad kind":          func(in *handlers.ReceiveFixInput) { in.Kind = "yaml" },
		"missing verifies":  func(in *handlers.ReceiveFixInput) { in.VerifiesReleaseID = "" },
		"try below one":     func(in *handlers.ReceiveFixInput) { in.Try = 0 },
		"round below one":   func(in *handlers.ReceiveFixInput) { in.Round = 0 },
		"overlay on python": func(in *handlers.ReceiveFixInput) { in.Kind = "python" },
	} {
		t.Run(name, func(t *testing.T) {
			in := validFix()
			mutate(&in)
			err := handlers.ReceiveFix(context.Background(), deps, in)
			require.Error(t, err)
			assert.ErrorIs(t, err, handlers.ErrInvalidInput)
		})
	}
}

func TestReceiveFix_NamesTheTryAndRoundItRefuses(t *testing.T) {
	deps, _ := newDeps(time.Unix(1_700_000_000, 0).UTC())
	for want, mutate := range map[string]func(*handlers.ReceiveFixInput){
		"try must be at least 1":               func(in *handlers.ReceiveFixInput) { in.Try = 0 },
		"remediation_round must be at least 1": func(in *handlers.ReceiveFixInput) { in.Round = 0 },
	} {
		t.Run(want, func(t *testing.T) {
			in := validFix()
			mutate(&in)
			err := handlers.ReceiveFix(context.Background(), deps, in)
			require.ErrorIs(t, err, handlers.ErrInvalidInput)
			assert.Contains(t, err.Error(), want)
		})
	}
}

func TestReceiveFix_AcceptsPython(t *testing.T) {
	deps, store := newDeps(time.Unix(1_700_000_000, 0).UTC())
	in := validFix()
	in.Kind = "python"
	in.SourceOverlayURI = ""
	require.NoError(t, handlers.ReceiveFix(context.Background(), deps, in))

	got, err := store.GetRelease(in.CheckID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, release.ManifestKindPython, got.ManifestKind())
	assert.Empty(t, got.SourceOverlayURI())
}

func TestReceiveFix_RefusesASourceOverlayForPython(t *testing.T) {
	deps, store := newDeps(time.Unix(1_700_000_000, 0).UTC())
	in := validFix()
	in.Kind = "python" // validFix carries a source overlay
	err := handlers.ReceiveFix(context.Background(), deps, in)
	require.Error(t, err)
	assert.ErrorIs(t, err, handlers.ErrInvalidInput)
	assert.Contains(t, err.Error(), "source_overlay_uri")
	_, getErr := store.GetRelease(in.CheckID)
	assert.Error(t, getErr, "a refused fix's release check stores nothing")
}

func TestReceiveFix_IdempotentOnCheckID_ConflictsWithACandidate(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	require.NoError(t, handlers.ReceiveFix(context.Background(), deps, validFix()))
	require.NoError(t, handlers.ReceiveFix(context.Background(), deps, validFix()), "a redelivery is accepted")
	store.SeedRelease(releasecheck.NewCandidate("rel-9", "core", "img", false, false, "org/r", "sha", release.ManifestKindDbt, now))
	in := validFix()
	in.CheckID = "rel-9"
	err := handlers.ReceiveFix(context.Background(), deps, in)
	assert.ErrorIs(t, err, handlers.ErrCheckKindConflict)
}

func TestReceiveCandidate_RefusesFixFields(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, _ := newDeps(now)
	base := handlers.ReceiveCandidateInput{Project: "core", ReleaseID: "rel-1", ImageTag: "img", Repo: "org/r", CommitSHA: "sha"}
	for name, mutate := range map[string]func(*handlers.ReceiveCandidateInput){
		"shadow":              func(in *handlers.ReceiveCandidateInput) { in.Shadow = true },
		"source_overlay_uri":  func(in *handlers.ReceiveCandidateInput) { in.SourceOverlayURI = "s3://x" },
		"verifies_release_id": func(in *handlers.ReceiveCandidateInput) { in.VerifiesReleaseID = "rel-0" },
	} {
		t.Run(name, func(t *testing.T) {
			in := base
			mutate(&in)
			err := handlers.ReceiveCandidate(context.Background(), deps, in)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "POST /release-checks")
		})
	}
}

// Two first submissions of one check id race: this one's Load finds no row, then
// a concurrent submission commits its check before this one's Create. The
// insert-only Create leaves the winner's row untouched.
func TestReceiveFix_LosingARaceToAFixIsANoOp(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	in := validFix()
	winner := releasecheck.NewFix(in.CheckID, in.Project, "img:winner", in.VerifiesReleaseID, in.Round, in.Try, in.SourceOverlayURI, release.ManifestKindDbt, now)
	store.RaceRelease(winner)

	require.NoError(t, handlers.ReceiveFix(context.Background(), deps, in))

	got, err := store.GetRelease(in.CheckID)
	require.NoError(t, err)
	assert.Same(t, winner, got, "the loser must not overwrite the winner's check")
	assert.Equal(t, "img:winner", got.ImageTags()[in.Project])
}

func TestReceiveFix_LosingARaceToACandidateIsAKindConflict(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	in := validFix()
	winner := releasecheck.NewCandidate(in.CheckID, "core", "img:1", false, false, "org/r", "sha", release.ManifestKindDbt, now)
	store.RaceRelease(winner)

	err := handlers.ReceiveFix(context.Background(), deps, in)

	assert.ErrorIs(t, err, handlers.ErrCheckKindConflict)
	got, gerr := store.GetRelease(in.CheckID)
	require.NoError(t, gerr)
	assert.Same(t, winner, got)
}
