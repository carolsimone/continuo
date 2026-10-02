package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validVerification() handlers.ReceiveVerificationInput {
	return handlers.ReceiveVerificationInput{
		RunID: "verify-rel-1-core-a1", Service: "core", ImageTag: "img:1", Kind: "dbt",
		VerifiesReleaseID: "rel-1", Attempt: 1, SourceOverlayURI: "s3://b/core/verify-rel-1-core-a1/source-overlay.tar.gz",
	}
}

func TestReceiveVerification_PersistsAReceivedVerification(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	require.NoError(t, handlers.ReceiveVerification(context.Background(), deps, validVerification()))
	got, err := store.GetRelease("verify-rel-1-core-a1")
	require.NoError(t, err)
	assert.Equal(t, pipeline.KindVerification, got.Kind())
	assert.Equal(t, pipeline.StatusReceived, got.Status())
	assert.Equal(t, "rel-1", got.VerifiesReleaseID())
	assert.Equal(t, 1, got.Attempt())
	assert.Equal(t, release.ManifestKindDbt, got.ManifestKind())
}

func TestReceiveVerification_Validation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, _ := newDeps(now)
	for name, mutate := range map[string]func(*handlers.ReceiveVerificationInput){
		"missing run_id":         func(in *handlers.ReceiveVerificationInput) { in.RunID = "" },
		"missing service":        func(in *handlers.ReceiveVerificationInput) { in.Service = "" },
		"missing image_tag":      func(in *handlers.ReceiveVerificationInput) { in.ImageTag = "" },
		"missing kind":           func(in *handlers.ReceiveVerificationInput) { in.Kind = "" },
		"bad kind":               func(in *handlers.ReceiveVerificationInput) { in.Kind = "yaml" },
		"missing verifies":       func(in *handlers.ReceiveVerificationInput) { in.VerifiesReleaseID = "" },
		"attempt below one":      func(in *handlers.ReceiveVerificationInput) { in.Attempt = 0 },
		"overlay on python kind": func(in *handlers.ReceiveVerificationInput) { in.Kind = "python" },
	} {
		t.Run(name, func(t *testing.T) {
			in := validVerification()
			mutate(&in)
			err := handlers.ReceiveVerification(context.Background(), deps, in)
			require.Error(t, err)
			assert.ErrorIs(t, err, handlers.ErrInvalidInput)
		})
	}
}

func TestReceiveVerification_IdempotentOnRunID_ConflictsWithACandidate(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	require.NoError(t, handlers.ReceiveVerification(context.Background(), deps, validVerification()))
	require.NoError(t, handlers.ReceiveVerification(context.Background(), deps, validVerification()), "a redelivery is accepted")
	store.SeedRelease(pipeline.NewCandidate("rel-9", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, now))
	in := validVerification()
	in.RunID = "rel-9"
	err := handlers.ReceiveVerification(context.Background(), deps, in)
	assert.ErrorIs(t, err, handlers.ErrRunKindConflict)
}

func TestReceiveCandidate_RefusesVerificationFields(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, _ := newDeps(now)
	base := handlers.ReceiveCandidateInput{Service: "core", ReleaseID: "rel-1", ImageTag: "img", Repo: "org/r", CommitSHA: "sha"}
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
			assert.Contains(t, err.Error(), "POST /verification-runs")
		})
	}
}

// Two first submissions of one run id race: this one's Load finds no row, then
// a concurrent submission commits its run before this one's Create. The
// insert-only Create leaves the winner's row untouched.
func TestReceiveVerification_LosingARaceToAVerificationIsANoOp(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	in := validVerification()
	winner := pipeline.NewVerification(in.RunID, in.Service, "img:winner", in.VerifiesReleaseID, in.Attempt, in.SourceOverlayURI, release.ManifestKindDbt, now)
	store.RaceRelease(winner)

	require.NoError(t, handlers.ReceiveVerification(context.Background(), deps, in))

	got, err := store.GetRelease(in.RunID)
	require.NoError(t, err)
	assert.Same(t, winner, got, "the loser must not overwrite the winner's run")
	assert.Equal(t, "img:winner", got.ImageTags()[in.Service])
}

func TestReceiveVerification_LosingARaceToACandidateIsAKindConflict(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	in := validVerification()
	winner := pipeline.NewCandidate(in.RunID, "core", "img:1", false, "org/r", "sha", release.ManifestKindDbt, now)
	store.RaceRelease(winner)

	err := handlers.ReceiveVerification(context.Background(), deps, in)

	assert.ErrorIs(t, err, handlers.ErrRunKindConflict)
	got, gerr := store.GetRelease(in.RunID)
	require.NoError(t, gerr)
	assert.Same(t, winner, got)
}
