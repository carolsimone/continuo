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

func TestReceiveCandidate_PersistsAsReceived(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	input := handlers.ReceiveCandidateInput{
		Service:   "service-1",
		ReleaseID: "sha-abc",
		ImageTag:  "sha-abc",
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
	}
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, input))
	r, err := store.GetRelease("sha-abc")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusReceived, r.Status())
	assert.Equal(t, "service-1", r.ChangedService())
	assert.Equal(t, map[string]string{"service-1": "sha-abc"}, r.ImageTags())
}

func TestReceiveCandidate_IsIdempotentOnReleaseID(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	input := handlers.ReceiveCandidateInput{
		Service:   "svc",
		ReleaseID: "sha-abc",
		ImageTag:  "sha-abc",
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
	}
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, input))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, input))
	r, err := store.GetRelease("sha-abc")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusReceived, r.Status())
}

func TestReceiveCandidate_RejectsEmptyReleaseID(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc",
		ReleaseID: "",
		ImageTag:  "t",
	})
	assert.Error(t, err)
}

func TestReceiveCandidate_RejectsEmptyService(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "",
		ReleaseID: "sha-abc",
		ImageTag:  "t",
	})
	assert.Error(t, err)
}

func TestReceiveCandidate_RejectsEmptyImageTag(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc",
		ReleaseID: "sha-abc",
		ImageTag:  "",
	})
	assert.Error(t, err)
}

func TestReceiveCandidate_PersistsBootstrapFlag(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc-a",
		ReleaseID: "rBoot",
		ImageTag:  "sha-a",
		Bootstrap: true,
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
	}))
	r, err := store.GetRelease("rBoot")
	require.NoError(t, err)
	assert.True(t, r.IsBootstrap())
}

func TestReceiveCandidate_RejectsEmptyRepo(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc",
		ReleaseID: "sha-abc",
		ImageTag:  "t",
		Repo:      "",
		CommitSHA: "deadbeef",
	})
	assert.Error(t, err)
}

func TestReceiveCandidate_RejectsEmptyCommitSHA(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc",
		ReleaseID: "sha-abc",
		ImageTag:  "t",
		Repo:      "acme/demo",
		CommitSHA: "",
	})
	assert.Error(t, err)
}

func TestReceiveCandidate_DefaultsAbsentKindToDbt(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rK1", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	r, err := store.GetRelease("rK1")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindDbt, r.ManifestKind())
}

func TestReceiveCandidate_PersistsPythonKind(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rK2", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
		Kind: "python",
	}))
	r, err := store.GetRelease("rK2")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindPython, r.ManifestKind())
}

func TestReceiveCandidate_RejectsUnknownKind(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rK3", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
		Kind: "r",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown manifest kind")
	r, _ := store.GetRelease("rK3")
	assert.Nil(t, r, "an invalid kind must not persist a release")
}

func TestReceiveCandidate_ConflictsWithAVerification(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	store.SeedRelease(pipeline.NewVerification("run-9", "core", "img", "rel-0", 1, "", release.ManifestKindDbt, now))
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "core", ReleaseID: "run-9", ImageTag: "img", Repo: "acme/demo", CommitSHA: "deadbeef",
	})
	assert.ErrorIs(t, err, handlers.ErrRunKindConflict)
}

func TestReceiveCandidate_ValidationErrorsWrapErrInvalidCandidate(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	input := handlers.ReceiveCandidateInput{Service: "core", ReleaseID: "rel-1", Repo: "o/r", CommitSHA: "sha"}
	err := handlers.ReceiveCandidate(context.Background(), deps, input)
	require.ErrorIs(t, err, handlers.ErrInvalidCandidate)
	require.ErrorContains(t, err, "image_tag is required")

	input.ImageTag = "img"
	input.Kind = "yaml"
	require.ErrorIs(t, handlers.ReceiveCandidate(context.Background(), deps, input), handlers.ErrInvalidCandidate)
}

func conflictBase() handlers.ReceiveCandidateInput {
	return handlers.ReceiveCandidateInput{
		Service: "core", ReleaseID: "rel-1", ImageTag: "img:1", Repo: "acme/demo", CommitSHA: "deadbeef",
	}
}

// A release id that already names a candidate is a conflict when any fact of
// the new submission differs, and the stored candidate is left as it was.
func TestReceiveCandidate_ConflictingResubmitWrapsErrReleaseIDConflict(t *testing.T) {
	for name, mutate := range map[string]func(*handlers.ReceiveCandidateInput){
		"service":    func(in *handlers.ReceiveCandidateInput) { in.Service = "billing" },
		"image tag":  func(in *handlers.ReceiveCandidateInput) { in.ImageTag = "img:2" },
		"kind":       func(in *handlers.ReceiveCandidateInput) { in.Kind = "python" },
		"bootstrap":  func(in *handlers.ReceiveCandidateInput) { in.Bootstrap = true },
		"repo":       func(in *handlers.ReceiveCandidateInput) { in.Repo = "acme/fork" },
		"commit sha": func(in *handlers.ReceiveCandidateInput) { in.CommitSHA = "cafebabe" },
	} {
		t.Run(name, func(t *testing.T) {
			deps, store := newDeps(time.Unix(100, 0).UTC())
			require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, conflictBase()))
			in := conflictBase()
			mutate(&in)
			err := handlers.ReceiveCandidate(context.Background(), deps, in)
			require.ErrorIs(t, err, handlers.ErrReleaseIDConflict)
			assert.ErrorContains(t, err, `"rel-1"`)
			r, gerr := store.GetRelease("rel-1")
			require.NoError(t, gerr)
			assert.Equal(t, "core", r.ChangedService())
			assert.Equal(t, "img:1", r.ImageTags()["core"])
		})
	}
}

// An identical resubmit of a candidate that has already left the queue, and
// whose image-tag map activation has filled in, is still a no-op.
func TestReceiveCandidate_IdenticalResubmitOfAnAdvancedCandidateIsANoOp(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	r := pipeline.NewCandidate("rel-1", "core", "img:1", false, "acme/demo", "deadbeef", release.ManifestKindDbt, now)
	r.SetAssembledImageTags(map[string]string{"core": "img:1", "billing": "img:9"})
	require.NoError(t, r.TransitionToParsing(now))
	store.SeedRelease(r)

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, conflictBase()))
	got, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusParsing, got.Status())
}

// "dbt" spelled out names the same kind as an absent kind, so it is not a
// conflict.
func TestReceiveCandidate_ExplicitDbtKindMatchesAnAbsentKind(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, conflictBase()))
	in := conflictBase()
	in.Kind = "dbt"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, in))
}

// Two first submissions of one id race: this one's Load finds no row, then a
// concurrent submission commits its run before this one's Create. The insert
// is refused, and the submission is judged against the winner's run instead of
// overwriting it.
func TestReceiveCandidate_LosingARaceToADifferentCandidateIsAConflict(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	store.RaceRelease(pipeline.NewCandidate("rel-1", "core", "img:winner", false, "acme/demo", "deadbeef", release.ManifestKindDbt, now))

	in := conflictBase()
	in.ImageTag = "img:loser"
	err := handlers.ReceiveCandidate(context.Background(), deps, in)

	require.ErrorIs(t, err, handlers.ErrReleaseIDConflict)
	r, gerr := store.GetRelease("rel-1")
	require.NoError(t, gerr)
	assert.Equal(t, "img:winner", r.ImageTags()["core"])
}

func TestReceiveCandidate_LosingARaceToTheSameCandidateIsANoOp(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	winner := pipeline.NewCandidate("rel-1", "core", "img:1", false, "acme/demo", "deadbeef", release.ManifestKindDbt, now)
	store.RaceRelease(winner)

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, conflictBase()))
	r, err := store.GetRelease("rel-1")
	require.NoError(t, err)
	assert.Same(t, winner, r)
}

func TestReceiveCandidate_LosingARaceToAVerificationIsAKindConflict(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, store := newDeps(now)
	store.RaceRelease(pipeline.NewVerification("rel-1", "core", "img:1", "rel-0", 1, "", release.ManifestKindDbt, now))
	assert.ErrorIs(t, handlers.ReceiveCandidate(context.Background(), deps, conflictBase()), handlers.ErrRunKindConflict)
}
