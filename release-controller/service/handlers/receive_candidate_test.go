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
