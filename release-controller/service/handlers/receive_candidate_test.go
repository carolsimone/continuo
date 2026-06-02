package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReceiveCandidate_PersistsAsReceived(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	input := handlers.ReceiveCandidateInput{
		ReleaseID:    "sha-abc",
		ImageTags:    map[string]string{"service-1": "sha-abc"},
		ManifestsURI: "s3://b/releases/sha-abc/manifests/",
	}
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, input))
	r, err := store.GetRelease("sha-abc")
	require.NoError(t, err)
	assert.Equal(t, release.StatusReceived, r.Status())
}

func TestReceiveCandidate_IsIdempotentOnReleaseID(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	input := handlers.ReceiveCandidateInput{
		ReleaseID:    "sha-abc",
		ImageTags:    map[string]string{"s": "sha-abc"},
		ManifestsURI: "s3://x",
	}
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, input))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, input))
	r, err := store.GetRelease("sha-abc")
	require.NoError(t, err)
	assert.Equal(t, release.StatusReceived, r.Status())
}

func TestReceiveCandidate_RejectsEmptyReleaseID(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	err := handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID:    "",
		ImageTags:    map[string]string{"s": "t"},
		ManifestsURI: "s3://x",
	})
	assert.Error(t, err)
}

func TestReceiveCandidate_PersistsBootstrapFlag(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID:    "rBoot",
		ImageTags:    map[string]string{"svc-a": "sha-a"},
		ManifestsURI: "s3://continuo/releases/rBoot/manifests/",
		Bootstrap:    true,
	}))
	r, err := store.GetRelease("rBoot")
	require.NoError(t, err)
	assert.True(t, r.IsBootstrap())
}
