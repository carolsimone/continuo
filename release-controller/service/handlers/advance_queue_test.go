package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdvanceQueue_NoActive_PromotesNextReceivedToParsing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rA", ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	r, _ := store.GetRelease("rA")
	assert.Equal(t, release.StatusParsing, r.Status())
	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))
	assert.Equal(t, "rA", payload["release_id"])
	assert.Equal(t, "u", payload["manifests_uri"])
}

func TestAdvanceQueue_ActiveExists_DoesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rA", ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rB", ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u2",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rB, _ := store.GetRelease("rB")
	assert.Equal(t, release.StatusReceived, rB.Status())
	assert.Len(t, outboxEntries(store), 1) // only rA emitted
}

func TestAdvanceQueue_NoQueued_DoesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	assert.Empty(t, outboxEntries(store))
}

func TestAdvanceQueue_PicksOldestFirst(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rOLD", ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rNEW", ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rOLD, _ := store.GetRelease("rOLD")
	rNEW, _ := store.GetRelease("rNEW")
	assert.Equal(t, release.StatusParsing, rOLD.Status())
	assert.Equal(t, release.StatusReceived, rNEW.Status())
}
