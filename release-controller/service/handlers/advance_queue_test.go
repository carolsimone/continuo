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
	deps, u := newDeps(time.Unix(200, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rA", ChangedNodeIDs: []string{"n"}, ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	r, _ := u.ReleaseRepo().Get(context.Background(), "rA")
	assert.Equal(t, release.StatusParsing, r.Status())
	entries := outboxEntries(u)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))
	assert.Equal(t, "rA", payload["release_id"])
	assert.Equal(t, "u", payload["manifests_uri"])
}

func TestAdvanceQueue_ActiveExists_DoesNothing(t *testing.T) {
	deps, u := newDeps(time.Unix(200, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rA", ChangedNodeIDs: []string{"n"}, ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rB", ChangedNodeIDs: []string{"n"}, ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u2",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rB, _ := u.ReleaseRepo().Get(context.Background(), "rB")
	assert.Equal(t, release.StatusReceived, rB.Status())
	assert.Len(t, outboxEntries(u), 1) // only rA emitted
}

func TestAdvanceQueue_NoQueued_DoesNothing(t *testing.T) {
	deps, u := newDeps(time.Unix(200, 0).UTC())
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	assert.Empty(t, outboxEntries(u))
}

func TestAdvanceQueue_PicksOldestFirst(t *testing.T) {
	deps, u := newDeps(time.Unix(200, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rOLD", ChangedNodeIDs: []string{"n"}, ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rNEW", ChangedNodeIDs: []string{"n"}, ImageTags: map[string]string{"s": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rOLD, _ := u.ReleaseRepo().Get(context.Background(), "rOLD")
	rNEW, _ := u.ReleaseRepo().Get(context.Background(), "rNEW")
	assert.Equal(t, release.StatusParsing, rOLD.Status())
	assert.Equal(t, release.StatusReceived, rNEW.Status())
}
