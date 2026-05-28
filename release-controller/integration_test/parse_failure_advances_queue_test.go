//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression for parse-rejection queue-advance fix: a failed parse must unblock
// the next queued candidate by advancing the queue after the rejection is
// committed. Without this, queued releases stay in StatusReceived indefinitely
// because no validation.completed:v1 ever arrives for a rejected release.
func TestIntegration_ParseRejection_AdvancesQueuedRelease(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()

	// Post two candidates.
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rA", ChangedNodeIDs: []string{"a"}, ImageTags: map[string]string{"service-1": "sha-rA"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rB", ChangedNodeIDs: []string{"a"}, ImageTags: map[string]string{"service-1": "sha-rB"}, ManifestsURI: "u",
	}))

	// Advance: rA becomes Parsing; rB stays Received.
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	// Simulate a failed parse for rA.
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA", Status: "failed", ErrorClass: "ParseError", ErrorDetail: "bad SQL",
	}))

	// The Redis binding calls AdvanceQueue after each parse result; simulate the same.
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	// rA must be Rejected; rB must now be Parsing (queue unblocked).
	rA, _ := deps.NewUoW().ReleaseRepo().Get(context.Background(), "rA")
	rB, _ := deps.NewUoW().ReleaseRepo().Get(context.Background(), "rB")
	assert.Equal(t, release.StatusRejected, rA.Status())
	assert.Equal(t, release.StatusParsing, rB.Status())
}
