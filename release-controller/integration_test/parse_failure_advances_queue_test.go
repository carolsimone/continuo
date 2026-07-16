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
// because no `kind:"complete"` validation terminal (on `validation.result:v1`) ever arrives for a rejected release.
func TestIntegration_ParseRejection_AdvancesQueuedRelease(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()

	// Post two candidates.
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "service-1", ReleaseID: "rA", ImageTag: "sha-rA",
		Repo: "acme/demo", CommitSHA: "deadbeefcafe1234",
	}))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "service-1", ReleaseID: "rB", ImageTag: "sha-rB",
		Repo: "acme/demo", CommitSHA: "deadbeefcafe1234",
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
