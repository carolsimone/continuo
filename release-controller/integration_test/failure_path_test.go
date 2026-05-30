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

func TestIntegration_FailedValidationKeepsCurrentProdUnchanged(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()

	// Seed candidate through happy stages up to Validating
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rFAIL", ImageTags: map[string]string{"service-1": "sha-rFAIL"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rFAIL", Status: "ok",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "service-1"},
			{UniqueID: "b", ServiceName: "service-1", UpstreamUniqueIDs: []string{"a"}},
		},
	}))

	// Validation fails on node b
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rFAIL",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/b"},
		},
		AggregateStatus: "partial_failed",
	}))

	r, _ := deps.NewUoW().ReleaseRepo().Get(context.Background(), "rFAIL")
	assert.Equal(t, release.StatusRejected, r.Status())

	cp, _ := deps.NewUoW().CurrentProdRepo().Get(context.Background())
	assert.Empty(t, cp.ReleaseID(), "current_prod must remain unchanged on validation failure")
}
