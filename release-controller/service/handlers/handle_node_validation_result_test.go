package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleNodeValidationResult_UpsertsOneNode verifies that a single
// validation.node.result:v1 event is projected onto the release's
// per_node_results read model, so the UI count can climb 0→N as nodes settle
// mid-validation rather than jumping straight from 0 to N on the terminal
// validation.completed:v1.
func TestHandleNodeValidationResult_UpsertsOneNode(t *testing.T) {
	deps, store := seedToValidating(t, "rA")

	err := handlers.HandleNodeValidationResult(context.Background(), deps, handlers.NodeValidationResultInput{
		ReleaseID: "rA",
		Stage:     "validation",
		NodeID:    "a",
		Status:    "ok",
		DBTLogURI: "s3://a",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)

	got := r.PerNodeResults()
	require.Len(t, got, 1)
	assert.Equal(t, "a", got[0].NodeID)
	assert.Equal(t, "ok", got[0].Status)
	assert.Equal(t, "validation", got[0].Stage)
	assert.Equal(t, "s3://a", got[0].DBTLogURI)
}

// TestHandleNodeValidationResult_UnknownReleaseDropsCleanly guards against a
// stale or duplicate validation.node.result:v1 message whose release row no
// longer exists (e.g. it was pruned, or the message was reclaimed from a
// previous consumer for a deleted release). The handler must ack and drop
// rather than dereference a nil aggregate.
func TestHandleNodeValidationResult_UnknownReleaseDropsCleanly(t *testing.T) {
	deps, _ := newDeps(time.Unix(100, 0).UTC())

	err := handlers.HandleNodeValidationResult(context.Background(), deps, handlers.NodeValidationResultInput{
		ReleaseID: "ghost",
		Stage:     "validation",
		NodeID:    "a",
		Status:    "ok",
	})
	require.NoError(t, err)
}
