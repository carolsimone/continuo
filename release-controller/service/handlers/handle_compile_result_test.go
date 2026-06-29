package handlers_test

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeJSON unmarshals a JSON byte slice into a map[string]any for
// field-level assertions in outbox payload tests.
func decodeJSON(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

// putCompilingRelease seeds a release that has been activated (Received →
// Compiling via AdvanceQueue) and has a service_prod pointer for a second
// service so AssembleManifestSet in HandleCompileResult produces a non-trivial
// manifest_keys list.
func putCompilingRelease(t *testing.T, store *fakeStore, deps *handlers.Deps, releaseID string) {
	t.Helper()
	require.NoError(t, handlers.ReceiveCandidate(ctx(t), deps, handlers.ReceiveCandidateInput{
		Service:   "svc-a",
		ReleaseID: releaseID,
		ImageTag:  "sha-compile",
		Repo:      "acme/demo",
		CommitSHA: "cafebabe",
	}))
	require.NoError(t, handlers.AdvanceQueue(ctx(t), deps))

	r, err := store.GetRelease(releaseID)
	require.NoError(t, err)
	require.Equal(t, release.StatusCompiling, r.Status(),
		"putCompilingRelease: release must be in compiling after AdvanceQueue")
}

// anyOutboxWithStream reports whether any outbox entry has the given stream name.
func anyOutboxWithStream(t *testing.T, store *fakeStore, streamName string) bool {
	t.Helper()
	for _, e := range store.OutboxEntries() {
		if e.StreamName == streamName {
			return true
		}
	}
	return false
}

func TestHandleCompileResult_OKEmitsReleaseRequested(t *testing.T) {
	d, fakes := newTestDeps(t)
	releaseID := "rel-c-ok"
	putCompilingRelease(t, fakes, d, releaseID)

	require.NoError(t, handlers.HandleCompileResult(ctx(t), d, handlers.HandleCompileResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

	r := mustGetRelease(t, fakes, releaseID)
	assert.Equal(t, release.StatusParsing, r.Status())

	e := lastOutbox(t, fakes)
	assert.Equal(t, streams.ReleaseRequestedV1, e.StreamName)
	p := decodeJSON(t, e.Payload)
	assert.NotEmpty(t, p["manifest_keys"])
	assert.Equal(t, releaseID, p["release_id"])

	// compile.requested was the first outbox entry; release.requested is the last.
	assert.True(t, anyOutboxWithStream(t, fakes, streams.CompileRequestedV1),
		"compile.requested must have been emitted by AdvanceQueue")
}

func TestHandleCompileResult_FailedRejects(t *testing.T) {
	d, fakes := newTestDeps(t)
	releaseID := "rel-c-fail"
	putCompilingRelease(t, fakes, d, releaseID)

	require.NoError(t, handlers.HandleCompileResult(ctx(t), d, handlers.HandleCompileResultInput{
		ReleaseID: releaseID, Status: "failed", ErrorClass: "compile_error", ErrorDetail: "ref not found",
	}))

	r := mustGetRelease(t, fakes, releaseID)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "compile_failed", r.RejectReason())
	assert.Equal(t, streams.ReleaseRejectedV1, lastOutbox(t, fakes).StreamName)
}

func TestHandleCompileResult_UnknownReleaseDropped(t *testing.T) {
	d, fakes := newTestDeps(t)
	require.NoError(t, handlers.HandleCompileResult(ctx(t), d, handlers.HandleCompileResultInput{
		ReleaseID: "nope", Status: "ok",
	}))
	assert.Equal(t, 0, outboxCount(t, fakes))
}
