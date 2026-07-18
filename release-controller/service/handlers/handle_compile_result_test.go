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

// TestHandleCompileResult_ParseContainerFailure_RejectsAsParseRehearsalFailed
// verifies that a compile-leg failure attributed to the parse-prod or
// parse-candidate container is reported as parse_rehearsal_failed, not
// compile_failed — the failure is continuo's parse-export leg, not a dbt SQL
// error, and must not mislead the remediation agent into proposing a model fix.
func TestHandleCompileResult_ParseContainerFailure_RejectsAsParseRehearsalFailed(t *testing.T) {
	for _, container := range []string{"parse-prod", "parse-candidate"} {
		t.Run(container, func(t *testing.T) {
			d, fakes := newTestDeps(t)
			releaseID := "rel-parse-" + container
			putCompilingRelease(t, fakes, d, releaseID)

			in := handlers.HandleCompileResultInput{
				ReleaseID:   releaseID,
				Status:      "failed",
				PerNode:     []handlers.NodeResult{{NodeID: "core", Status: "failed", FailedContainer: container}},
				ErrorClass:  "compilation_error",
				ErrorDetail: "some dbt compile detail that should be overridden",
			}
			require.NoError(t, handlers.HandleCompileResult(ctx(t), d, in))

			r := mustGetRelease(t, fakes, releaseID)
			assert.Equal(t, release.StatusRejected, r.Status())
			assert.Equal(t, "parse_rehearsal_failed", r.RejectReason())

			e := lastOutbox(t, fakes)
			assert.Equal(t, streams.ReleaseRejectedV1, e.StreamName)
			p := decodeJSON(t, e.Payload)
			assert.Equal(t, "compile", p["stage"])
			assert.Equal(t, "parse_rehearsal_failed", p["reason"])
			assert.Equal(t, "parse_rehearsal_failed", p["error_class"])
			detail, _ := p["error_detail"].(string)
			assert.Contains(t, detail, "not a SQL error")
			assert.Contains(t, detail, "env_var()")
		})
	}
}

// TestHandleCompileResult_UploadContainerFailure_RejectsAsArtifactUploadFailed
// verifies that a compile-leg failure attributed to the upload container is
// reported as artifact_upload_failed — an internal publication failure, not
// something any dbt project change can fix.
func TestHandleCompileResult_UploadContainerFailure_RejectsAsArtifactUploadFailed(t *testing.T) {
	d, fakes := newTestDeps(t)
	releaseID := "rel-upload"
	putCompilingRelease(t, fakes, d, releaseID)

	in := handlers.HandleCompileResultInput{
		ReleaseID:   releaseID,
		Status:      "failed",
		PerNode:     []handlers.NodeResult{{NodeID: "core", Status: "failed", FailedContainer: "upload"}},
		ErrorClass:  "compilation_error",
		ErrorDetail: "some dbt compile detail that should be overridden",
	}
	require.NoError(t, handlers.HandleCompileResult(ctx(t), d, in))

	r := mustGetRelease(t, fakes, releaseID)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "artifact_upload_failed", r.RejectReason())

	e := lastOutbox(t, fakes)
	assert.Equal(t, streams.ReleaseRejectedV1, e.StreamName)
	p := decodeJSON(t, e.Payload)
	assert.Equal(t, "compile", p["stage"])
	assert.Equal(t, "artifact_upload_failed", p["reason"])
	assert.Equal(t, "artifact_upload_failed", p["error_class"])
	detail, _ := p["error_detail"].(string)
	assert.Contains(t, detail, "no change to the dbt project will fix this")
}

// TestHandleCompileResult_NoFailedContainer_RejectsAsCompileFailedUnchanged
// verifies that a compile failure with no failed_container (or "compile")
// keeps the plain compile_failed reason and passes through the producer's
// error_class/error_detail unchanged.
func TestHandleCompileResult_NoFailedContainer_RejectsAsCompileFailedUnchanged(t *testing.T) {
	for _, container := range []string{"", "compile"} {
		t.Run("failed_container="+container, func(t *testing.T) {
			d, fakes := newTestDeps(t)
			releaseID := "rel-compile-" + container
			if container == "" {
				releaseID = "rel-compile-empty"
			}
			putCompilingRelease(t, fakes, d, releaseID)

			in := handlers.HandleCompileResultInput{
				ReleaseID:   releaseID,
				Status:      "failed",
				PerNode:     []handlers.NodeResult{{NodeID: "core", Status: "failed", FailedContainer: container}},
				ErrorClass:  "compilation_error",
				ErrorDetail: "Compilation Error in model daily_transactions",
			}
			require.NoError(t, handlers.HandleCompileResult(ctx(t), d, in))

			r := mustGetRelease(t, fakes, releaseID)
			assert.Equal(t, release.StatusRejected, r.Status())
			assert.Equal(t, "compile_failed", r.RejectReason())

			e := lastOutbox(t, fakes)
			p := decodeJSON(t, e.Payload)
			assert.Equal(t, "compile", p["stage"])
			assert.Equal(t, "compile_failed", p["reason"])
			assert.Equal(t, "compilation_error", p["error_class"])
			assert.Equal(t, "Compilation Error in model daily_transactions", p["error_detail"])
		})
	}
}

func TestHandleCompileResult_UnknownReleaseDropped(t *testing.T) {
	d, fakes := newTestDeps(t)
	require.NoError(t, handlers.HandleCompileResult(ctx(t), d, handlers.HandleCompileResultInput{
		ReleaseID: "nope", Status: "ok",
	}))
	assert.Equal(t, 0, outboxCount(t, fakes))
}

// TestHandleCompileResult_Failed_EmitsUniformRejected verifies that a compile
// failure emits a release.rejected:v1 outbox payload with the canonical uniform
// shape: stage="compile", reason="compile_failed", repo, commit_sha,
// failing_nodes, and per_node with dbt_log_uri. It also verifies that the
// release aggregate records a compile-stage PerNodeResult with Stage=="compile".
func TestHandleCompileResult_Failed_EmitsUniformRejected(t *testing.T) {
	d, fakes := newTestDeps(t)
	releaseID := "rel-1"
	putCompilingRelease(t, fakes, d, releaseID)

	in := handlers.HandleCompileResultInput{
		ReleaseID:   releaseID,
		Status:      "failed",
		PerNode:     []handlers.NodeResult{{NodeID: "core", Status: "failed", DBTLogURI: "s3://c.log"}},
		ErrorClass:  "compilation_error",
		ErrorDetail: "Compilation Error in model daily_transactions",
	}
	err := handlers.HandleCompileResult(ctx(t), d, in)
	require.NoError(t, err)

	r := mustGetRelease(t, fakes, releaseID)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "compile_failed", r.RejectReason())

	// The aggregate must record a per-node compile-stage result.
	require.Len(t, r.PerNodeResults(), 1)
	assert.Equal(t, "compile", r.PerNodeResults()[0].Stage)

	// The outbox payload must match the canonical uniform rejected shape.
	e := lastOutbox(t, fakes)
	assert.Equal(t, streams.ReleaseRejectedV1, e.StreamName)

	var topLevel map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(e.Payload, &topLevel))

	var stage string
	require.NoError(t, json.Unmarshal(topLevel["stage"], &stage))
	assert.Equal(t, "compile", stage)

	var reason string
	require.NoError(t, json.Unmarshal(topLevel["reason"], &reason))
	assert.Equal(t, "compile_failed", reason)

	var repo string
	require.NoError(t, json.Unmarshal(topLevel["repo"], &repo))
	assert.Equal(t, "acme/demo", repo)

	var commitSHA string
	require.NoError(t, json.Unmarshal(topLevel["commit_sha"], &commitSHA))
	assert.Equal(t, "cafebabe", commitSHA)

	var failingNodes []string
	require.NoError(t, json.Unmarshal(topLevel["failing_nodes"], &failingNodes))
	assert.Equal(t, []string{"core"}, failingNodes)

	var perNode []struct {
		NodeID    string `json:"node_id"`
		Status    string `json:"status"`
		DBTLogURI string `json:"dbt_log_uri"`
	}
	require.NoError(t, json.Unmarshal(topLevel["per_node"], &perNode))
	require.Len(t, perNode, 1)
	assert.Equal(t, "failed", perNode[0].Status)
	assert.Equal(t, "s3://c.log", perNode[0].DBTLogURI)
}
