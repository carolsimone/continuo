package redis

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// v2Message renders p as the stream entry topology-controller publishes.
func v2Message(t *testing.T, p pkgevents.ManifestLoadedCandidate) goredis.XMessage {
	t.Helper()
	fields, err := pkgevents.ManifestLoadedCandidateFields(pkgevents.DefaultTenantID, "topology-controller",
		time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC), p)
	require.NoError(t, err)
	return goredis.XMessage{ID: "1-0", Values: fields}
}

func TestParsedManifestInput_OKCarriesTheTopologyReference(t *testing.T) {
	in := parsedManifestInput(pkgevents.ManifestLoadedCandidate{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusOK,
		TopologyURI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", TopologySHA256: "ab", NodeCount: 3,
		CodeBundleURI: "s3://continuo/code-bundles/r1/bundle.json",
	}, slog.Default())
	assert.Equal(t, "ok", in.Status)
	assert.Equal(t, release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", SHA256: "ab", NodeCount: 3}, in.TopologyRef)
	assert.Equal(t, "s3://continuo/code-bundles/r1/bundle.json", in.CodeBundleURI)
}

func TestParsedManifestInput_Failed(t *testing.T) {
	in := parsedManifestInput(pkgevents.ManifestLoadedCandidate{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusFailed, FailureKind: "invalid_sql", Detail: "1 node failed to parse: a.b",
		FailedNodes: []pkgevents.ManifestFailedNode{{NodeID: "a.b", Kind: "invalid_sql", Service: "s", FilePath: "models/b.sql", NodeType: "dbt-model", Detail: "Expecting )"}},
	}, slog.Default())
	assert.Equal(t, pkg_model.ParseFailureKindInvalidSQL, in.FailureKind)
	assert.Equal(t, "1 node failed to parse: a.b", in.Detail)
	assert.True(t, in.TopologyRef.IsZero())
	require.Len(t, in.FailedNodes, 1)
	assert.Equal(t, handlers.ParsedFailedNode{NodeID: "a.b", Kind: pkg_model.ParseFailureKindInvalidSQL, Service: "s",
		FilePath: "models/b.sql", NodeType: "dbt-model", Detail: "Expecting )"}, in.FailedNodes[0])
}

func TestParsedManifestInput_UnknownKindPassesThrough(t *testing.T) {
	in := parsedManifestInput(pkgevents.ManifestLoadedCandidate{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusFailed, FailureKind: "from_the_future", Detail: "x",
	}, slog.Default())
	assert.False(t, in.FailureKind.IsValid())
	assert.Equal(t, pkg_model.RejectReasonInternalError, handlers.ParseReason(in.FailureKind))
}

// A v2 entry decodes from its envelope fields and reaches HandleParsedManifest
// (proven by the release id reaching Get); the queue then advances.
func TestManifestLoadedCandidateHandler_DecodesTheV2Entry(t *testing.T) {
	deps, repo := newValidationResultDeps()
	h := newManifestLoadedCandidateHandler(deps, newDiscardLogger())

	err := h(context.Background(), v2Message(t, pkgevents.ManifestLoadedCandidate{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusOK,
		TopologyURI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", TopologySHA256: "ab", NodeCount: 1,
	}))
	require.NoError(t, err, "an unknown release is dropped")
	assert.Equal(t, "r1", repo.gotID)
	assert.True(t, repo.advanceLocked, "every parse result advances the queue")
}

// An entry in the v1 shape (a bare payload field) carries no envelope; it is
// dead-lettered rather than retried.
func TestManifestLoadedCandidateHandler_V1ShapedEntryIsPermanent(t *testing.T) {
	h := newManifestLoadedCandidateHandler(nil, newDiscardLogger())
	msg := goredis.XMessage{ID: "2-0", Values: map[string]any{
		"payload": `{"release_id":"r1","status":"ok","topology":[]}`,
	}}
	assert.True(t, errors.Is(h(context.Background(), msg), pkgevents.ErrPermanent))
}
