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
func v2Message(t *testing.T, p pkgevents.ReleaseParsed) goredis.XMessage {
	t.Helper()
	fields, err := pkgevents.ReleaseParsedFields(pkgevents.DefaultTenantID, "topology-controller",
		time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC), p)
	require.NoError(t, err)
	return goredis.XMessage{ID: "1-0", Values: fields}
}

func TestParsedManifestInput_OKCarriesTheTopologyReference(t *testing.T) {
	in := parsedManifestInput(pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusOK,
		TopologyURI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", TopologySHA256: "ab", NodeCount: 3,
		CodeBundleURI: "s3://continuo/code-bundles/r1/bundle.json",
	}, slog.Default())
	assert.Equal(t, "ok", in.Status)
	assert.Equal(t, release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", SHA256: "ab", NodeCount: 3}, in.TopologyRef)
	assert.Equal(t, "s3://continuo/code-bundles/r1/bundle.json", in.CodeBundleURI)
}

func TestParsedManifestInput_CarriesTheContractBundleAndTheLibraryVersion(t *testing.T) {
	ok := parsedManifestInput(pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusOK,
		TopologyURI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", TopologySHA256: "ab", NodeCount: 3,
		ContractBundleURI:    "s3://continuo/tenants/default/topologies/r1/contracts.json.gz",
		ContractBundleSHA256: "cd", ContinuoContracts: "0.2.0",
	}, slog.Default())
	assert.Equal(t, release.ContractBundleRef{URI: "s3://continuo/tenants/default/topologies/r1/contracts.json.gz", SHA256: "cd"}, ok.ContractBundle)
	assert.Equal(t, "0.2.0", ok.ContinuoContracts)

	failed := parsedManifestInput(pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusFailed, FailureKind: "invalid_contract", Detail: "x",
		ContinuoContracts: "0.2.0",
	}, slog.Default())
	assert.Equal(t, "0.2.0", failed.ContinuoContracts, "a failed parse names the library version too")
	assert.Equal(t, release.ContractBundleRef{}, failed.ContractBundle)
}

func TestParsedManifestInput_CarriesTheParseTimings(t *testing.T) {
	ok := parsedManifestInput(pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusOK,
		TopologyURI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", TopologySHA256: "ab", NodeCount: 3,
		ParseMs: 4180, ContractsMs: 1260, ContractsByFamily: map[pkg_model.ContractFamily]int{"dbt": 3, "python": 2},
	}, slog.Default())
	assert.Equal(t, release.NewParseTimings(4180, 1260, map[pkg_model.ContractFamily]int{"dbt": 3, "python": 2}), ok.ParseTimings)

	failed := parsedManifestInput(pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusFailed, FailureKind: "invalid_sql", Detail: "x",
		ParseMs: 310, ContractsMs: 45,
	}, slog.Default())
	assert.Equal(t, release.NewParseTimings(310, 45, nil), failed.ParseTimings)
	assert.Nil(t, failed.ParseTimings.ContractsByFamily, "a failed parse counts no contracts")

	old := parsedManifestInput(pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusOK,
		TopologyURI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", TopologySHA256: "ab", NodeCount: 3,
	}, slog.Default())
	assert.Equal(t, release.ParseTimings{}, old.ParseTimings, "an entry from a producer that predates the fields reports none")
}

func TestParsedManifestInput_Failed(t *testing.T) {
	in := parsedManifestInput(pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusFailed, FailureKind: "invalid_sql", Detail: "1 node failed to parse: a.b",
		FailedNodes: []pkgevents.ManifestFailedNode{{NodeID: "a.b", Kind: "invalid_sql", Project: "s", FilePath: "models/b.sql", NodeType: "dbt-model", Detail: "Expecting )"}},
	}, slog.Default())
	assert.Equal(t, pkg_model.ParseFailureKindInvalidSQL, in.FailureKind)
	assert.Equal(t, "1 node failed to parse: a.b", in.Detail)
	assert.True(t, in.TopologyRef.IsZero())
	require.Len(t, in.FailedNodes, 1)
	assert.Equal(t, handlers.ParsedFailedNode{NodeID: "a.b", Kind: pkg_model.ParseFailureKindInvalidSQL, Project: "s",
		FilePath: "models/b.sql", NodeType: "dbt-model", Detail: "Expecting )"}, in.FailedNodes[0])
}

func TestParsedManifestInput_UnknownKindPassesThrough(t *testing.T) {
	in := parsedManifestInput(pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusFailed, FailureKind: "from_the_future", Detail: "x",
	}, slog.Default())
	assert.False(t, in.FailureKind.IsValid())
	assert.Equal(t, pkg_model.RejectReasonInternalError, handlers.ParseReason(in.FailureKind))
}

// A v2 entry decodes from its envelope fields and reaches HandleParsedManifest
// (proven by the release id reaching Get); the queue then advances.
func TestReleaseParsedHandler_DecodesTheV2Entry(t *testing.T) {
	deps, repo := newValidationResultDeps()
	h := newReleaseParsedHandler(deps, newDiscardLogger())

	err := h(context.Background(), v2Message(t, pkgevents.ReleaseParsed{
		ReleaseID: "r1", Status: pkgevents.ManifestStatusOK,
		TopologyURI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", TopologySHA256: "ab", NodeCount: 1,
	}))
	require.NoError(t, err, "an unknown release is dropped")
	assert.Equal(t, "r1", repo.gotID)
	assert.True(t, repo.advanceLocked, "every parse result advances the queue")
}

// An entry in the v1 shape (a bare payload field) carries no envelope; it is
// dead-lettered rather than retried.
func TestReleaseParsedHandler_V1ShapedEntryIsPermanent(t *testing.T) {
	h := newReleaseParsedHandler(nil, newDiscardLogger())
	msg := goredis.XMessage{ID: "2-0", Values: map[string]any{
		"payload": `{"release_id":"r1","status":"ok","topology":[]}`,
	}}
	assert.True(t, errors.Is(h(context.Background(), msg), pkgevents.ErrPermanent))
}
