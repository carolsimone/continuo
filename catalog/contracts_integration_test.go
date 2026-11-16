//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/contractbundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The contracts group as main wires it, against the test Neo4j and an object
// store in memory: the production unit of work and contract-bundle reader,
// with the handler of catalog/service/handlers. They live in the composition
// root because the service layer's tests never reach the object store.

// recordedContractMetrics records the versions the handler counts and the
// steps it times.
type recordedContractMetrics struct {
	versions []int
	steps    []string
}

func (m *recordedContractMetrics) VersionsWritten(n int)    { m.versions = append(m.versions, n) }
func (m *recordedContractMetrics) LiveWrite(string, string) {}
func (m *recordedContractMetrics) ObserveStep(step string, _ time.Duration) {
	m.steps = append(m.steps, step)
}

var _ ports.ContractMetrics = (*recordedContractMetrics)(nil)

// countedVersions adds the versions the handler counted.
func (m *recordedContractMetrics) countedVersions() int {
	total := 0
	for _, n := range m.versions {
		total += n
	}
	return total
}

// recordedLiveRewrite records each promotion it rewrites the live folder for,
// and fails with err when set.
type recordedLiveRewrite struct {
	err   error
	calls int
}

func (r *recordedLiveRewrite) Rewrite(context.Context, domainModel.PromoteReleaseInput) error {
	r.calls++
	return r.err
}

var _ handlers.LiveContractsRewriter = (*recordedLiveRewrite)(nil)

// contractsFixture is the contracts handler of one tenant over the production
// unit of work and contract-bundle reader.
type contractsFixture struct {
	*graphFixture
	tenantID string
	metrics  *recordedContractMetrics
	live     *recordedLiveRewrite
	handler  *handlers.ReleasePromotedContractsHandler
}

func newContractsFixture(t *testing.T) *contractsFixture {
	t.Helper()
	g := newGraphFixture(t)
	f := &contractsFixture{
		graphFixture: g,
		tenantID:     g.tenant("contracts"),
		metrics:      &recordedContractMetrics{},
		live:         &recordedLiveRewrite{},
	}
	f.handler = handlers.NewReleasePromotedContractsHandler(
		g.uow, s3infra.NewContractBundleReader(g.mem), f.live, f.metrics, quietLogger())
	return f
}

// release stores the contract bundle of the tenant's release releaseID,
// holding one python contract per entry of contracts (unique id to contract
// hash), gzipped as topology-controller stores it, and returns promotion seq
// of the release naming the bundle and its SHA-256.
func (f *contractsFixture) release(releaseID string, seq int64, contracts map[string]string) domainModel.PromoteReleaseInput {
	f.t.Helper()
	doc := contractbundle.Document{
		SchemaVersion:     contractbundle.SchemaVersion,
		TenantID:          f.tenantID,
		ReleaseID:         releaseID,
		ContinuoContracts: "0.2.0",
		Contracts:         map[string]contractbundle.Contract{},
	}
	for id, hash := range contracts {
		body, err := json.Marshal(map[string]any{"id": id, "kind": "DataContract", "description": hash})
		require.NoError(f.t, err)
		doc.Contracts[id] = contractbundle.Contract{
			Family: "python", Service: "service-py", ContractHash: hash, ContentHash: "sha256:code-" + id,
			Source:   contractbundle.Source{Path: "contracts/" + id + ".odcs.yaml", SHA256: "sha256:src"},
			Document: body,
		}
	}
	raw, err := json.Marshal(doc)
	require.NoError(f.t, err)
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	_, err = w.Write(raw)
	require.NoError(f.t, err)
	require.NoError(f.t, w.Close())
	ref := objectstore.Ref{Bucket: "continuo", Key: contractbundle.Key(f.tenantID, releaseID)}
	require.NoError(f.t, f.mem.Put(context.Background(), ref, gz.Bytes(), "application/gzip"))
	sum := sha256.Sum256(gz.Bytes())

	return domainModel.PromoteReleaseInput{
		TenantID:             f.tenantID,
		ReleaseID:            releaseID,
		PromotionSeq:         seq,
		PromotedAt:           time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Hour),
		ContractBundleURI:    ref.String(),
		ContractBundleSHA256: hex.EncodeToString(sum[:]),
	}
}

// value runs a statement under the fixture's tenant and returns column v of
// its only row.
func (f *contractsFixture) value(cypher string) any {
	f.t.Helper()
	recs := f.query(f.tenantID, cypher)
	require.Len(f.t, recs, 1, cypher)
	v, _ := recs[0].Get("v")
	return v
}

// edges counts the tenant's :CONTRACT_PROMOTED edges.
func (f *contractsFixture) edges() any {
	return f.value(`MATCH (:Node {tenant_id: $tenant_id})-[e:CONTRACT_PROMOTED]->(:ContractVersion {tenant_id: $tenant_id}) RETURN count(e) AS v`)
}

// versions counts the tenant's stored contracts.
func (f *contractsFixture) versions() any {
	return f.value(`MATCH (c:ContractVersion {tenant_id: $tenant_id}) RETURN count(c) AS v`)
}

// recorded reports whether the contracts group's marker of messageID is
// recorded for the tenant.
func (f *contractsFixture) recorded(messageID string) bool {
	f.t.Helper()
	ok, err := f.uow.Processed(context.Background(),
		uow.Marker{TenantID: f.tenantID, Group: streams.CatalogReleaseContracts, MessageID: messageID})
	require.NoError(f.t, err)
	return ok
}

// inflight returns the in-flight marks on the tenant's export lease node.
func (f *contractsFixture) inflight() any {
	return f.value(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'}) RETURN m.inflight_message_ids AS v`)
}

// A node's first promotion with a contract writes its version 1: one stored
// contract, one promotion edge numbered 1 and the pointer at it, with the
// marker recorded and no in-flight mark left.
func TestContractsHandler_AFirstPromotionWritesVersionOne(t *testing.T) {
	f := newContractsFixture(t)
	in := f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})

	require.NoError(t, f.handler.Handle(context.Background(), "41-0", nil, in))

	assert.Equal(t, int64(1), f.versions())
	assert.Equal(t, int64(1), f.edges())
	edge := `MATCH (:Node {tenant_id: $tenant_id, unique_id: 'analytics.py_daily_kpis'})-[e:CONTRACT_PROMOTED]->(:ContractVersion {tenant_id: $tenant_id}) `
	assert.Equal(t, int64(1), f.value(edge+`RETURN e.version AS v`))
	assert.Equal(t, int64(41), f.value(edge+`RETURN e.promotion_seq AS v`))
	assert.Equal(t, "rel-1", f.value(edge+`RETURN e.release_id AS v`))
	assert.Equal(t, "0.2.0", f.value(edge+`RETURN e.continuo_contracts AS v`))
	assert.Equal(t, "contracts/analytics.py_daily_kpis.odcs.yaml", f.value(edge+`RETURN e.source_path AS v`))
	assert.Equal(t, int64(1), f.value(`MATCH (:Node {tenant_id: $tenant_id, unique_id: 'analytics.py_daily_kpis'})-[p:CURRENT_CONTRACT]->(:ContractVersion {tenant_id: $tenant_id, contract_hash: 'sha256:3f1c'}) RETURN p.version AS v`))
	assert.Equal(t, int64(41), f.value(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'analytics.py_daily_kpis'}) RETURN n.contract_promotion_seq AS v`))
	assert.True(t, f.recorded("41-0"))
	assert.Nil(t, f.inflight(), "transaction B clears the in-flight mark")
	assert.Equal(t, []int{1}, f.metrics.versions)
	assert.Equal(t, 1, f.live.calls)
}

// The same message delivered twice writes once: one edge, and one version
// counted. The second delivery finds the marker and reads nothing.
func TestContractsHandler_ARedeliveredMessageWritesNothingTwice(t *testing.T) {
	ctx := context.Background()
	f := newContractsFixture(t)
	in := f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})

	require.NoError(t, f.handler.Handle(ctx, "41-0", nil, in))
	require.NoError(t, f.handler.Handle(ctx, "41-0", nil, in))

	assert.Equal(t, int64(1), f.edges())
	assert.Equal(t, int64(1), f.versions())
	assert.Equal(t, []int{1}, f.metrics.versions, "the second delivery counts nothing")
	assert.Equal(t, 1, f.live.calls, "the second delivery rewrites nothing")
	assert.True(t, f.recorded("41-0"))
}

// A failure after transaction A committed (the live rewrite, here) leaves the
// versions written, the message marked in flight and no marker. The
// redelivery's transaction A finds what the first wrote and changes nothing,
// and its transaction B records the marker and clears the mark.
func TestContractsHandler_AFailureAfterTransactionARedeliversCleanly(t *testing.T) {
	ctx := context.Background()
	f := newContractsFixture(t)
	in := f.release("rel-1", 41, map[string]string{
		"analytics.py_daily_kpis": "sha256:3f1c",
		"analytics.py_weekly":     "sha256:a0b1",
	})

	errStorage := errors.New("object storage unavailable")
	f.live.err = errStorage
	require.ErrorIs(t, f.handler.Handle(ctx, "41-0", nil, in), errStorage)
	assert.Equal(t, int64(2), f.edges(), "transaction A committed")
	assert.False(t, f.recorded("41-0"), "the marker waits for transaction B")
	assert.Equal(t, []any{streams.CatalogReleaseContracts + "/41-0"}, f.inflight(),
		"transaction A marks the message in flight, so an export waits for transaction B")

	f.live.err = nil
	require.NoError(t, f.handler.Handle(ctx, "41-0", nil, in))
	assert.Equal(t, int64(2), f.edges(), "the redelivery records no change twice")
	assert.Equal(t, int64(2), f.versions())
	assert.Equal(t, 2, f.metrics.countedVersions(), "each version is counted once")
	assert.True(t, f.recorded("41-0"))
	assert.Nil(t, f.inflight())
	assert.Equal(t, 2, f.live.calls)
}

// A bundle whose bytes differ from the SHA-256 the promotion carries is
// dead-lettered, and nothing of it is written.
func TestContractsHandler_ABundleThatFailsItsChecksumWritesNothing(t *testing.T) {
	f := newContractsFixture(t)
	in := f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})
	in.ContractBundleSHA256 = hex.EncodeToString(make([]byte, sha256.Size))

	err := f.handler.Handle(context.Background(), "41-0", nil, in)

	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent, "a checksum mismatch is permanent")
	assert.Equal(t, int64(0), f.versions())
	assert.False(t, f.recorded("41-0"))
	assert.Zero(t, f.live.calls)
}
