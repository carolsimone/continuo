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
	"fmt"
	"os"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/contractbundle"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
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

func (r *recordedLiveRewrite) Rewrite(context.Context, domainModel.PromoteReleaseInput) (string, error) {
	r.calls++
	if r.err != nil {
		return "", r.err
	}
	return "sha-settings", nil
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
		g.uow, s3infra.NewContractBundleReader(g.mem), f.live, f.metrics, time.Minute, quietLogger())
	return f
}

// release stores the contract bundle of the tenant's release releaseID,
// holding one python contract per entry of contracts (unique id to contract
// hash), gzipped as topology-controller stores it, and returns promotion seq
// of the release naming the bundle and its SHA-256.
func (f *contractsFixture) release(releaseID string, seq int64, contracts map[string]string) domainModel.PromoteReleaseInput {
	f.t.Helper()
	return f.releaseResolvedAgainst(releaseID, seq, contracts, nil)
}

// releaseResolvedAgainst is release for a bundle whose ontology value is
// ontology (nil leaves it null, as an install with the switch off writes it).
func (f *contractsFixture) releaseResolvedAgainst(releaseID string, seq int64, contracts map[string]string, ontology json.RawMessage) domainModel.PromoteReleaseInput {
	f.t.Helper()
	doc := f.bundle(releaseID)
	doc.Ontology = ontology
	for id, hash := range contracts {
		body, err := json.Marshal(map[string]any{"id": id, "kind": "DataContract", "description": hash})
		require.NoError(f.t, err)
		doc.Contracts[id] = contractbundle.Contract{
			Family: "python", Project: "service-py", ContractHash: hash, ContentHash: "sha256:code-" + id,
			Source:   contractbundle.Source{Path: "contracts/" + id + ".odcs.yaml", SHA256: "sha256:src"},
			Document: body,
		}
	}
	return f.promote(doc, seq)
}

// bundle is an empty contract bundle of the tenant's release releaseID.
func (f *contractsFixture) bundle(releaseID string) contractbundle.Document {
	return contractbundle.Document{
		SchemaVersion:     contractbundle.SchemaVersion,
		TenantID:          f.tenantID,
		ReleaseID:         releaseID,
		ContinuoContracts: "0.2.0",
		Contracts:         map[string]contractbundle.Contract{},
	}
}

// promote stores doc gzipped as topology-controller stores it and returns
// promotion seq of doc's release naming the bundle and its SHA-256.
func (f *contractsFixture) promote(doc contractbundle.Document, seq int64) domainModel.PromoteReleaseInput {
	f.t.Helper()
	raw, err := json.Marshal(doc)
	require.NoError(f.t, err)
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	_, err = w.Write(raw)
	require.NoError(f.t, err)
	require.NoError(f.t, w.Close())
	ref := objectstore.Ref{Bucket: "continuo", Key: contractbundle.Key(f.tenantID, doc.ReleaseID)}
	require.NoError(f.t, f.mem.Put(context.Background(), ref, gz.Bytes(), "application/gzip"))
	sum := sha256.Sum256(gz.Bytes())

	return domainModel.PromoteReleaseInput{
		TenantID:             f.tenantID,
		ReleaseID:            doc.ReleaseID,
		PromotionSeq:         seq,
		PromotedAt:           time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Hour),
		ContractBundleURI:    ref.String(),
		ContractBundleSHA256: hex.EncodeToString(sum[:]),
	}
}

// sharedBundle is the bundle of pkg/contractbundle/testdata/contracts_v1.json,
// the document topology-controller's tests read a byte-identical copy of: a
// dbt model, analytics.governed, and a Python node, analytics.py_daily_kpis,
// as the contracts of the tenant's release releaseID.
func (f *contractsFixture) sharedBundle(releaseID string) contractbundle.Document {
	f.t.Helper()
	raw, err := os.ReadFile("../pkg/contractbundle/testdata/contracts_v1.json")
	require.NoError(f.t, err)
	var fixture struct {
		Canonical string `json:"canonical"`
	}
	require.NoError(f.t, json.Unmarshal(raw, &fixture))
	var doc contractbundle.Document
	require.NoError(f.t, json.Unmarshal([]byte(fixture.Canonical), &doc))
	doc.TenantID, doc.ReleaseID = f.tenantID, releaseID
	return doc
}

// decodedDocument decodes a stored contract the way catalog reads one back:
// numbers as json.Number, the text the bundle holds.
func decodedDocument(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var document map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&document))
	return document
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

// inflight returns the in-flight marks on the tenant's backup lease node.
func (f *contractsFixture) inflight() any {
	return f.value(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'backup'}) RETURN m.inflight_message_ids AS v`)
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

// A promotion whose bundle holds a dbt contract next to a Python one records
// both alike: each is its own version 1, with its promotion edge, its pointer
// and the family and project its producer wrote on the stored contract, and the
// dbt contract reads back as the document the bundle held.
func TestContractsHandler_IngestsDbtContracts(t *testing.T) {
	ctx := context.Background()
	f := newContractsFixture(t)
	doc := f.sharedBundle("rel-1")
	const dbtID, pythonID = "analytics.governed", "analytics.py_daily_kpis"
	dbtContract, pythonContract := doc.Contracts[dbtID], doc.Contracts[pythonID]
	require.Equal(t, pkgmodel.ContractFamilyDbt, dbtContract.Family, "the shared bundle holds a dbt contract")
	require.Equal(t, pkgmodel.ContractFamilyPython, pythonContract.Family, "and a Python one")
	for id := range doc.Contracts {
		// The nodes the topology swap made live, so the current contract reads back.
		f.exec(f.tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: $id, active: true})`, map[string]any{"id": id})
	}

	require.NoError(t, f.handler.Handle(ctx, "41-0", nil, f.promote(doc, 41)))

	assert.Equal(t, int64(2), f.versions())
	assert.Equal(t, int64(2), f.edges())
	for id, want := range map[string]contractbundle.Contract{dbtID: dbtContract, pythonID: pythonContract} {
		stored := `MATCH (v:ContractVersion {tenant_id: $tenant_id, unique_id: '` + id + `', contract_hash: '` + want.ContractHash + `'}) `
		assert.Equal(t, string(want.Family), f.value(stored+`RETURN v.family AS v`), id)
		assert.Equal(t, want.Project, f.value(stored+`RETURN v.project AS v`), id)
		edge := `MATCH (:Node {tenant_id: $tenant_id, unique_id: '` + id + `'})-[e:CONTRACT_PROMOTED]->(:ContractVersion {tenant_id: $tenant_id, contract_hash: '` + want.ContractHash + `'}) `
		assert.Equal(t, int64(1), f.value(edge+`RETURN e.version AS v`), id)
		assert.Equal(t, int64(41), f.value(edge+`RETURN e.promotion_seq AS v`), id)
		assert.Equal(t, want.ContentHash, f.value(edge+`RETURN e.content_hash AS v`), id)
		assert.Equal(t, want.Source.Path, f.value(edge+`RETURN e.source_path AS v`), id)
		assert.Equal(t, doc.ContinuoContracts, f.value(edge+`RETURN e.continuo_contracts AS v`), id)
		assert.Equal(t, int64(1), f.value(`MATCH (:Node {tenant_id: $tenant_id, unique_id: '`+id+`'})-[p:CURRENT_CONTRACT]->(:ContractVersion {tenant_id: $tenant_id, contract_hash: '`+want.ContractHash+`'}) RETURN p.version AS v`), id)
	}
	assert.Equal(t, "models/tagged/governed.sql", dbtContract.Source.Path, "a dbt contract's source is the model's file")

	current, err := neo4jinfra.NewContractQueryRepository(f.client).Current(ctx, f.tenantID, dbtID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), current.Version)
	assert.Equal(t, dbtContract.ContractHash, current.ContractHash)
	assert.Equal(t, decodedDocument(t, dbtContract.Document), current.Document, "the dbt contract reads back as the bundle held it")
	assert.True(t, f.recorded("41-0"))
	assert.Equal(t, []int{2}, f.metrics.versions)
}

// A promotion whose bundle names the ontology release its contracts were
// resolved against records "<release_id>@sha256:<hex>" on the promotion edge it
// creates, and a later promotion whose bundle names none records none, so each
// version keeps the ontology it used.
func TestContractsHandler_RecordsTheOntologyVersion(t *testing.T) {
	ctx := context.Background()
	f := newContractsFixture(t)
	const id = "analytics.py_daily_kpis"
	f.exec(f.tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: $id, active: true})`, map[string]any{"id": id})
	ontology := json.RawMessage(`{"enabled":true,"project":"ontology","release_id":"rel-o-3","sha256":"abc123","declared_projects":[],"findings":[]}`)

	require.NoError(t, f.handler.Handle(ctx, "41-0", nil, f.releaseResolvedAgainst("rel-1", 41, map[string]string{id: "sha256:3f1c"}, ontology)))
	require.NoError(t, f.handler.Handle(ctx, "47-0", nil, f.release("rel-2", 47, map[string]string{id: "sha256:a0b1"})))

	edge := `MATCH (:Node {tenant_id: $tenant_id, unique_id: '` + id + `'})-[e:CONTRACT_PROMOTED {version: %d}]->(:ContractVersion {tenant_id: $tenant_id}) `
	assert.Equal(t, "rel-o-3@sha256:abc123", f.value(fmt.Sprintf(edge, 1)+`RETURN e.ontology_version AS v`))
	assert.Nil(t, f.value(fmt.Sprintf(edge, 2)+`RETURN e.ontology_version AS v`), "a promotion that names no ontology records none")

	current, err := neo4jinfra.NewContractQueryRepository(f.client).Current(ctx, f.tenantID, id)
	require.NoError(t, err)
	assert.Equal(t, int64(2), current.Version)
	assert.Empty(t, current.OntologyVersion)
	first, err := neo4jinfra.NewContractQueryRepository(f.client).ByVersion(ctx, f.tenantID, id, 1)
	require.NoError(t, err)
	assert.Equal(t, "rel-o-3@sha256:abc123", first.OntologyVersion, "the first version keeps the ontology it used")
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
		"transaction A marks the message in flight, so a backup waits for transaction B")

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

// A promotion older than the node's watermark that carries a contract the
// node never held stores nothing: no version nothing points at, no edge, and
// nothing counted.
func TestContractsHandler_ALatePromotionWithANewHashCreatesNoVersion(t *testing.T) {
	ctx := context.Background()
	f := newContractsFixture(t)
	require.NoError(t, f.handler.Handle(ctx, "52-0", nil,
		f.release("rel-52", 52, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})))

	require.NoError(t, f.handler.Handle(ctx, "47-0", nil,
		f.release("rel-47", 47, map[string]string{"analytics.py_daily_kpis": "sha256:never-promoted"})))

	assert.Equal(t, int64(1), f.versions(), "the late contract is not stored")
	assert.Equal(t, int64(0), f.value(`MATCH (c:ContractVersion {tenant_id: $tenant_id, contract_hash: 'sha256:never-promoted'}) RETURN count(c) AS v`))
	assert.Equal(t, int64(1), f.edges())
	assert.Equal(t, []int{1, 0}, f.metrics.versions, "the late promotion counts 0")
	assert.Equal(t, int64(52), f.value(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'analytics.py_daily_kpis'}) RETURN n.contract_promotion_seq AS v`))
	assert.True(t, f.recorded("47-0"))
}

// A promotion whose contract bundle was written for another tenant is
// dead-lettered, and nothing is written under either tenant.
func TestContractsHandler_ABundleOfAnotherTenantWritesNothing(t *testing.T) {
	f := newContractsFixture(t)
	in := f.release("rel-1", 41, map[string]string{"analytics.py_daily_kpis": "sha256:3f1c"})
	in.TenantID = f.tenant("other")

	err := f.handler.Handle(context.Background(), "41-0", nil, in)

	require.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Contains(t, err.Error(), f.tenantID)
	assert.Equal(t, int64(0), f.versions())
	assert.Empty(t, f.query(in.TenantID, `MATCH (c:ContractVersion {tenant_id: $tenant_id}) RETURN c`))
	assert.Empty(t, f.metrics.versions)
	assert.Zero(t, f.live.calls)
}
