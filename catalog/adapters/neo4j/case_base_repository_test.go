//go:build integration

package neo4jinfra

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests of this file write the case base for the swap fixture's tenant
// through the unit of work, one message per Write, as the case-base groups do.

// writeCaseBase runs fn on the case base of one final Write for the fixture's
// tenant, under a fresh message of group.
func (f *swapFixture) writeCaseBase(group string, fn func(ctx context.Context, cb repository.CaseBaseRepository) error) {
	f.t.Helper()
	m := uow.Marker{TenantID: f.tenant, Group: group, MessageID: uuid.NewString()}
	require.NoError(f.t, f.uow.Write(context.Background(), m, true, func(ctx context.Context, tx uow.Tx) error {
		return fn(ctx, tx.CaseBase())
	}))
}

// recordFailurePrecedent records rej for the fixture's tenant, as the failure precedents
// group does.
func (f *swapFixture) recordFailurePrecedent(rej casebase.FailurePrecedent) {
	f.t.Helper()
	f.writeCaseBase(streams.CatalogHealTriggeredFailurePrecedents, func(ctx context.Context, cb repository.CaseBaseRepository) error {
		return cb.RecordFailurePrecedent(ctx, f.tenant, rej)
	})
}

// recordFix records p and its PR for the fixture's tenant, as the
// fixes group does.
func (f *swapFixture) recordFix(p casebase.Fix, pr casebase.PullRequest) {
	f.t.Helper()
	f.writeCaseBase(streams.CatalogFixPrOpenedFixes, func(ctx context.Context, cb repository.CaseBaseRepository) error {
		return cb.RecordFix(ctx, f.tenant, p, pr)
	})
}

// recordOutcome records a fix PR's outcome for the fixture's tenant, as the
// provenance group does.
func (f *swapFixture) recordOutcome(o casebase.PullRequestOutcome) {
	f.t.Helper()
	f.writeCaseBase(streams.CatalogFixPrClosedProvenance, func(ctx context.Context, cb repository.CaseBaseRepository) error {
		return cb.RecordPullRequestOutcome(ctx, f.tenant, o)
	})
}

// scalar runs a statement that returns at most one row and returns its column
// v, or nil when it returns none.
func (f *swapFixture) scalar(cypher string, params map[string]any) any {
	f.t.Helper()
	recs := f.rows(cypher, params)
	require.LessOrEqual(f.t, len(recs), 1)
	if len(recs) == 0 {
		return nil
	}
	v, _ := recs[0].Get("v")
	return v
}

// seedVersion creates one :NodeVersion of the tenant, promoted at promotedAt.
func (f *swapFixture) seedVersion(uniqueID, contentHash string, promotedAt time.Time) {
	f.t.Helper()
	f.exec(`CREATE (:NodeVersion {tenant_id: $tenant_id, unique_id: $uid, content_hash: $hash, promoted_at: $at})`,
		map[string]any{"uid": uniqueID, "hash": contentHash, "at": promotedAt.UTC()})
}

// seedCurrentAt points the tenant's :Node uniqueID at its version
// contentHash with a [:CURRENT] stamped promotedAt, creating the node when it
// is absent.
func (f *swapFixture) seedCurrentAt(uniqueID, contentHash string, promotedAt time.Time) {
	f.t.Helper()
	f.exec(`
		MERGE (n:Node {tenant_id: $tenant_id, unique_id: $uid})
		WITH n
		MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: $uid, content_hash: $hash})
		MERGE (n)-[cur:CURRENT]->(v)
		SET cur.promoted_at = $at`,
		map[string]any{"uid": uniqueID, "hash": contentHash, "at": promotedAt.UTC()})
}

// resolvedByVersion returns the content_hash of every :NodeVersion the
// failure precedent (releaseID, nodeID) is [:RESOLVED_BY], sorted.
func (f *swapFixture) resolvedByVersion(releaseID, nodeID string) []string {
	f.t.Helper()
	return f.column(`
		MATCH (:FailurePrecedent {tenant_id: $tenant_id, release_id: $release_id, node_id: $node_id})
		      -[:RESOLVED_BY]->(v:NodeVersion {tenant_id: $tenant_id})
		RETURN v.content_hash AS v ORDER BY v`, map[string]any{"release_id": releaseID, "node_id": nodeID})
}

// failedEdges counts the [:FAILED] edges into the failure precedent (releaseID,
// nodeID) from the tenant's :Node nodeID.
func (f *swapFixture) failedEdges(releaseID, nodeID string) int64 {
	f.t.Helper()
	return f.count(`
		MATCH (:Node {tenant_id: $tenant_id, unique_id: $node_id})-[r:FAILED {release_id: $release_id}]->
		      (:FailurePrecedent {tenant_id: $tenant_id, release_id: $release_id, node_id: $node_id})
		RETURN count(r) AS n`, map[string]any{"release_id": releaseID, "node_id": nodeID})
}

func failurePrecedentOf(releaseID, nodeID, signature string, at time.Time) casebase.FailurePrecedent {
	return casebase.FailurePrecedent{
		ReleaseID: releaseID, NodeID: nodeID, Stage: "validation",
		Category: "cat", Reason: "reason", Signature: signature,
		ErrorExcerpt: "excerpt", DBTLogURI: "uri", At: at,
	}
}

func openPullRequest(fixID, project string, number int, openedAt time.Time) casebase.PullRequest {
	return casebase.PullRequest{
		FixID: fixID, Project: project,
		PrURL: "https://github.com/org/repo/pull/" + strconv.Itoa(number), PrNumber: number,
		State: "open", OpenedBy: "agent-remediation", OpenedAt: openedAt,
	}
}

// ── RecordFailurePrecedent ─────────────────────────────────────────────────────────

// RecordFailurePrecedent writes one :FailurePrecedent with all its properties, MERGEs the
// tenant's :ErrorSignature hub keyed on the signature, and links them with one
// [:HAS_SIGNATURE]; recording it twice converges.
func TestRecordFailurePrecedent_WritesFailurePrecedentAndSignature(t *testing.T) {
	f := newSwapFixture(t)
	rej := casebase.FailurePrecedent{
		ReleaseID:    "rel-1",
		NodeID:       "node-1",
		Stage:        "validation",
		Category:     "null_constraint",
		Reason:       "not-null violation on column x",
		Signature:    "sig-1",
		ErrorExcerpt: "ERROR: null value in column \"x\" violates not-null constraint",
		DBTLogURI:    "s3://bucket/logs/node-1.log",
		At:           time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC),
		RawCode:      "select 1 as x",
		ContentHash:  "hash-1",
	}

	f.recordFailurePrecedent(rej)
	f.recordFailurePrecedent(rej)

	recs := f.rows(`MATCH (rej:FailurePrecedent {tenant_id: $tenant_id, release_id: 'rel-1', node_id: 'node-1'}) RETURN properties(rej) AS p`, nil)
	require.Len(t, recs, 1, "exactly one :FailurePrecedent")
	v, _ := recs[0].Get("p")
	p := v.(map[string]any)
	assert.Equal(t, rej.Stage, p["stage"])
	assert.Equal(t, rej.Category, p["category"])
	assert.Equal(t, rej.Reason, p["reason"])
	assert.Equal(t, rej.ErrorExcerpt, p["error_excerpt"])
	assert.Equal(t, rej.DBTLogURI, p["dbt_log_uri"])
	at, ok := p["at"].(time.Time)
	require.True(t, ok, "at must round-trip as a time.Time, got %T", p["at"])
	assert.True(t, rej.At.Equal(at), "at round-trips; want %v got %v", rej.At, at)
	assert.Equal(t, rej.RawCode, p["raw_code"])
	assert.Equal(t, rej.ContentHash, p["content_hash"])
	assert.Equal(t, false, p["stub"])

	sig := f.rows(`
		MATCH (:FailurePrecedent {tenant_id: $tenant_id, release_id: 'rel-1', node_id: 'node-1'})-[:HAS_SIGNATURE]->
		      (sig:ErrorSignature {tenant_id: $tenant_id, signature: 'sig-1'})
		RETURN sig.category AS category, sig.reason AS reason`, nil)
	require.Len(t, sig, 1, "exactly one [:HAS_SIGNATURE] to the signature hub")
	category, _ := sig[0].Get("category")
	reason, _ := sig[0].Get("reason")
	assert.Equal(t, rej.Category, category)
	assert.Equal(t, rej.Reason, reason)
	assert.Equal(t, int64(1), f.count(`MATCH (s:ErrorSignature {tenant_id: $tenant_id, signature: 'sig-1'}) RETURN count(s) AS n`, nil),
		"the two calls MERGE one hub")
}

// [:FAILED] anchors a failure precedent to its node's :Node when the tenant has one;
// otherwise the failure precedent is recorded alone, and no :Node is created: the
// topology group owns that lifecycle.
func TestRecordFailurePrecedent_FailedEdgeOnlyWhenNodeExists(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("node-a")
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	f.recordFailurePrecedent(failurePrecedentOf("rel-a", "node-a", "sig-a", at))
	assert.Equal(t, int64(1), f.failedEdges("rel-a", "node-a"), "a node of the tenant gets its [:FAILED]")

	f.recordFailurePrecedent(failurePrecedentOf("rel-b", "node-b", "sig-b", at))
	assert.Equal(t, int64(1), f.count(`MATCH (r:FailurePrecedent {tenant_id: $tenant_id, release_id: 'rel-b', node_id: 'node-b'}) RETURN count(r) AS n`, nil),
		"the failure precedent is recorded with no :Node")
	assert.Zero(t, f.count(`MATCH (:FailurePrecedent {tenant_id: $tenant_id, node_id: 'node-b'})<-[f:FAILED]-() RETURN count(f) AS n`, nil))
	assert.Nil(t, f.node("node-b"), "the case base never creates a :Node")
}

// A node a release dropped is retired, not deleted, so a failure precedent of it
// still finds its :Node and links [:FAILED]; a node no release ever carried
// still gets no link and no :Node.
func TestRecordFailurePrecedent_AttachesToRetiredNode(t *testing.T) {
	f := newSwapFixture(t)
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	f.promoteAt("r1", 1, releaseOf(modelNode("analytics.table_a"), modelNode("analytics.table_d")), base)
	f.promoteAt("r2", 2, releaseOf(modelNode("analytics.table_a")), base.Add(time.Hour))
	require.Equal(t, false, f.node("analytics.table_d")["active"], "r2 retired table_d")

	f.recordFailurePrecedent(failurePrecedentOf("r2", "analytics.table_d", "sig-d", base.Add(2*time.Hour)))
	assert.Equal(t, int64(1), f.failedEdges("r2", "analytics.table_d"), "the retired node's failure precedent links [:FAILED]")
	assert.Equal(t, false, f.node("analytics.table_d")["active"], "recording a failure precedent does not revive the node")

	f.recordFailurePrecedent(failurePrecedentOf("r2", "analytics.never_promoted", "sig-n", base.Add(2*time.Hour)))
	assert.Zero(t, f.count(`MATCH (:FailurePrecedent {tenant_id: $tenant_id, node_id: 'analytics.never_promoted'})<-[f:FAILED]-() RETURN count(f) AS n`, nil),
		"a never-promoted node gets no [:FAILED]")
	assert.Nil(t, f.node("analytics.never_promoted"), "the case base never creates a :Node")
}

// When the fix was promoted before the failure precedent lands, RecordFailurePrecedent links
// [:RESOLVED_BY] to the OLDEST version promoted after the failure precedent, and a
// later call never moves an established link, not even to a later-arriving
// version that would rank first.
func TestRecordFailurePrecedent_BackLinksResolvedByWhenNewerVersionExists(t *testing.T) {
	f := newSwapFixture(t)
	t0 := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	rej := failurePrecedentOf("rel", "node", "sig", t0)

	// Two versions newer than the failure precedent qualify at once, so only an
	// ascending order picks the older one.
	f.seedVersion("node", "hash-seq1", t0.Add(-time.Hour))
	f.seedVersion("node", "hash-seq2", t0.Add(time.Hour))
	f.seedVersion("node", "hash-seq3", t0.Add(2*time.Hour))

	f.recordFailurePrecedent(rej)
	assert.Equal(t, []string{"hash-seq2"}, f.resolvedByVersion("rel", "node"), "the oldest qualifying version")

	f.recordFailurePrecedent(rej)
	assert.Equal(t, []string{"hash-seq2"}, f.resolvedByVersion("rel", "node"), "a redelivery leaves the link")

	f.seedVersion("node", "hash-seq4", t0.Add(30*time.Minute))
	f.recordFailurePrecedent(rej)
	assert.Equal(t, []string{"hash-seq2"}, f.resolvedByVersion("rel", "node"),
		"the existing link does not move to a later-arriving version that would rank first")
}

// The back-link's existing-edge guard looks only at [:RESOLVED_BY] edges to a
// :NodeVersion: a merged fix PR's [:RESOLVED_BY] to its :Fix, drawn
// before the failure precedent landed, does not suppress the version back-link.
func TestRecordFailurePrecedent_BackLinkGuardIsPerNodeVersionFamily(t *testing.T) {
	f := newSwapFixture(t)
	t0 := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

	f.seedVersion("node-a", "hash-a1", t0.Add(time.Hour))
	f.recordOutcome(casebase.PullRequestOutcome{
		FixID: "fix", ReleaseID: "rel-a", Project: "core", Outcome: "merged",
		ClosedAt: t0, ResolvedNodeIDs: []string{"node-a"},
	})
	f.recordFailurePrecedent(failurePrecedentOf("rel-a", "node-a", "sig-a", t0))
	assert.Equal(t, []string{"hash-a1"}, f.resolvedByVersion("rel-a", "node-a"),
		"the fix-resolved failure precedent still gets its version back-link")
	assert.Equal(t, int64(1), f.count(`
		MATCH (:FailurePrecedent {tenant_id: $tenant_id, release_id: 'rel-a', node_id: 'node-a'})-[r:RESOLVED_BY]->(:Fix {tenant_id: $tenant_id})
		RETURN count(r) AS n`, nil), "the fix edge is untouched")

	f.seedVersion("node-b", "hash-b1", t0.Add(time.Hour))
	f.recordFailurePrecedent(failurePrecedentOf("rel-b", "node-b", "sig-b", t0))
	f.seedVersion("node-b", "hash-b2", t0.Add(30*time.Minute))
	f.recordFailurePrecedent(failurePrecedentOf("rel-b", "node-b", "sig-b", t0))
	assert.Equal(t, []string{"hash-b1"}, f.resolvedByVersion("rel-b", "node-b"),
		"a failure precedent already linked to a version is left alone")
}

// A revert reuses an existing, older version, so the ordinary branch finds no
// version promoted after the failure precedent; the node's [:CURRENT], stamped with
// the promotion that put it there, shows the fix landed after it.
func TestRecordFailurePrecedent_BackLinksResolvedByOnRevert(t *testing.T) {
	f := newSwapFixture(t)
	t0 := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	f.seedVersion("node", "hash-v1", t0)
	f.seedVersion("node", "hash-v2", t0.Add(10*time.Minute))
	f.seedCurrentAt("node", "hash-v1", t0.Add(30*time.Minute))

	f.recordFailurePrecedent(failurePrecedentOf("rel", "node", "sig", t0.Add(20*time.Minute)))

	assert.Equal(t, []string{"hash-v1"}, f.resolvedByVersion("rel", "node"),
		"the revert links to the current version, found through [:CURRENT]'s promoted_at")
}

// ── RecordFix ──────────────────────────────────────────────────────────

// A fix landing before its failure precedent creates a stub :FailurePrecedent to anchor
// its [:PROPOSED]; the failure precedent completes that stub in place; a replayed
// fix converges on one :Fix and one :PullRequest and leaves the
// completed failure precedent alone.
func TestRecordFix_CreatesStubFailurePrecedent(t *testing.T) {
	f := newSwapFixture(t)
	openedAt := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	p := casebase.Fix{FixID: "fix", ReleaseID: "rel", NodeID: "node"}
	pr := openPullRequest("fix", "core", 42, openedAt)

	f.recordFix(p, pr)
	stub := f.rows(`
		MATCH (rej:FailurePrecedent {tenant_id: $tenant_id, release_id: 'rel', node_id: 'node'})-[:PROPOSED]->
		      (:Fix {tenant_id: $tenant_id, fix_id: 'fix'})-[:HAS_PR]->
		      (pl:PullRequest {tenant_id: $tenant_id, fix_id: 'fix', project: 'core'})
		RETURN rej.stub AS stub, properties(pl) AS pl`, nil)
	require.Len(t, stub, 1, "a stub :FailurePrecedent with a [:PROPOSED]->[:HAS_PR] chain")
	isStub, _ := stub[0].Get("stub")
	assert.Equal(t, true, isStub)
	v, _ := stub[0].Get("pl")
	pl := v.(map[string]any)
	assert.Equal(t, pr.PrURL, pl["pr_url"])
	assert.EqualValues(t, pr.PrNumber, pl["pr_number"])
	assert.Equal(t, "open", pl["pr_state"])
	assert.Equal(t, pr.OpenedBy, pl["opened_by"])
	assert.Nil(t, f.scalar(`MATCH (p:Fix {tenant_id: $tenant_id, fix_id: 'fix'}) RETURN p.pr_url AS v`, nil),
		"the PR facts live on the :PullRequest, not the :Fix")

	rej := failurePrecedentOf("rel", "node", "sig", time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC))
	rej.RawCode, rej.ContentHash = "select 1", "hash-1"
	f.recordFailurePrecedent(rej)
	filled := f.rows(`MATCH (rej:FailurePrecedent {tenant_id: $tenant_id, release_id: 'rel', node_id: 'node'}) RETURN properties(rej) AS p`, nil)
	require.Len(t, filled, 1, "the failure precedent completes the stub in place")
	v, _ = filled[0].Get("p")
	props := v.(map[string]any)
	assert.Equal(t, false, props["stub"])
	assert.Equal(t, rej.Stage, props["stage"])
	assert.Equal(t, rej.RawCode, props["raw_code"])
	assert.Equal(t, int64(1), f.count(`
		MATCH (:FailurePrecedent {tenant_id: $tenant_id, release_id: 'rel', node_id: 'node'})-[r:PROPOSED]->(:Fix {tenant_id: $tenant_id})
		RETURN count(r) AS n`, nil), "[:PROPOSED] survives the stub being completed")

	f.recordFix(p, pr)
	assert.Equal(t, int64(1), f.count(`MATCH (p:Fix {tenant_id: $tenant_id}) RETURN count(p) AS n`, nil))
	assert.Equal(t, int64(1), f.count(`MATCH (p:PullRequest {tenant_id: $tenant_id}) RETURN count(p) AS n`, nil))
	after := f.rows(`MATCH (rej:FailurePrecedent {tenant_id: $tenant_id, release_id: 'rel', node_id: 'node'}) RETURN rej.stub AS stub, rej.at AS at`, nil)
	require.Len(t, after, 1)
	stubAfter, _ := after[0].Get("stub")
	assert.Equal(t, false, stubAfter, "a replayed fix does not turn the failure precedent back into a stub")
	atAfter, _ := after[0].Get("at")
	assert.True(t, rej.At.Equal(atAfter.(time.Time)), "nor overwrite its classified-at with the fix's opened_at")
}

// ── tenancy ─────────────────────────────────────────────────────────────────

// Two tenants record the same failure precedent, fix and outcome: each tenant
// gets its own nodes, its own signature hub and its own links, and neither
// write reaches the other tenant's nodes.
func TestCaseBase_LeavesOtherTenantsAlone(t *testing.T) {
	a := newSwapFixture(t)
	b := a.another()
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	for _, f := range []*swapFixture{a, b} {
		f.seedLiveNode("node")
		f.seedLiveNode("edited")
	}
	beforeB := b.snapshot()

	a.recordFailurePrecedent(failurePrecedentOf("rel", "node", "sig", at))
	a.recordFix(casebase.Fix{FixID: "fix", ReleaseID: "rel", NodeID: "node"}, openPullRequest("fix", "core", 7, at))
	a.recordOutcome(casebase.PullRequestOutcome{
		FixID: "fix", ReleaseID: "rel", Project: "core", Outcome: "merged", ClosedAt: at.Add(time.Hour),
		ResolvedNodeIDs: []string{"node"}, Edits: []casebase.EditOutcome{{Path: "models/edited.sql", TargetNodeID: "edited"}},
	})
	assert.Equal(t, beforeB, b.snapshot(), "tenant a's case base writes nothing of tenant b")
	assert.Equal(t, int64(1), a.failedEdges("rel", "node"))
	assert.Equal(t, int64(1), a.count(`MATCH (:Fix {tenant_id: $tenant_id})-[r:EDITED]->(:Node {tenant_id: $tenant_id, unique_id: 'edited'}) RETURN count(r) AS n`, nil))

	b.recordFailurePrecedent(failurePrecedentOf("rel", "node", "sig", at))
	for _, f := range []*swapFixture{a, b} {
		assert.Equal(t, int64(1), f.count(`MATCH (s:ErrorSignature {tenant_id: $tenant_id, signature: 'sig'}) RETURN count(s) AS n`, nil),
			"one signature hub per tenant")
		assert.Equal(t, int64(1), f.count(`
			MATCH (:FailurePrecedent {tenant_id: $tenant_id})-[r:HAS_SIGNATURE]->(:ErrorSignature {tenant_id: $tenant_id})
			RETURN count(r) AS n`, nil), "a tenant's failure precedent links its own tenant's hub")
		assert.Equal(t, int64(1), f.failedEdges("rel", "node"))
	}
	assert.Zero(t, a.count(`MATCH (x {tenant_id: $tenant_id})-[r]-(y) WHERE y.tenant_id <> x.tenant_id RETURN count(r) AS n`, nil),
		"no relationship joins nodes of two tenants")
}
