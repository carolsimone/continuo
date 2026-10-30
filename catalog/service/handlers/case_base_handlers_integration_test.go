//go:build integration

package handlers_test

import (
	"context"
	"testing"

	domainEvent "github.com/carolsimone/continuo/catalog/domain/event"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests of this file drive the case-base handlers of the fixture's tenant
// on the Neo4j unit of work of versionsGroupFixture, whose topology handler
// promotes the nodes their links attach to and whose bundle reader serves
// every bundle from memory.

// promoteNodes applies release r1 carrying the models uniqueIDs, as the
// topology group does.
func (g *versionsGroupFixture) promoteNodes(uniqueIDs ...string) {
	g.t.Helper()
	nodes := make([]topologyartifact.Node, 0, len(uniqueIDs))
	for _, id := range uniqueIDs {
		nodes = append(nodes, modelNode(id, id, "svc", "img", "daily"))
	}
	require.NoError(g.t, g.handler.Handle(context.Background(), "r1-topology", nil, g.promotion("r1", 1, nodes...)))
}

// count runs a statement that returns one integer column v.
func (g *versionsGroupFixture) count(cypher string) int64 {
	g.t.Helper()
	n, ok := g.scalar(cypher).(int64)
	require.True(g.t, ok, "%s returns an integer v", cypher)
	return n
}

// processedBy reports whether group's marker of messageID is recorded for the
// tenant.
func (g *versionsGroupFixture) processedBy(group, messageID string) bool {
	g.t.Helper()
	ok, err := g.uow.Processed(context.Background(), uow.Marker{TenantID: g.tenant, Group: group, MessageID: messageID})
	require.NoError(g.t, err)
	return ok
}

// markerGroups lists the group of every marker of messageID the tenant holds.
func (g *versionsGroupFixture) markerGroups(messageID string) []string {
	g.t.Helper()
	out := []string{}
	for _, rec := range g.rows(`MATCH (p:ProcessedMessage {tenant_id: $tenant_id, message_id: $message_id}) RETURN p.group AS v`,
		map[string]any{"message_id": messageID}) {
		v, _ := rec.Get("v")
		out = append(out, v.(string))
	}
	return out
}

// A crash after the rejections' statements ran rolls back every rejection
// with the marker: nothing of the message is left, and the message is not
// processed. The redelivery records every rejection and the marker in one
// write; a third delivery finds the marker and reads no bundle.
func TestRejectionsHandler_MarkerAtomicWithRecord(t *testing.T) {
	ctx := context.Background()
	g := newVersionsGroupFixture(t)
	g.promoteNodes("svc.public.a", "svc.public.b")
	const uri = "s3://continuo/code-bundles/r2/bundle.json"
	g.bundles.bundles[uri] = codebundle.Bundle{ContractVersion: 1, ReleaseID: "r2", Nodes: map[string]codebundle.Node{
		"svc.public.a": {RawCode: "select a", ContentHash: "ha"},
		"svc.public.b": {RawCode: "select b", ContentHash: "hb"},
	}}
	in := domainEvent.RemediationRequested{
		Source: "validation", ReleaseID: "r2", CodeBundleURI: uri, ClassifiedAt: "2026-10-30T09:00:00Z",
		Nodes: []domainEvent.RemediationRequestedNode{
			{NodeID: "svc.public.a", Category: "logic", ErrorSignature: "sig", Reason: "logic:missing_object"},
			{NodeID: "svc.public.b", Category: "logic", ErrorSignature: "sig", Reason: "logic:missing_object"},
		},
	}
	rejections := func(u uow.UnitOfWork) *handlers.RemediationRequestedRejectionsHandler {
		return handlers.NewRemediationRequestedRejectionsHandler(u, g.bundles, newTestLogger())
	}
	const (
		rejectionCount = `MATCH (r:Rejection {tenant_id: $tenant_id}) RETURN count(r) AS v`
		failedCount    = `MATCH (:Node {tenant_id: $tenant_id})-[f:FAILED]->(:Rejection {tenant_id: $tenant_id}) RETURN count(f) AS v`
		hubCount       = `MATCH (s:ErrorSignature {tenant_id: $tenant_id}) RETURN count(s) AS v`
	)

	crash := &crashingUnitOfWork{UnitOfWork: g.uow, crashAt: 1}
	require.ErrorIs(t, rejections(crash).Handle(ctx, g.tenant, "rej-1", nil, in), errCrash)
	assert.Equal(t, 1, crash.writes, "every rejection and the marker share one write")
	assert.Zero(t, g.count(rejectionCount), "the crash rolls every rejection back")
	assert.Zero(t, g.count(hubCount))
	assert.Zero(t, g.count(failedCount))
	assert.False(t, g.processedBy(streams.CatalogRemediationRequestedRejections, "rej-1"), "no marker without its rejections")

	counted := &crashingUnitOfWork{UnitOfWork: g.uow}
	require.NoError(t, rejections(counted).Handle(ctx, g.tenant, "rej-1", nil, in))
	assert.Equal(t, 1, counted.writes)
	assert.Equal(t, int64(2), g.count(rejectionCount))
	assert.Equal(t, int64(1), g.count(hubCount), "one signature hub for the tenant")
	assert.Equal(t, int64(2), g.count(failedCount))
	assert.Equal(t, []string{streams.CatalogRemediationRequestedRejections}, g.markerGroups("rej-1"),
		"the marker commits with the rejections")

	g.bundles.calls = nil
	require.NoError(t, rejections(g.uow).Handle(ctx, g.tenant, "rej-1", nil, in))
	assert.Empty(t, g.bundles.calls, "a processed message reads no bundle")
	assert.Equal(t, int64(2), g.count(rejectionCount))
	assert.Equal(t, []string{streams.CatalogRemediationRequestedRejections}, g.markerGroups("rej-1"))
}

// A crash after the proposals' statements ran rolls back every proposal, PR
// and stub rejection with the marker; the redelivery records them and the
// marker in one write.
func TestProposalsHandler_MarkerAtomicWithRecord(t *testing.T) {
	ctx := context.Background()
	g := newVersionsGroupFixture(t)
	in := domainEvent.PROpened{
		ProposalID: "prop-1", ReleaseID: "r2", NodeID: "svc.public.a",
		ResolvedNodeIDs: []string{"svc.public.a", "svc.public.b"},
		PrURL:           "https://github.com/org/svc/pull/9", PrNumber: 9,
		OpenedBy: "agent-remediation", OpenedAt: "2026-10-30T10:00:00Z", Service: "svc",
	}
	proposals := func(u uow.UnitOfWork) *handlers.PrOpenedProposalsHandler {
		return handlers.NewPrOpenedProposalsHandler(u, newTestLogger())
	}
	const (
		proposedCount = `MATCH (:Rejection {tenant_id: $tenant_id})-[p:PROPOSED]->(:Proposal {tenant_id: $tenant_id}) RETURN count(p) AS v`
		prCount       = `MATCH (:Proposal {tenant_id: $tenant_id})-[:HAS_PR]->(p:PullRequest {tenant_id: $tenant_id}) RETURN count(p) AS v`
		anyCount      = `MATCH (n {tenant_id: $tenant_id}) WHERE n:Rejection OR n:Proposal OR n:PullRequest RETURN count(n) AS v`
	)

	crash := &crashingUnitOfWork{UnitOfWork: g.uow, crashAt: 1}
	require.ErrorIs(t, proposals(crash).Handle(ctx, g.tenant, "prop-msg", nil, in), errCrash)
	assert.Equal(t, 1, crash.writes, "every proposal and the marker share one write")
	assert.Zero(t, g.count(anyCount), "the crash rolls every proposal back")
	assert.False(t, g.processedBy(streams.CatalogRemediationPrOpenedProposals, "prop-msg"))

	counted := &crashingUnitOfWork{UnitOfWork: g.uow}
	require.NoError(t, proposals(counted).Handle(ctx, g.tenant, "prop-msg", nil, in))
	assert.Equal(t, 1, counted.writes)
	assert.Equal(t, int64(2), g.count(proposedCount), "one [:PROPOSED] per resolved node")
	assert.Equal(t, int64(1), g.count(prCount))
	assert.Equal(t, []string{streams.CatalogRemediationPrOpenedProposals}, g.markerGroups("prop-msg"))

	require.NoError(t, proposals(g.uow).Handle(ctx, g.tenant, "prop-msg", nil, in))
	assert.Equal(t, int64(2), g.count(proposedCount))
	assert.Equal(t, []string{streams.CatalogRemediationPrOpenedProposals}, g.markerGroups("prop-msg"))
}

// A crash after the outcome's statements ran rolls back the PR's state and
// its provenance edges with the marker; the redelivery records them and the
// marker in one write, the [:EDITED] on the tenant's :Node.
func TestPrClosedProvenanceHandler_MarkerAtomicWithRecord(t *testing.T) {
	ctx := context.Background()
	g := newVersionsGroupFixture(t)
	g.promoteNodes("svc.public.a", "svc.public.e")
	in := domainEvent.PRClosed{
		ProposalID: "prop-1", ReleaseID: "r2", NodeID: "svc.public.a",
		ResolvedNodeIDs: []string{"svc.public.a"}, Service: "svc",
		PrURL: "https://github.com/org/svc/pull/9", PrNumber: 9,
		Outcome: "merged", ClosedAt: "2026-10-30T11:00:00Z",
		Edits: []domainEvent.PRClosedEdit{{Path: "models/e.sql", TargetNodeID: "svc.public.e", Amended: true, Diff: "d"}},
	}
	provenance := func(u uow.UnitOfWork) *handlers.PrClosedProvenanceHandler {
		return handlers.NewPrClosedProvenanceHandler(u, newTestLogger())
	}
	const (
		mergedCount   = `MATCH (p:PullRequest {tenant_id: $tenant_id}) WHERE p.pr_state = 'merged' RETURN count(p) AS v`
		resolvedCount = `MATCH (:Rejection {tenant_id: $tenant_id})-[r:RESOLVED_BY]->(:Proposal {tenant_id: $tenant_id}) RETURN count(r) AS v`
		editedCount   = `MATCH (:Proposal {tenant_id: $tenant_id})-[e:EDITED]->(:Node {tenant_id: $tenant_id, unique_id: 'svc.public.e'}) RETURN count(e) AS v`
	)

	crash := &crashingUnitOfWork{UnitOfWork: g.uow, crashAt: 1}
	require.ErrorIs(t, provenance(crash).Handle(ctx, g.tenant, "closed-msg", nil, in), errCrash)
	assert.Equal(t, 1, crash.writes, "the outcome and the marker share one write")
	assert.Zero(t, g.count(mergedCount), "the crash rolls the outcome back")
	assert.Zero(t, g.count(resolvedCount))
	assert.Zero(t, g.count(editedCount))
	assert.False(t, g.processedBy(streams.CatalogRemediationPrClosedProvenance, "closed-msg"))

	counted := &crashingUnitOfWork{UnitOfWork: g.uow}
	require.NoError(t, provenance(counted).Handle(ctx, g.tenant, "closed-msg", nil, in))
	assert.Equal(t, 1, counted.writes)
	assert.Equal(t, int64(1), g.count(mergedCount))
	assert.Equal(t, int64(1), g.count(resolvedCount))
	assert.Equal(t, int64(1), g.count(editedCount))
	assert.Equal(t, []string{streams.CatalogRemediationPrClosedProvenance}, g.markerGroups("closed-msg"))

	require.NoError(t, provenance(g.uow).Handle(ctx, g.tenant, "closed-msg", nil, in))
	assert.Equal(t, int64(1), g.count(resolvedCount))
	assert.Equal(t, int64(1), g.count(editedCount))
	assert.Equal(t, []string{streams.CatalogRemediationPrClosedProvenance}, g.markerGroups("closed-msg"))
}
