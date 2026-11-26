//go:build integration

package neo4jinfra

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcadapter "github.com/carolsimone/continuo/catalog/adapters/grpc"
	"github.com/carolsimone/continuo/catalog/adapters/serialization"
	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/queries"
	"github.com/carolsimone/continuo/pkg/identity"
)

// The tests of this file read the design's contract history of py_daily_kpis
// (contract a at seq 41, b at 47, a again at 52) through the gRPC handler, the
// contract query service (queries.ContractQueryService) and the Neo4j reader.

// newQueryHandler is the CatalogQuery handler over f's graph, wired as main
// wires it.
func (f *swapFixture) newQueryHandler(versions *CodeVersionQueryRepository) *grpcadapter.QueryHandler {
	return grpcadapter.NewQueryHandler(
		NewNodeQueryRepository(f.client, quietLogger()),
		queries.NewCodeVersionQueryService(versions),
		queries.NewPrecedentQueryService(NewPrecedentQueryRepository(f.client, quietLogger())),
		queries.NewContractQueryService(f.contractReader(), serialization.ODCSYAML{}, serialization.CanonicalJSON, ""),
		quietLogger())
}

func (f *swapFixture) contractHandler() (*grpcadapter.QueryHandler, context.Context) {
	return f.newQueryHandler(NewCodeVersionQueryRepository(f.client, quietLogger())), identity.WithTenant(context.Background(), f.tenant)
}

func TestGetNodeContract_ServesTheCurrentVersionOfTheDesignsHistory(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.designHistory()
	h, ctx := f.contractHandler()

	resp, err := h.GetNodeContract(ctx, &catalogv1.GetNodeContractRequest{UniqueId: kpisNode, IncludeDocument: true})

	require.NoError(t, err)
	got := resp.GetContract()
	assert.Equal(t, int64(3), got.GetVersion(), "the revert is the third promotion")
	assert.Equal(t, int64(1), got.GetSameAsVersion())
	assert.Equal(t, contractA, got.GetContractHash())
	assert.Equal(t, "rel-77aa0bc-230", got.GetReleaseId())
	assert.Equal(t, kpisContent, got.GetContentHash())
	assert.Equal(t, "0.2.0", got.GetContinuoContracts())
	assert.True(t, got.GetIsCurrent())
	assert.Equal(t, promotedAtOf(52).UTC().Format(time.RFC3339), got.GetPromotedAt())
	assert.Contains(t, got.GetDocumentYaml(), "version: \"3\"\n")
	assert.Contains(t, got.GetDocumentYaml(), "name: service-py", "the stored form of contract a")
	assert.Contains(t, got.GetDocumentYaml(), "releaseId: rel-77aa0bc-230", "the provenance of the promotion")
	assert.NotContains(t, got.GetDocumentYaml(), "servers", "the published form without servers")

	bare, err := h.GetNodeContract(ctx, &catalogv1.GetNodeContractRequest{UniqueId: kpisNode})
	require.NoError(t, err)
	assert.Empty(t, bare.GetContract().GetDocumentYaml(), "no document unless asked for")
}

func TestListNodeContractVersions_ServesTheDesignsHistoryNewestFirst(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.designHistory()
	h, ctx := f.contractHandler()

	resp, err := h.ListNodeContractVersions(ctx, &catalogv1.ListNodeContractVersionsRequest{UniqueId: kpisNode})
	require.NoError(t, err)

	var order []int64
	var current []int64
	for _, v := range resp.GetVersions() {
		order = append(order, v.GetVersion())
		if v.GetIsCurrent() {
			current = append(current, v.GetVersion())
		}
		assert.Empty(t, v.GetDocumentYaml())
	}
	assert.Equal(t, []int64{3, 2, 1}, order)
	assert.Equal(t, []int64{3}, current)

	two, err := h.ListNodeContractVersions(ctx, &catalogv1.ListNodeContractVersionsRequest{UniqueId: kpisNode, Limit: 2})
	require.NoError(t, err)
	assert.Len(t, two.GetVersions(), 2)

	none, err := h.ListNodeContractVersions(ctx, &catalogv1.ListNodeContractVersionsRequest{UniqueId: "analytics.absent"})
	require.NoError(t, err)
	assert.Empty(t, none.GetVersions(), "a node with no history has an empty list")
}

func TestGetNodeContractDiff_ServesTheFieldsTwoVersionsDifferAt(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.designHistory()
	h, ctx := f.contractHandler()

	resp, err := h.GetNodeContractDiff(ctx, &catalogv1.GetNodeContractDiffRequest{UniqueId: kpisNode, FromVersion: 1, ToVersion: 2})
	require.NoError(t, err)
	require.Len(t, resp.GetChanges(), 1)
	assert.Equal(t, "team.name", resp.GetChanges()[0].GetPath())
	assert.Equal(t, `"service-py"`, resp.GetChanges()[0].GetBeforeJson())
	assert.Equal(t, `"analytics-eng"`, resp.GetChanges()[0].GetAfterJson())

	revert, err := h.GetNodeContractDiff(ctx, &catalogv1.GetNodeContractDiffRequest{UniqueId: kpisNode, FromVersion: 1, ToVersion: 3})
	require.NoError(t, err)
	assert.Empty(t, revert.GetChanges(), "version 3 reverts to version 1's contract")

	_, err = h.GetNodeContractDiff(ctx, &catalogv1.GetNodeContractDiffRequest{UniqueId: kpisNode, FromVersion: 1, ToVersion: 4})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = h.GetNodeContractDiff(ctx, &catalogv1.GetNodeContractDiffRequest{UniqueId: kpisNode, FromVersion: 0, ToVersion: 2})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetNodeContract_NotFoundWithoutAContractOrForARetiredNode(t *testing.T) {
	f := newSwapFixture(t)
	f.promoteAt("rel-40", 40, []topology.ReleasePromotedTopologyNode{modelNode(kpisNode), modelNode("analytics.retired"), modelNode("analytics.no_contract")}, time.Now())
	f.writeContracts(contractPromotion("rel-41", 41),
		contractOf(kpisNode, contractA, "service-py"), contractOf("analytics.retired", contractA, "gone"))
	f.promoteAt("rel-42", 42, []topology.ReleasePromotedTopologyNode{modelNode(kpisNode), modelNode("analytics.no_contract")}, time.Now())
	require.Equal(t, false, f.node("analytics.retired")["active"])
	h, ctx := f.contractHandler()

	_, err := h.GetNodeContract(ctx, &catalogv1.GetNodeContractRequest{UniqueId: "analytics.no_contract"})
	assert.Equal(t, codes.NotFound, status.Code(err), "a node never promoted with a contract")
	_, err = h.GetNodeContract(ctx, &catalogv1.GetNodeContractRequest{UniqueId: "analytics.retired"})
	assert.Equal(t, codes.NotFound, status.Code(err), "a retired node has no current contract")
	_, err = h.GetNodeContract(ctx, &catalogv1.GetNodeContractRequest{UniqueId: "analytics.absent"})
	assert.Equal(t, codes.NotFound, status.Code(err), "an unknown node")

	history, err := h.ListNodeContractVersions(ctx, &catalogv1.ListNodeContractVersionsRequest{UniqueId: "analytics.retired"})
	require.NoError(t, err)
	require.Len(t, history.GetVersions(), 1, "a retired node keeps its history")
	assert.False(t, history.GetVersions()[0].GetIsCurrent())
}

func TestContractRPCs_ReadOnlyTheRequestTenant(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.designHistory()
	other := f.another()
	h, _ := f.contractHandler()
	otherCtx := identity.WithTenant(context.Background(), other.tenant)

	_, err := h.GetNodeContract(otherCtx, &catalogv1.GetNodeContractRequest{UniqueId: kpisNode})
	assert.Equal(t, codes.NotFound, status.Code(err))
	history, err := h.ListNodeContractVersions(otherCtx, &catalogv1.ListNodeContractVersionsRequest{UniqueId: kpisNode})
	require.NoError(t, err)
	assert.Empty(t, history.GetVersions())
	_, err = h.GetNodeContractDiff(otherCtx, &catalogv1.GetNodeContractDiffRequest{UniqueId: kpisNode, FromVersion: 1, ToVersion: 2})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
