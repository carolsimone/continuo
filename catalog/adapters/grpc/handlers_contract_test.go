package grpc_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcadapter "github.com/carolsimone/continuo/catalog/adapters/grpc"
	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"
	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/queries"
)

// fakeContractHistoryReader satisfies grpcadapter.ContractHistoryReader. It
// records the args the handler passed down and the tenant of every call.
type fakeContractHistoryReader struct {
	contract    queries.NodeContract
	contractErr error
	gotDocument bool
	getCalled   bool

	versions    []contractversion.VersionView
	versionsErr error
	gotLimit    int
	listCalled  bool

	changes    []contractversion.FieldChange
	changesErr error
	gotFrom    int64
	gotTo      int64
	diffCalled bool

	tenants []string
}

var _ grpcadapter.ContractHistoryReader = (*fakeContractHistoryReader)(nil)

func (f *fakeContractHistoryReader) GetNodeDataContract(_ context.Context, tenantID, _ string, includeDocument bool) (queries.NodeContract, error) {
	f.tenants = append(f.tenants, tenantID)
	f.getCalled = true
	f.gotDocument = includeDocument
	return f.contract, f.contractErr
}

func (f *fakeContractHistoryReader) ListNodeDataContractVersions(_ context.Context, tenantID, _ string, limit int) ([]contractversion.VersionView, error) {
	f.tenants = append(f.tenants, tenantID)
	f.listCalled = true
	f.gotLimit = limit
	return f.versions, f.versionsErr
}

func (f *fakeContractHistoryReader) GetNodeDataContractDiff(_ context.Context, tenantID, _ string, from, to int64) ([]contractversion.FieldChange, error) {
	f.tenants = append(f.tenants, tenantID)
	f.diffCalled = true
	f.gotFrom, f.gotTo = from, to
	return f.changes, f.changesErr
}

func newHandlerWithContracts(c *fakeContractHistoryReader) *grpcadapter.QueryHandler {
	return grpcadapter.NewQueryHandler(&fakeNodeReader{}, &fakeCodeVersionHistoryReader{}, &fakePrecedentHistoryReader{}, c, discardLogger())
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var promotedAt = time.Date(2026, 11, 27, 16, 40, 12, 0, time.UTC)

// currentKPIs is the revert of the design's example: version 3 holds version
// 1's contract.
func currentKPIs() queries.NodeContract {
	return queries.NodeContract{
		Version: contractversion.VersionView{
			UniqueID: "analytics.py_daily_kpis", Version: 3, ContractHash: "sha256:aaa", ReleaseID: "rel-3",
			PromotionSeq: 52, PromotedAt: promotedAt, ContentHash: "sha256:c3", ContinuoContracts: "0.2.0",
			OntologyVersion: "2026.1", SameAsVersion: 1, IsCurrent: true,
		},
		DocumentYAML: "apiVersion: v3.2.0\nversion: \"3\"\n",
	}
}

func TestGetNodeDataContract_ReturnsTheCurrentVersion(t *testing.T) {
	c := &fakeContractHistoryReader{contract: currentKPIs()}
	h := newHandlerWithContracts(c)

	resp, err := h.GetNodeDataContract(tenantCtx(), &catalogv1.GetNodeDataContractRequest{UniqueId: "analytics.py_daily_kpis", IncludeDocument: true})

	require.NoError(t, err)
	got := resp.GetContract()
	assert.Equal(t, "analytics.py_daily_kpis", got.GetUniqueId())
	assert.Equal(t, int64(3), got.GetVersion())
	assert.Equal(t, int64(1), got.GetSameAsVersion())
	assert.Equal(t, "sha256:aaa", got.GetContractHash())
	assert.Equal(t, "rel-3", got.GetReleaseId())
	assert.Equal(t, "2026-11-27T16:40:12Z", got.GetPromotedAt())
	assert.Equal(t, "sha256:c3", got.GetContentHash())
	assert.Equal(t, "0.2.0", got.GetContinuoContracts())
	assert.Equal(t, "2026.1", got.GetOntologyVersion())
	assert.True(t, got.GetIsCurrent())
	assert.Equal(t, "apiVersion: v3.2.0\nversion: \"3\"\n", got.GetDocumentYaml())
	assert.True(t, c.gotDocument, "include_document reaches the service")
	assert.Equal(t, []string{testTenant}, c.tenants)
}

func TestGetNodeDataContract_WithoutIncludeDocumentAsksForNone(t *testing.T) {
	c := &fakeContractHistoryReader{contract: queries.NodeContract{Version: currentKPIs().Version}}
	h := newHandlerWithContracts(c)

	resp, err := h.GetNodeDataContract(tenantCtx(), &catalogv1.GetNodeDataContractRequest{UniqueId: "analytics.py_daily_kpis"})

	require.NoError(t, err)
	assert.False(t, c.gotDocument)
	assert.Empty(t, resp.GetContract().GetDocumentYaml())
}

func TestGetNodeDataContract_PromotionTimeIsUTC(t *testing.T) {
	contract := currentKPIs()
	contract.Version.PromotedAt = promotedAt.In(time.FixedZone("CET", 3600))
	h := newHandlerWithContracts(&fakeContractHistoryReader{contract: contract})

	resp, err := h.GetNodeDataContract(tenantCtx(), &catalogv1.GetNodeDataContractRequest{UniqueId: "analytics.py_daily_kpis"})

	require.NoError(t, err)
	assert.Equal(t, "2026-11-27T16:40:12Z", resp.GetContract().GetPromotedAt())
}

func TestGetNodeDataContract_NotFoundWithoutAContract(t *testing.T) {
	h := newHandlerWithContracts(&fakeContractHistoryReader{contractErr: contractversion.ErrNotFound})

	_, err := h.GetNodeDataContract(tenantCtx(), &catalogv1.GetNodeDataContractRequest{UniqueId: "analytics.ghost"})

	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetNodeDataContract_ReadFailureIsInternal(t *testing.T) {
	h := newHandlerWithContracts(&fakeContractHistoryReader{contractErr: errors.New("neo4j is down")})

	_, err := h.GetNodeDataContract(tenantCtx(), &catalogv1.GetNodeDataContractRequest{UniqueId: "analytics.py_daily_kpis"})

	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestGetNodeDataContract_RequiresAUniqueID(t *testing.T) {
	c := &fakeContractHistoryReader{}

	_, err := newHandlerWithContracts(c).GetNodeDataContract(tenantCtx(), &catalogv1.GetNodeDataContractRequest{})

	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.False(t, c.getCalled)
}

func TestListNodeDataContractVersions_NewestFirstWithIsCurrent(t *testing.T) {
	c := &fakeContractHistoryReader{versions: []contractversion.VersionView{
		{UniqueID: "analytics.py_daily_kpis", Version: 3, ContractHash: "sha256:aaa", ReleaseID: "rel-3", PromotedAt: promotedAt, SameAsVersion: 1, IsCurrent: true},
		{UniqueID: "analytics.py_daily_kpis", Version: 2, ContractHash: "sha256:bbb", ReleaseID: "rel-2", PromotedAt: promotedAt.Add(-24 * time.Hour)},
		{UniqueID: "analytics.py_daily_kpis", Version: 1, ContractHash: "sha256:aaa", ReleaseID: "rel-1", PromotedAt: promotedAt.Add(-48 * time.Hour)},
	}}
	h := newHandlerWithContracts(c)

	resp, err := h.ListNodeDataContractVersions(tenantCtx(), &catalogv1.ListNodeDataContractVersionsRequest{UniqueId: "analytics.py_daily_kpis", Limit: 3})

	require.NoError(t, err)
	require.Len(t, resp.GetVersions(), 3)
	var order []int64
	for _, v := range resp.GetVersions() {
		order = append(order, v.GetVersion())
		assert.Empty(t, v.GetDocumentYaml(), "a history row carries no document")
	}
	assert.Equal(t, []int64{3, 2, 1}, order)
	assert.True(t, resp.GetVersions()[0].GetIsCurrent())
	assert.False(t, resp.GetVersions()[1].GetIsCurrent())
	assert.Equal(t, int64(1), resp.GetVersions()[0].GetSameAsVersion())
	assert.Equal(t, "2026-11-26T16:40:12Z", resp.GetVersions()[1].GetPromotedAt())
	assert.Equal(t, 3, c.gotLimit)
	assert.Equal(t, []string{testTenant}, c.tenants)
}

func TestListNodeDataContractVersions_LimitZeroMeansAll(t *testing.T) {
	c := &fakeContractHistoryReader{}

	resp, err := newHandlerWithContracts(c).ListNodeDataContractVersions(tenantCtx(), &catalogv1.ListNodeDataContractVersionsRequest{UniqueId: "analytics.py_daily_kpis"})

	require.NoError(t, err)
	assert.Empty(t, resp.GetVersions(), "a node with no history has an empty list, not an error")
	assert.Equal(t, 0, c.gotLimit)
}

func TestListNodeDataContractVersions_RefusesANegativeLimitAndNoUniqueID(t *testing.T) {
	c := &fakeContractHistoryReader{}
	h := newHandlerWithContracts(c)

	_, errLimit := h.ListNodeDataContractVersions(tenantCtx(), &catalogv1.ListNodeDataContractVersionsRequest{UniqueId: "analytics.py_daily_kpis", Limit: -1})
	_, errID := h.ListNodeDataContractVersions(tenantCtx(), &catalogv1.ListNodeDataContractVersionsRequest{})

	assert.Equal(t, codes.InvalidArgument, status.Code(errLimit))
	assert.Equal(t, codes.InvalidArgument, status.Code(errID))
	assert.False(t, c.listCalled)
}

func TestListNodeDataContractVersions_ReadFailureIsInternal(t *testing.T) {
	h := newHandlerWithContracts(&fakeContractHistoryReader{versionsErr: errors.New("neo4j is down")})

	_, err := h.ListNodeDataContractVersions(tenantCtx(), &catalogv1.ListNodeDataContractVersionsRequest{UniqueId: "analytics.py_daily_kpis"})

	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestGetNodeDataContractDiff_TheDesignsExample(t *testing.T) {
	c := &fakeContractHistoryReader{changes: []contractversion.FieldChange{{
		Path:       "schema[0].properties[2].physicalType",
		BeforeJSON: `"NUMERIC(12,2)"`,
		AfterJSON:  `"NUMERIC(14,2)"`,
	}}}
	h := newHandlerWithContracts(c)

	resp, err := h.GetNodeDataContractDiff(tenantCtx(), &catalogv1.GetNodeDataContractDiffRequest{UniqueId: "analytics.py_daily_kpis", FromVersion: 1, ToVersion: 2})

	require.NoError(t, err)
	require.Len(t, resp.GetChanges(), 1)
	change := resp.GetChanges()[0]
	assert.Equal(t, "schema[0].properties[2].physicalType", change.GetPath())
	assert.Equal(t, `"NUMERIC(12,2)"`, change.GetBeforeJson())
	assert.Equal(t, `"NUMERIC(14,2)"`, change.GetAfterJson())
	assert.Equal(t, int64(1), c.gotFrom)
	assert.Equal(t, int64(2), c.gotTo)
	assert.Equal(t, []string{testTenant}, c.tenants)
}

func TestGetNodeDataContractDiff_AnAddedPathHasNoBeforeValue(t *testing.T) {
	h := newHandlerWithContracts(&fakeContractHistoryReader{changes: []contractversion.FieldChange{
		{Path: "schema[0].properties[3]", AfterJSON: `{"name":"fee"}`},
	}})

	resp, err := h.GetNodeDataContractDiff(tenantCtx(), &catalogv1.GetNodeDataContractDiffRequest{UniqueId: "analytics.py_daily_kpis", FromVersion: 1, ToVersion: 2})

	require.NoError(t, err)
	require.Len(t, resp.GetChanges(), 1)
	assert.Empty(t, resp.GetChanges()[0].GetBeforeJson())
	assert.Equal(t, `{"name":"fee"}`, resp.GetChanges()[0].GetAfterJson())
}

func TestGetNodeDataContractDiff_RefusesAVersionTheNodeDoesNotHave(t *testing.T) {
	h := newHandlerWithContracts(&fakeContractHistoryReader{changesErr: contractversion.ErrNotFound})

	_, err := h.GetNodeDataContractDiff(tenantCtx(), &catalogv1.GetNodeDataContractDiffRequest{UniqueId: "analytics.py_daily_kpis", FromVersion: 1, ToVersion: 9})

	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetNodeDataContractDiff_RefusesAVersionBelowOne(t *testing.T) {
	c := &fakeContractHistoryReader{}
	h := newHandlerWithContracts(c)

	for _, req := range []*catalogv1.GetNodeDataContractDiffRequest{
		{UniqueId: "analytics.py_daily_kpis", FromVersion: 0, ToVersion: 2},
		{UniqueId: "analytics.py_daily_kpis", FromVersion: 1, ToVersion: 0},
		{UniqueId: "analytics.py_daily_kpis", FromVersion: -1, ToVersion: 2},
		{UniqueId: "analytics.py_daily_kpis"},
	} {
		_, err := h.GetNodeDataContractDiff(tenantCtx(), req)
		assert.Equal(t, codes.InvalidArgument, status.Code(err), req.String())
	}
	assert.False(t, c.diffCalled, "no read ran")
}

func TestGetNodeDataContractDiff_RequiresAUniqueID(t *testing.T) {
	c := &fakeContractHistoryReader{}

	_, err := newHandlerWithContracts(c).GetNodeDataContractDiff(tenantCtx(), &catalogv1.GetNodeDataContractDiffRequest{FromVersion: 1, ToVersion: 2})

	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.False(t, c.diffCalled)
}

func TestGetNodeDataContractDiff_ReadFailureIsInternal(t *testing.T) {
	h := newHandlerWithContracts(&fakeContractHistoryReader{changesErr: errors.New("neo4j is down")})

	_, err := h.GetNodeDataContractDiff(tenantCtx(), &catalogv1.GetNodeDataContractDiffRequest{UniqueId: "analytics.py_daily_kpis", FromVersion: 1, ToVersion: 2})

	assert.Equal(t, codes.Internal, status.Code(err))
}
