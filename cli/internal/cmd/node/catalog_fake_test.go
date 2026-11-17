package node

import (
	"context"

	catalogv1 "github.com/carolsimone/continuo/cli/proto/catalog/v1"
)

// fakeNodeCatalog is the client.CatalogClient fake shared by every
// node-package test file that exercises a code-version-history command
// (versions, diff, upstream-changes, code-units), the contract commands
// (contract, contract-history, contract-diff) and history's degrade-path
// join. Each RPC records the arguments it was called with so tests can assert
// on them without a real gRPC server.
type fakeNodeCatalog struct {
	versionsResp           *catalogv1.GetNodeVersionsResponse
	versionsErr            error
	gotVersionsUniqueID    string
	gotVersionsLimit       int32
	gotVersionsIncludeCode bool

	diffResp        *catalogv1.GetNodeVersionDiffResponse
	diffErr         error
	gotDiffUniqueID string
	gotDiffFromSeq  int64
	gotDiffToSeq    int64

	upstreamResp        *catalogv1.GetUpstreamChangesResponse
	upstreamErr         error
	gotUpstreamUniqueID string
	gotUpstreamDepth    int32
	gotUpstreamSince    string

	unitsResp        *catalogv1.GetCodeUnitVersionsResponse
	unitsErr         error
	gotUnitsUnitID   string
	gotUnitsUniqueID string
	gotUnitsLimit    int32

	precedentsResp           *catalogv1.GetPrecedentsResponse
	precedentsErr            error
	gotPrecedentsSignature   string
	gotPrecedentsCategory    string
	gotPrecedentsReason      string
	gotPrecedentsLimit       int32
	gotPrecedentsIncludeCode bool

	contractResp               *catalogv1.GetNodeContractResponse
	contractErr                error
	gotContractUniqueID        string
	gotContractIncludeDocument bool
	contractCalls              int

	contractVersionsResp        *catalogv1.ListNodeContractVersionsResponse
	contractVersionsErr         error
	gotContractVersionsUniqueID string
	gotContractVersionsLimit    int32
	contractVersionsCalls       int

	contractDiffResp        *catalogv1.GetNodeContractDiffResponse
	contractDiffErr         error
	gotContractDiffUniqueID string
	gotContractDiffFrom     int64
	gotContractDiffTo       int64
	contractDiffCalls       int

	closeErr error
}

func (f *fakeNodeCatalog) GetNodeVersions(_ context.Context, uniqueID string, limit int32, includeCode bool) (*catalogv1.GetNodeVersionsResponse, error) {
	f.gotVersionsUniqueID, f.gotVersionsLimit, f.gotVersionsIncludeCode = uniqueID, limit, includeCode
	return f.versionsResp, f.versionsErr
}

func (f *fakeNodeCatalog) GetNodeVersionDiff(_ context.Context, uniqueID string, fromSeq, toSeq int64) (*catalogv1.GetNodeVersionDiffResponse, error) {
	f.gotDiffUniqueID, f.gotDiffFromSeq, f.gotDiffToSeq = uniqueID, fromSeq, toSeq
	return f.diffResp, f.diffErr
}

func (f *fakeNodeCatalog) GetUpstreamChanges(_ context.Context, uniqueID string, depth int32, since string) (*catalogv1.GetUpstreamChangesResponse, error) {
	f.gotUpstreamUniqueID, f.gotUpstreamDepth, f.gotUpstreamSince = uniqueID, depth, since
	return f.upstreamResp, f.upstreamErr
}

func (f *fakeNodeCatalog) GetCodeUnitVersions(_ context.Context, unitID, uniqueID string, limit int32) (*catalogv1.GetCodeUnitVersionsResponse, error) {
	f.gotUnitsUnitID, f.gotUnitsUniqueID, f.gotUnitsLimit = unitID, uniqueID, limit
	return f.unitsResp, f.unitsErr
}

func (f *fakeNodeCatalog) GetPrecedents(_ context.Context, signature, category, reason string, limit int32, includeCode bool) (*catalogv1.GetPrecedentsResponse, error) {
	f.gotPrecedentsSignature, f.gotPrecedentsCategory, f.gotPrecedentsReason = signature, category, reason
	f.gotPrecedentsLimit, f.gotPrecedentsIncludeCode = limit, includeCode
	if f.precedentsResp == nil && f.precedentsErr == nil {
		return &catalogv1.GetPrecedentsResponse{}, nil
	}
	return f.precedentsResp, f.precedentsErr
}

func (f *fakeNodeCatalog) GetNodeContract(_ context.Context, uniqueID string, includeDocument bool) (*catalogv1.GetNodeContractResponse, error) {
	f.contractCalls++
	f.gotContractUniqueID, f.gotContractIncludeDocument = uniqueID, includeDocument
	return f.contractResp, f.contractErr
}

func (f *fakeNodeCatalog) ListNodeContractVersions(_ context.Context, uniqueID string, limit int32) (*catalogv1.ListNodeContractVersionsResponse, error) {
	f.contractVersionsCalls++
	f.gotContractVersionsUniqueID, f.gotContractVersionsLimit = uniqueID, limit
	return f.contractVersionsResp, f.contractVersionsErr
}

func (f *fakeNodeCatalog) GetNodeContractDiff(_ context.Context, uniqueID string, fromVersion, toVersion int64) (*catalogv1.GetNodeContractDiffResponse, error) {
	f.contractDiffCalls++
	f.gotContractDiffUniqueID, f.gotContractDiffFrom, f.gotContractDiffTo = uniqueID, fromVersion, toVersion
	return f.contractDiffResp, f.contractDiffErr
}

func (f *fakeNodeCatalog) Close() error { return f.closeErr }
