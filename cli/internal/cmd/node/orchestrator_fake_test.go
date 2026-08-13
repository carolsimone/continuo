package node

import (
	"context"

	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
)

// fakeNodeOrchestrator is the client.OrchestratorClient fake shared by every
// node-package test file that exercises a code-version-history command
// (versions, diff, upstream-changes, code-units) plus history's degrade-path
// join. Each RPC records the arguments it was called with so tests can assert
// on them without a real gRPC server.
type fakeNodeOrchestrator struct {
	versionsResp           *orchestratorv1.GetNodeVersionsResponse
	versionsErr            error
	gotVersionsUniqueID    string
	gotVersionsLimit       int32
	gotVersionsIncludeCode bool

	diffResp        *orchestratorv1.GetNodeVersionDiffResponse
	diffErr         error
	gotDiffUniqueID string
	gotDiffFromSeq  int64
	gotDiffToSeq    int64

	upstreamResp        *orchestratorv1.GetUpstreamChangesResponse
	upstreamErr         error
	gotUpstreamUniqueID string
	gotUpstreamDepth    int32
	gotUpstreamSince    string

	unitsResp        *orchestratorv1.GetCodeUnitVersionsResponse
	unitsErr         error
	gotUnitsUnitID   string
	gotUnitsUniqueID string
	gotUnitsLimit    int32

	runHistoryResp         *orchestratorv1.GetNodeRunHistoryResponse
	runHistoryErr          error
	gotRunHistoryUniqueID  string
	gotRunHistoryLimit     int32
	gotRunHistoryOperation string

	closeErr error
}

func (f *fakeNodeOrchestrator) GetScheduleGraph(context.Context, string) (*orchestratorv1.GetScheduleGraphResponse, error) {
	panic("GetScheduleGraph should not be called in node tests")
}

func (f *fakeNodeOrchestrator) GetNodeVersions(_ context.Context, uniqueID string, limit int32, includeCode bool) (*orchestratorv1.GetNodeVersionsResponse, error) {
	f.gotVersionsUniqueID, f.gotVersionsLimit, f.gotVersionsIncludeCode = uniqueID, limit, includeCode
	return f.versionsResp, f.versionsErr
}

func (f *fakeNodeOrchestrator) GetNodeVersionDiff(_ context.Context, uniqueID string, fromSeq, toSeq int64) (*orchestratorv1.GetNodeVersionDiffResponse, error) {
	f.gotDiffUniqueID, f.gotDiffFromSeq, f.gotDiffToSeq = uniqueID, fromSeq, toSeq
	return f.diffResp, f.diffErr
}

func (f *fakeNodeOrchestrator) GetUpstreamChanges(_ context.Context, uniqueID string, depth int32, since string) (*orchestratorv1.GetUpstreamChangesResponse, error) {
	f.gotUpstreamUniqueID, f.gotUpstreamDepth, f.gotUpstreamSince = uniqueID, depth, since
	return f.upstreamResp, f.upstreamErr
}

func (f *fakeNodeOrchestrator) GetCodeUnitVersions(_ context.Context, unitID, uniqueID string, limit int32) (*orchestratorv1.GetCodeUnitVersionsResponse, error) {
	f.gotUnitsUnitID, f.gotUnitsUniqueID, f.gotUnitsLimit = unitID, uniqueID, limit
	return f.unitsResp, f.unitsErr
}

func (f *fakeNodeOrchestrator) GetNodeRunHistory(_ context.Context, uniqueID string, limit int32, operation string) (*orchestratorv1.GetNodeRunHistoryResponse, error) {
	f.gotRunHistoryUniqueID, f.gotRunHistoryLimit, f.gotRunHistoryOperation = uniqueID, limit, operation
	return f.runHistoryResp, f.runHistoryErr
}

func (f *fakeNodeOrchestrator) Close() error { return f.closeErr }
