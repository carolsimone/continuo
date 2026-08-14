package grpc_test

import (
	"testing"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"

	grpcadapter "github.com/carolsimone/continuo/remediation-agent/adapters/grpc"
	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
)

func TestMapUpstreamChanges(t *testing.T) {
	resp := &orchestratorv1.GetUpstreamChangesResponse{Changes: []*orchestratorv1.UpstreamChange{{
		UniqueId: "analytics.payments", Depth: 1,
		Diff: &orchestratorv1.VersionDiff{RawCodeDiff: "-a\n+b", ConfigDiff: "-x\n+y", Truncated: true},
	}}}
	got := grpcadapter.MapUpstreamChanges(resp)
	want := prompt.UpstreamChange{NodeID: "analytics.payments", Depth: 1,
		CodeDiff: "-a\n+b", ConfigDiff: "-x\n+y", Truncated: true}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("got %+v", got)
	}
}

func TestMapPrecedents(t *testing.T) {
	resp := &orchestratorv1.GetPrecedentsResponse{Precedents: []*orchestratorv1.Precedent{{
		ReleaseId: "rel-9", NodeId: "analytics.orders", Stage: "validation",
		Category: "sql_error", Reason: "missing_column", ErrorExcerpt: "column x does not exist",
		RejectedAt: "2026-08-01T00:00:00Z", Resolved: true,
		ResolutionDiff: "-select 1\n+select 2", ResolutionDiffTruncated: false,
		Proposals: []*orchestratorv1.PrecedentProposal{{PrUrl: "https://github.com/acme/repo/pull/7"}},
	}}}
	got := grpcadapter.MapPrecedents(resp)
	if len(got) != 1 || !got[0].Resolved || got[0].PRURL != "https://github.com/acme/repo/pull/7" ||
		got[0].ResolutionDiff != "-select 1\n+select 2" || got[0].ReleaseID != "rel-9" {
		t.Fatalf("got %+v", got)
	}
}

func TestMapCurrentVersion_EmptyResponse_NotOK(t *testing.T) {
	_, ok := grpcadapter.MapCurrentVersion(&orchestratorv1.GetNodeVersionsResponse{})
	if ok {
		t.Fatal("empty versions must map to ok=false")
	}
}
