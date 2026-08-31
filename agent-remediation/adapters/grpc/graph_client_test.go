package grpc_test

import (
	"testing"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"

	grpcadapter "github.com/carolsimone/continuo/agent-remediation/adapters/grpc"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
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

// TestMapPrecedents_CopiesEditedEntries verifies that every proto `edited`
// entry (the provenance of a merged fix PR: which node it touched, that
// node's path, whether a human amended it, and the diff that shipped) is
// carried into prompt.Precedent.Edited, in the order the server returned
// them.
func TestMapPrecedents_CopiesEditedEntries(t *testing.T) {
	resp := &orchestratorv1.GetPrecedentsResponse{Precedents: []*orchestratorv1.Precedent{{
		ReleaseId: "rel-9", NodeId: "analytics.orders", Stage: "validation",
		Category: "sql_error", Reason: "missing_column", ErrorExcerpt: "column x does not exist",
		RejectedAt: "2026-08-01T00:00:00Z", Resolved: true,
		Edited: []*orchestratorv1.PrecedentEdit{
			{NodeId: "analytics.payments", Path: "models/payments.sql", Amended: true, Diff: "-old\n+new"},
			{NodeId: "analytics.orders", Path: "models/orders.sql", Amended: false, Diff: "-a\n+b"},
		},
	}}}
	got := grpcadapter.MapPrecedents(resp)
	if len(got) != 1 {
		t.Fatalf("got %d precedents, want 1", len(got))
	}
	want := []prompt.EditedPrecedent{
		{NodeID: "analytics.payments", Path: "models/payments.sql", Amended: true, Diff: "-old\n+new"},
		{NodeID: "analytics.orders", Path: "models/orders.sql", Amended: false, Diff: "-a\n+b"},
	}
	if len(got[0].Edited) != len(want) {
		t.Fatalf("got %d edited entries, want %d: %+v", len(got[0].Edited), len(want), got[0].Edited)
	}
	for i := range want {
		if got[0].Edited[i] != want[i] {
			t.Fatalf("edited[%d] = %+v, want %+v", i, got[0].Edited[i], want[i])
		}
	}
}

func TestMapCurrentVersion_EmptyResponse_NotOK(t *testing.T) {
	_, ok := grpcadapter.MapCurrentVersion(&orchestratorv1.GetNodeVersionsResponse{})
	if ok {
		t.Fatal("empty versions must map to ok=false")
	}
}
