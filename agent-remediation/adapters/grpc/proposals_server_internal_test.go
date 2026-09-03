package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

// TestViewToProto_PRCloseFields verifies pr_state passes through verbatim and
// pr_closed_at formats as RFC3339 (empty while nil).
func TestViewToProto_PRCloseFields(t *testing.T) {
	closedAt := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)
	p := viewToProto(proposal.View{ID: "p1", PrState: "merged", PrClosedAt: &closedAt}, nil)
	require.Equal(t, "merged", p.PrState)
	require.Equal(t, "2026-07-03T10:00:00Z", p.PrClosedAt)

	p = viewToProto(proposal.View{ID: "p2", PrState: "open", PrClosedAt: nil}, nil)
	require.Equal(t, "", p.PrClosedAt)
}

// TestViewToProto_PrServicesPassesThroughVerbatim verifies that viewToProto's
// prServices argument — the caller's s.svc.PRServices(v) result — becomes the
// wire's pr_services field unchanged, and that pull_requests maps from
// v.PullRequests.
func TestViewToProto_PrServicesPassesThroughVerbatim(t *testing.T) {
	p := viewToProto(proposal.View{
		ID: "p1",
		PullRequests: []proposal.PullRequest{
			{Service: "core", Repo: "org/core", PrURL: "https://gh/pr/1", PrNumber: 1, PrState: "open"},
		},
	}, []string{"billing", "core"})
	require.Equal(t, []string{"billing", "core"}, p.PrServices)
	require.Len(t, p.PullRequests, 1)
	require.Equal(t, "core", p.PullRequests[0].Service)
	require.Equal(t, "org/core", p.PullRequests[0].Repo)
	require.Equal(t, "https://gh/pr/1", p.PullRequests[0].PrUrl)
	require.Equal(t, int32(1), p.PullRequests[0].PrNumber)
	require.Equal(t, "open", p.PullRequests[0].PrState)

	// Legacy: no service groups at all still produces a nil pull_requests
	// slice, never a one-element list of zero values.
	p = viewToProto(proposal.View{ID: "p2"}, []string{""})
	require.Equal(t, []string{""}, p.PrServices)
	require.Empty(t, p.PullRequests)
}

// TestEditsToProto_EmptyInputYieldsNoElements verifies that a nil or empty
// edit list converts to an absent repeated field rather than a one-element
// list holding a zero-valued FileEdit. A proposal that resolves no source file
// — a validation fix built from the candidate artifact alone — reaches this
// path, and a reader that saw one empty-path edit would try to commit a file
// with no name.
func TestEditsToProto_EmptyInputYieldsNoElements(t *testing.T) {
	require.Empty(t, editsToProto(nil))
	require.Empty(t, editsToProto([]proposal.FileEdit{}))
}

// TestPullRequestsToProto_EmptyInputYieldsNoElements verifies that a nil or
// empty pull-request list converts to an absent repeated field rather than a
// one-element list holding a zero-valued PullRequest — a proposal that never
// entered the PR lifecycle reaches this path.
func TestPullRequestsToProto_EmptyInputYieldsNoElements(t *testing.T) {
	require.Empty(t, pullRequestsToProto(nil))
	require.Empty(t, pullRequestsToProto([]proposal.PullRequest{}))
}

// TestViewToProto_VerificationFields verifies that the run judging a fix, and
// the reason it rejected one, both cross the gRPC boundary.
//
// Both live only in Postgres otherwise. An operator then sees a "Verifying
// fix…" chip on one screen and a release labelled as a verification on
// another with nothing connecting them, and a python proposal that failed
// shows the bare word "failed" while the reason sits unread in the row.
func TestViewToProto_VerificationFields(t *testing.T) {
	p := viewToProto(proposal.View{
		ID:                "p1",
		Status:            proposal.StatusFailed,
		VerificationRunID: "verify-rel-abc1234-1-analytics.py_kpis-a1",
		VerifyError:       `column "revenue_total" does not exist`,
	}, nil)
	require.Equal(t, "verify-rel-abc1234-1-analytics.py_kpis-a1", p.VerificationRunId)
	require.Equal(t, `column "revenue_total" does not exist`, p.VerifyError)

	// An attempt judged synchronously carries neither.
	p = viewToProto(proposal.View{ID: "p2", Status: proposal.StatusProposed}, nil)
	require.Equal(t, "", p.VerificationRunId)
	require.Equal(t, "", p.VerifyError)
}
