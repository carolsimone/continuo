package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
)

// TestViewToProto_PRCloseFields verifies pr_state passes through verbatim and
// pr_closed_at formats as RFC3339 (empty while nil).
func TestViewToProto_PRCloseFields(t *testing.T) {
	closedAt := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)
	p := viewToProto(fix.View{ID: "p1", PrState: "merged", PrClosedAt: &closedAt}, nil)
	require.Equal(t, "merged", p.PrState)
	require.Equal(t, "2026-07-03T10:00:00Z", p.PrClosedAt)

	p = viewToProto(fix.View{ID: "p2", PrState: "open", PrClosedAt: nil}, nil)
	require.Equal(t, "", p.PrClosedAt)
}

// TestViewToProto_PrProjectsPassesThroughVerbatim verifies that viewToProto's
// prProjects argument — the caller's s.svc.PRProjects(v) result — becomes the
// wire's pr_projects field unchanged, and that pull_requests maps from
// v.PullRequests.
func TestViewToProto_PrProjectsPassesThroughVerbatim(t *testing.T) {
	p := viewToProto(fix.View{
		ID: "p1",
		PullRequests: []fix.PullRequest{
			{Project: "core", Repo: "org/core", PrURL: "https://gh/pr/1", PrNumber: 1, PrState: "open"},
		},
	}, []string{"billing", "core"})
	require.Equal(t, []string{"billing", "core"}, p.PrProjects)
	require.Len(t, p.PullRequests, 1)
	require.Equal(t, "core", p.PullRequests[0].Project)
	require.Equal(t, "org/core", p.PullRequests[0].Repo)
	require.Equal(t, "https://gh/pr/1", p.PullRequests[0].PrUrl)
	require.Equal(t, int32(1), p.PullRequests[0].PrNumber)
	require.Equal(t, "open", p.PullRequests[0].PrState)

	// Legacy: no project groups at all still produces a nil pull_requests
	// slice, never a one-element list of zero values.
	p = viewToProto(fix.View{ID: "p2"}, []string{""})
	require.Equal(t, []string{""}, p.PrProjects)
	require.Empty(t, p.PullRequests)
}

// TestViewToProto_ProjectsPassesThroughVerbatim verifies that the row's
// projects — every project the attempt touched, the set ListFixes'
// project filter matches on — reach the wire unchanged, so a listing can
// show the same projects the filter selected by.
func TestViewToProto_ProjectsPassesThroughVerbatim(t *testing.T) {
	p := viewToProto(fix.View{ID: "p1", Projects: []string{"analytics", "billing"}}, []string{""})
	require.Equal(t, []string{"analytics", "billing"}, p.Projects)

	p = viewToProto(fix.View{ID: "p2"}, []string{""})
	require.Empty(t, p.Projects)
}

// TestEditsToProto_EmptyInputYieldsNoElements verifies that a nil or empty
// edit list converts to an absent repeated field rather than a one-element
// list holding a zero-valued FileEdit. A fix that resolves no source file
// — a validation fix built from the candidate artifact alone — reaches this
// path, and a reader that saw one empty-path edit would try to commit a file
// with no name.
func TestEditsToProto_EmptyInputYieldsNoElements(t *testing.T) {
	require.Empty(t, editsToProto(nil))
	require.Empty(t, editsToProto([]fix.FileEdit{}))
}

// TestPullRequestsToProto_EmptyInputYieldsNoElements verifies that a nil or
// empty pull-request list converts to an absent repeated field rather than a
// one-element list holding a zero-valued PullRequest — a fix that never
// entered the PR lifecycle reaches this path.
func TestPullRequestsToProto_EmptyInputYieldsNoElements(t *testing.T) {
	require.Empty(t, pullRequestsToProto(nil))
	require.Empty(t, pullRequestsToProto([]fix.PullRequest{}))
}

// TestViewToProto_ReleaseCheckFields verifies that the release check judging a fix, and
// the reason it rejected one, both cross the gRPC boundary.
//
// Both live only in Postgres otherwise. An operator then sees a "Verifying
// fix…" chip on one screen and a release labelled as a release check on
// another with nothing connecting them, and a python fix that failed
// shows the bare word "failed" while the reason sits unread in the row.
func TestViewToProto_ReleaseCheckFields(t *testing.T) {
	p := viewToProto(fix.View{
		ID:             "p1",
		Status:         fix.StatusFailed,
		ReleaseCheckID: "verify-rel-abc1234-1-analytics.py_kpis-a1",
		VerifyError:    `column "revenue_total" does not exist`,
	}, nil)
	require.Equal(t, "verify-rel-abc1234-1-analytics.py_kpis-a1", p.ReleaseCheckId)
	require.Equal(t, `column "revenue_total" does not exist`, p.VerifyError)

	// An attempt judged synchronously carries neither.
	p = viewToProto(fix.View{ID: "p2", Status: fix.StatusProposed}, nil)
	require.Equal(t, "", p.ReleaseCheckId)
	require.Equal(t, "", p.VerifyError)
}
