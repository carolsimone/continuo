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
	p := viewToProto(proposal.View{ID: "p1", PrState: "merged", PrClosedAt: &closedAt})
	require.Equal(t, "merged", p.PrState)
	require.Equal(t, "2026-07-03T10:00:00Z", p.PrClosedAt)

	p = viewToProto(proposal.View{ID: "p2", PrState: "open", PrClosedAt: nil})
	require.Equal(t, "", p.PrClosedAt)
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

// TestViewToProto_ShadowVerificationFields verifies that the release judging a
// fix, and the reason it rejected one, both cross the gRPC boundary.
//
// Both live only in Postgres otherwise. An operator then sees a "Verifying
// fix…" chip on one screen and a release labelled as a verification on
// another with nothing connecting them, and a python proposal that failed
// shows the bare word "failed" while the reason sits unread in the row.
func TestViewToProto_ShadowVerificationFields(t *testing.T) {
	p := viewToProto(proposal.View{
		ID:              "p1",
		Status:          proposal.StatusFailed,
		ShadowReleaseID: "shadow-rel-abc1234-1-analytics.py_kpis-a1",
		VerifyError:     `column "revenue_total" does not exist`,
	})
	require.Equal(t, "shadow-rel-abc1234-1-analytics.py_kpis-a1", p.ShadowReleaseId)
	require.Equal(t, `column "revenue_total" does not exist`, p.VerifyError)

	// An attempt judged synchronously carries neither.
	p = viewToProto(proposal.View{ID: "p2", Status: proposal.StatusProposed})
	require.Equal(t, "", p.ShadowReleaseId)
	require.Equal(t, "", p.VerifyError)
}
