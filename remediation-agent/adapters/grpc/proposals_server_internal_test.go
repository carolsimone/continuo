package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
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
