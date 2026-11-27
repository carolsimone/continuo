package grpc_test

import (
	"context"
	"testing"

	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	grpcadapter "github.com/carolsimone/continuo/release-controller/adapters/grpc"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// fakeFixesClient implements only ListFixes; embedding the interface
// satisfies the rest of remediationv1.RemediationFixesClient without
// needing stub methods this test never calls.
type fakeFixesClient struct {
	remediationv1.RemediationFixesClient
	gotReq *remediationv1.ListFixesRequest
	resp   *remediationv1.ListFixesResponse
	err    error
}

func (f *fakeFixesClient) ListFixes(_ context.Context, in *remediationv1.ListFixesRequest, _ ...grpc.CallOption) (*remediationv1.ListFixesResponse, error) {
	f.gotReq = in
	return f.resp, f.err
}

func TestFixesClient_ListFixesForRelease_MapsSummaries(t *testing.T) {
	fake := &fakeFixesClient{resp: &remediationv1.ListFixesResponse{
		Fixes: []*remediationv1.Fix{
			{Id: "p1", NodeId: "finance", Try: 1, Status: "failed", PrState: "", PrUrl: "", Round: 1},
			{
				Id: "p2", NodeId: "finance", Try: 2, Status: "proposed",
				PrState: "open", PrUrl: "https://x/pr/7",
				PullRequests: []*remediationv1.PullRequest{{Project: "core", PrState: "open", PrUrl: "https://x/pr/7"}},
				PrProjects:   []string{"core"},
				Round:        2,
			},
		},
	}}
	client := grpcadapter.NewFixesClient(fake)

	got, err := client.ListFixesForRelease(context.Background(), "rel-1")
	require.NoError(t, err)
	require.Equal(t, []ports.FixSummary{
		{ID: "p1", NodeID: "finance", Try: 1, Status: "failed", Round: 1},
		{
			ID: "p2", NodeID: "finance", Try: 2, Status: "proposed",
			PullRequests: []ports.FixPR{{Project: "core", PRState: "open", PRURL: "https://x/pr/7"}},
			PRProjects:   []string{"core"},
			Round:        2,
		},
	}, got)

	require.NotNil(t, fake.gotReq)
	require.Equal(t, "rel-1", fake.gotReq.ReleaseId)
	// The retry decision must see every try: a capped page could let an
	// older open PR or proposed try fall off the page and be missed.
	require.EqualValues(t, 0, fake.gotReq.Limit)
}

func TestFixesClient_ListFixesForRelease_PropagatesError(t *testing.T) {
	fake := &fakeFixesClient{err: context.DeadlineExceeded}
	client := grpcadapter.NewFixesClient(fake)

	_, err := client.ListFixesForRelease(context.Background(), "rel-1")
	require.Error(t, err)
}
