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

// fakeProposalsClient implements only ListProposals; embedding the interface
// satisfies the rest of remediationv1.RemediationProposalsClient without
// needing stub methods this test never calls.
type fakeProposalsClient struct {
	remediationv1.RemediationProposalsClient
	gotReq *remediationv1.ListProposalsRequest
	resp   *remediationv1.ListProposalsResponse
	err    error
}

func (f *fakeProposalsClient) ListProposals(_ context.Context, in *remediationv1.ListProposalsRequest, _ ...grpc.CallOption) (*remediationv1.ListProposalsResponse, error) {
	f.gotReq = in
	return f.resp, f.err
}

func TestProposalsClient_ListProposalsForRelease_MapsSummaries(t *testing.T) {
	fake := &fakeProposalsClient{resp: &remediationv1.ListProposalsResponse{
		Proposals: []*remediationv1.Proposal{
			{Id: "p1", NodeId: "finance", Attempt: 1, Status: "failed", PrState: "", PrUrl: "", RemediationRound: 1},
			{Id: "p2", NodeId: "finance", Attempt: 2, Status: "proposed", PrState: "open", PrUrl: "https://x/pr/7", RemediationRound: 2},
		},
	}}
	client := grpcadapter.NewProposalsClient(fake)

	got, err := client.ListProposalsForRelease(context.Background(), "rel-1")
	require.NoError(t, err)
	require.Equal(t, []ports.ProposalSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "failed", RemediationRound: 1},
		{ID: "p2", NodeID: "finance", Attempt: 2, Status: "proposed", PRState: "open", PRURL: "https://x/pr/7", RemediationRound: 2},
	}, got)

	require.NotNil(t, fake.gotReq)
	require.Equal(t, "rel-1", fake.gotReq.ReleaseId)
	require.EqualValues(t, 500, fake.gotReq.Limit)
}

func TestProposalsClient_ListProposalsForRelease_PropagatesError(t *testing.T) {
	fake := &fakeProposalsClient{err: context.DeadlineExceeded}
	client := grpcadapter.NewProposalsClient(fake)

	_, err := client.ListProposalsForRelease(context.Background(), "rel-1")
	require.Error(t, err)
}
