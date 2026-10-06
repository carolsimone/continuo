package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

type fakeStuckClient struct {
	listResp   *statev1.ListStuckCandidatesResponse
	listErr    error
	lastCutoff *timestamppb.Timestamp
	cancelReqs []*statev1.CancelSchedulerRequest
	cancelErr  error
}

func (f *fakeStuckClient) ListStuckCandidates(_ context.Context, in *statev1.ListStuckCandidatesRequest, _ ...googlegrpc.CallOption) (*statev1.ListStuckCandidatesResponse, error) {
	f.lastCutoff = in.GetCutoff()
	return f.listResp, f.listErr
}

func (f *fakeStuckClient) CancelScheduler(_ context.Context, in *statev1.CancelSchedulerRequest, _ ...googlegrpc.CallOption) (*statev1.SchedulerResponse, error) {
	f.cancelReqs = append(f.cancelReqs, in)
	if f.cancelErr != nil {
		return nil, f.cancelErr
	}
	return &statev1.SchedulerResponse{}, nil
}

func TestStuckScheduleAdapter_ListStuckCandidates_MapsCutoffAndCandidates(t *testing.T) {
	cutoff := time.Now().Add(-30 * time.Minute)
	fake := &fakeStuckClient{
		listResp: &statev1.ListStuckCandidatesResponse{
			Candidates: []*statev1.StuckCandidate{
				{ScheduleName: "a", ScheduleId: "id-a"},
				{ScheduleName: "b", ScheduleId: "id-b"},
			},
		},
	}
	a := NewStuckScheduleAdapter(fake)

	got, err := a.ListStuckCandidates(context.Background(), cutoff)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.lastCutoff == nil || !fake.lastCutoff.AsTime().Equal(cutoff.UTC()) {
		t.Fatalf("cutoff not forwarded: got %v want %v", fake.lastCutoff.AsTime(), cutoff)
	}
	if len(got) != 2 || got[0].ScheduleName != "a" || got[0].RunID != "id-a" {
		t.Fatalf("unexpected mapping: %+v", got)
	}
}

func TestStuckScheduleAdapter_ListStuckCandidates_ErrorPropagates(t *testing.T) {
	want := errors.New("state down")
	a := NewStuckScheduleAdapter(&fakeStuckClient{listErr: want})
	if _, err := a.ListStuckCandidates(context.Background(), time.Now()); !errors.Is(err, want) {
		t.Fatalf("expected propagated error, got %v", err)
	}
}

func TestStuckScheduleAdapter_CancelRun_ForwardsFields(t *testing.T) {
	fake := &fakeStuckClient{}
	a := NewStuckScheduleAdapter(fake)

	if err := a.CancelRun(context.Background(), "run-1", "watchdog", "stalled"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.cancelReqs) != 1 {
		t.Fatalf("want 1 cancel, got %d", len(fake.cancelReqs))
	}
	req := fake.cancelReqs[0]
	if req.ScheduleId != "run-1" || req.CancelledBy != "watchdog" || req.CancellationReason != "stalled" {
		t.Fatalf("fields not forwarded: %+v", req)
	}
}

// state answers FailedPrecondition for a run already terminal and NotFound for
// an unknown id; both mean there is nothing to cancel. Any other failure is
// returned as it came.
func TestStuckScheduleAdapter_CancelRun_MapsNothingToCancelToSentinel(t *testing.T) {
	cases := []struct {
		code         codes.Code
		notCancelled bool
	}{
		{codes.FailedPrecondition, true},
		{codes.NotFound, true},
		{codes.Unavailable, false},
		{codes.InvalidArgument, false},
	}
	for _, c := range cases {
		t.Run(c.code.String(), func(t *testing.T) {
			a := NewStuckScheduleAdapter(&fakeStuckClient{cancelErr: status.Error(c.code, "from state")})
			err := a.CancelRun(context.Background(), "run-1", "watchdog", "stalled")
			if err == nil {
				t.Fatal("want an error")
			}
			if got := errors.Is(err, ports.ErrRunNotCancellable); got != c.notCancelled {
				t.Fatalf("errors.Is(err, ErrRunNotCancellable) = %v, want %v (err: %v)", got, c.notCancelled, err)
			}
			if !c.notCancelled && status.Code(err) != c.code {
				t.Fatalf("status code = %v, want %v preserved", status.Code(err), c.code)
			}
		})
	}
}
