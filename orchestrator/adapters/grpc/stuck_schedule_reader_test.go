package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

type fakeStuckClient struct {
	listResp   *statev1.ListStuckCandidatesResponse
	listErr    error
	lastCutoff *timestamppb.Timestamp
	cancelReqs []*statev1.CancelScheduleRequest
	cancelErr  error
}

func (f *fakeStuckClient) ListStuckCandidates(_ context.Context, in *statev1.ListStuckCandidatesRequest, _ ...googlegrpc.CallOption) (*statev1.ListStuckCandidatesResponse, error) {
	f.lastCutoff = in.GetCutoff()
	return f.listResp, f.listErr
}

func (f *fakeStuckClient) CancelSchedule(_ context.Context, in *statev1.CancelScheduleRequest, _ ...googlegrpc.CallOption) (*statev1.CancelScheduleResponse, error) {
	f.cancelReqs = append(f.cancelReqs, in)
	return &statev1.CancelScheduleResponse{}, f.cancelErr
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

func TestStuckScheduleAdapter_CancelSchedule_ForwardsFields(t *testing.T) {
	fake := &fakeStuckClient{}
	a := NewStuckScheduleAdapter(fake)

	if err := a.CancelSchedule(context.Background(), "sched", "watchdog", "stalled"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.cancelReqs) != 1 {
		t.Fatalf("want 1 cancel, got %d", len(fake.cancelReqs))
	}
	req := fake.cancelReqs[0]
	if req.ScheduleName != "sched" || req.CancelledBy != "watchdog" || req.CancellationReason != "stalled" {
		t.Fatalf("fields not forwarded: %+v", req)
	}
}
