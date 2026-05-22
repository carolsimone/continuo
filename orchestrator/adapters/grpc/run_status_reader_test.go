package grpc

import (
	"context"
	"errors"
	"testing"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

type fakeSchedulerGetter struct {
	resp *statev1.SchedulerResponse
	err  error
}

func (f *fakeSchedulerGetter) GetScheduler(context.Context, *statev1.GetSchedulerRequest, ...googlegrpc.CallOption) (*statev1.SchedulerResponse, error) {
	return f.resp, f.err
}

func respWith(s statev1.SchedulerStatus) *statev1.SchedulerResponse {
	return &statev1.SchedulerResponse{Scheduler: &statev1.Scheduler{Status: s}}
}

func TestRunStatusReader_TerminalMapping(t *testing.T) {
	cases := []struct {
		name       string
		status     statev1.SchedulerStatus
		wantStatus string
		wantTerm   bool
	}{
		{"succeeded", statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED, "succeeded", true},
		{"failed", statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED, "failed", true},
		{"cancelled", statev1.SchedulerStatus_SCHEDULER_STATUS_CANCELLED, "cancelled", true},
		{"running", statev1.SchedulerStatus_SCHEDULER_STATUS_RUNNING, "", false},
		{"pending", statev1.SchedulerStatus_SCHEDULER_STATUS_PENDING, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := NewRunStatusReader(&fakeSchedulerGetter{resp: respWith(c.status)})
			got, term, err := r.GetTerminalStatus(context.Background(), "run-1")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.wantStatus || term != c.wantTerm {
				t.Fatalf("got (%q,%v), want (%q,%v)", got, term, c.wantStatus, c.wantTerm)
			}
		})
	}
}

func TestRunStatusReader_NotFoundIsNotTerminal(t *testing.T) {
	r := NewRunStatusReader(&fakeSchedulerGetter{err: status.Error(codes.NotFound, "no such run")})
	got, term, err := r.GetTerminalStatus(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("NotFound must not be an error, got %v", err)
	}
	if got != "" || term {
		t.Fatalf("NotFound must be (\"\",false), got (%q,%v)", got, term)
	}
}

func TestRunStatusReader_OtherErrorPropagates(t *testing.T) {
	want := errors.New("state down")
	r := NewRunStatusReader(&fakeSchedulerGetter{err: want})
	_, _, err := r.GetTerminalStatus(context.Background(), "run-1")
	if !errors.Is(err, want) {
		t.Fatalf("expected propagated error, got %v", err)
	}
}
