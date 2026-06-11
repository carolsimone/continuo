package grpc

import (
	"context"
	"time"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// stuckScheduleClient is the slice of the state gRPC client the watchdog needs.
type stuckScheduleClient interface {
	ListStuckCandidates(ctx context.Context, in *statev1.ListStuckCandidatesRequest, opts ...googlegrpc.CallOption) (*statev1.ListStuckCandidatesResponse, error)
	CancelSchedule(ctx context.Context, in *statev1.CancelScheduleRequest, opts ...googlegrpc.CallOption) (*statev1.CancelScheduleResponse, error)
}

// StuckScheduleAdapter translates between the orchestrator's domain-typed
// watchdog ports and the state service's gRPC surface. It keeps proto/grpc
// wire types out of the application layer.
type StuckScheduleAdapter struct {
	client stuckScheduleClient
}

var (
	_ ports.StuckScheduleReader = (*StuckScheduleAdapter)(nil)
	_ ports.ScheduleCanceller   = (*StuckScheduleAdapter)(nil)
)

// NewStuckScheduleAdapter constructs the adapter over the state gRPC client.
func NewStuckScheduleAdapter(client stuckScheduleClient) *StuckScheduleAdapter {
	return &StuckScheduleAdapter{client: client}
}

// ListStuckCandidates queries state for active runs whose dispatch has stalled.
func (a *StuckScheduleAdapter) ListStuckCandidates(ctx context.Context, cutoff time.Time) ([]ports.StuckSchedule, error) {
	resp, err := a.client.ListStuckCandidates(ctx, &statev1.ListStuckCandidatesRequest{
		Cutoff: timestamppb.New(cutoff),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ports.StuckSchedule, 0, len(resp.GetCandidates()))
	for _, c := range resp.GetCandidates() {
		out = append(out, ports.StuckSchedule{
			ScheduleName: c.GetScheduleName(),
			RunID:        c.GetScheduleId(),
		})
	}
	return out, nil
}

// CancelSchedule cancels the active run of the named schedule via state.
func (a *StuckScheduleAdapter) CancelSchedule(ctx context.Context, scheduleName, cancelledBy, reason string) error {
	_, err := a.client.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
		ScheduleName:       scheduleName,
		CancelledBy:        cancelledBy,
		CancellationReason: reason,
	})
	return err
}
