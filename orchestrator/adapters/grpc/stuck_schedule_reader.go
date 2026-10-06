package grpc

import (
	"context"
	"fmt"
	"time"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// stuckScheduleClient is the slice of the state gRPC client the watchdog needs.
type stuckScheduleClient interface {
	ListStuckCandidates(ctx context.Context, in *statev1.ListStuckCandidatesRequest, opts ...googlegrpc.CallOption) (*statev1.ListStuckCandidatesResponse, error)
	CancelScheduler(ctx context.Context, in *statev1.CancelSchedulerRequest, opts ...googlegrpc.CallOption) (*statev1.SchedulerResponse, error)
}

// StuckScheduleAdapter translates between the orchestrator's domain-typed
// watchdog ports and the state service's gRPC surface. It keeps proto/grpc
// wire types out of the application layer.
type StuckScheduleAdapter struct {
	client stuckScheduleClient
}

var (
	_ ports.StuckScheduleReader = (*StuckScheduleAdapter)(nil)
	_ ports.RunCanceller        = (*StuckScheduleAdapter)(nil)
)

// NewStuckScheduleAdapter constructs the adapter over the state gRPC client.
func NewStuckScheduleAdapter(client stuckScheduleClient) *StuckScheduleAdapter {
	return &StuckScheduleAdapter{client: client}
}

// ListStuckCandidates queries state for the active runs that have stopped making
// progress.
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

// CancelRun cancels one run by its id through state's CancelScheduler. state
// answers FailedPrecondition for a run already terminal and NotFound for an id
// it does not know; both are reported as ports.ErrRunNotCancellable. Any other
// error is returned unchanged.
func (a *StuckScheduleAdapter) CancelRun(ctx context.Context, runID, cancelledBy, reason string) error {
	_, err := a.client.CancelScheduler(ctx, &statev1.CancelSchedulerRequest{
		ScheduleId:         runID,
		CancelledBy:        cancelledBy,
		CancellationReason: reason,
	})
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.FailedPrecondition, codes.NotFound:
		return fmt.Errorf("%w: %s", ports.ErrRunNotCancellable, status.Convert(err).Message())
	default:
		return err
	}
}
