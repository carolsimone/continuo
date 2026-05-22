package grpc

import (
	"context"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// schedulerGetter is the slice of the state gRPC client RunStatusReader needs.
type schedulerGetter interface {
	GetScheduler(ctx context.Context, in *statev1.GetSchedulerRequest, opts ...googlegrpc.CallOption) (*statev1.SchedulerResponse, error)
}

// RunStatusReader reads a run's terminal lifecycle status from the state
// service over gRPC. run_id is the state schedule_id.
type RunStatusReader struct {
	client schedulerGetter
}

var _ ports.RunStatusReader = (*RunStatusReader)(nil)

// NewRunStatusReader constructs a RunStatusReader over the state gRPC client.
func NewRunStatusReader(client schedulerGetter) *RunStatusReader {
	return &RunStatusReader{client: client}
}

// GetTerminalStatus maps state's scheduler status to a terminal outcome string.
// A run state does not know about (NotFound) is reported as not terminal so the
// reconciler simply skips it rather than treating it as an error.
func (r *RunStatusReader) GetTerminalStatus(ctx context.Context, runID string) (string, bool, error) {
	resp, err := r.client.GetScheduler(ctx, &statev1.GetSchedulerRequest{ScheduleId: runID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", false, nil
		}
		return "", false, err
	}
	switch resp.GetScheduler().GetStatus() {
	case statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED:
		return "succeeded", true, nil
	case statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED:
		return "failed", true, nil
	case statev1.SchedulerStatus_SCHEDULER_STATUS_CANCELLED:
		return "cancelled", true, nil
	default:
		return "", false, nil
	}
}
