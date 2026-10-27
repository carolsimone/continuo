package runservice

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
)

// ListSchedules returns every schedule of the tenant, merging the catalog's
// names, the cron of each and its last run.
func (s *Server) ListSchedules(ctx context.Context, _ *runv1.ListSchedulesRequest) (*runv1.ListSchedulesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	views, err := s.reads.Schedules.List(ctx, tenantID)
	if err != nil {
		return nil, s.internalError("ListSchedules", err)
	}
	out := make([]*runv1.ScheduleSummary, 0, len(views))
	for _, v := range views {
		sum := &runv1.ScheduleSummary{
			ScheduleName:   v.Name,
			CronExpression: v.Cron,
			Description:    v.Description,
			Timezone:       v.Timezone,
			HasActiveRun:   v.HasActiveRun(),
		}
		if v.LastRun != nil {
			sum.LastRunAt = timestamppb.New(v.LastRun.CreatedAt)
			sum.LastRunStatus = string(v.LastRun.Status)
			sum.LastRunId = v.LastRun.RunID.String()
		}
		out = append(out, sum)
	}
	return &runv1.ListSchedulesResponse{Schedules: out}, nil
}
