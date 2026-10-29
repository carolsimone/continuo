package runservice

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/run-controller/domain/schedule"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/carolsimone/continuo/run-controller/service/schedules"
)

// UpsertSchedule creates or replaces the cron of the request's tenant's
// schedule, as an edit by the caller's identity, and answers with the schedule
// as saved and its next fires.
func (s *Server) UpsertSchedule(ctx context.Context, req *runv1.UpsertScheduleRequest) (*runv1.UpsertScheduleResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	saved, next, err := s.schedules.Upsert(ctx, tenantID, schedules.UpsertInput{
		Name: req.GetScheduleName(), Cron: req.GetCron(), Timezone: req.GetTimezone(),
		Description: req.GetDescription(), Paused: req.GetPaused(), ExpectedVersion: req.GetExpectedVersion(),
	}, identity.FromContext(ctx).UserID)
	if err != nil {
		return nil, s.scheduleError("UpsertSchedule", err, "schedule_name", req.GetScheduleName())
	}
	return &runv1.UpsertScheduleResponse{Schedule: toProtoSchedule(saved), NextFireTimes: toTimestamps(next)}, nil
}

// DeleteSchedule removes the cron of the request's tenant's schedule.
func (s *Server) DeleteSchedule(ctx context.Context, req *runv1.DeleteScheduleRequest) (*runv1.DeleteScheduleResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.schedules.Delete(ctx, tenantID, req.GetScheduleName()); err != nil {
		return nil, s.scheduleError("DeleteSchedule", err, "schedule_name", req.GetScheduleName())
	}
	return &runv1.DeleteScheduleResponse{}, nil
}

// PreviewSchedule reads a cron as UpsertSchedule would and answers with it in
// words and its next fires, writing nothing.
func (s *Server) PreviewSchedule(ctx context.Context, req *runv1.PreviewScheduleRequest) (*runv1.PreviewScheduleResponse, error) {
	if _, err := requestTenant(ctx); err != nil {
		return nil, err
	}
	words, next, err := s.schedules.Preview(ctx, req.GetCron(), req.GetTimezone())
	if err != nil {
		return nil, s.scheduleError("PreviewSchedule", err, "cron", req.GetCron(), "timezone", req.GetTimezone())
	}
	return &runv1.PreviewScheduleResponse{CronWords: words, NextFireTimes: toTimestamps(next)}, nil
}

// ListSchedules returns every schedule name of the tenant that its live
// release has or that has a cron, each with its cron, its models and its last
// run.
func (s *Server) ListSchedules(ctx context.Context, _ *runv1.ListSchedulesRequest) (*runv1.ListSchedulesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	summaries, err := s.schedules.List(ctx, tenantID)
	if err != nil {
		return nil, s.internalError("ListSchedules", err)
	}
	out := make([]*runv1.ScheduleSummary, 0, len(summaries))
	for _, v := range summaries {
		sum := &runv1.ScheduleSummary{
			ScheduleName: v.Name,
			HasActiveRun: v.HasActiveRun(),
			ModelCount:   num.ClampInt32(v.ModelCount),
			CronWords:    v.Words,
		}
		if c := v.Schedule; c != nil {
			sum.Cron, sum.Timezone, sum.Description, sum.Paused = c.Cron(), c.Timezone(), c.Description(), c.Paused()
			sum.NextFireAt = nextFireAt(c)
			sum.Version, sum.UpdatedBy, sum.UpdatedAt = c.Version(), c.UpdatedBy(), timestamppb.New(c.UpdatedAt())
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

// scheduleError maps the error of a schedule call to the status the RPC
// returns: a refused input is InvalidArgument with the refusal's own message,
// a first cron for a name no model of the live release has is NotFound, and an
// expected version the schedule is not at is Aborted. Anything else is logged
// and Internal.
func (s *Server) scheduleError(rpc string, err error, attrs ...any) error {
	switch {
	case errors.Is(err, schedules.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, schedule.ErrUnknownName):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, schedule.ErrStaleVersion):
		return status.Error(codes.Aborted, schedule.ErrStaleVersion.Error())
	}
	return s.internalError(rpc, err, attrs...)
}

func toProtoSchedule(c *schedule.Schedule) *runv1.Schedule {
	return &runv1.Schedule{
		ScheduleName: c.Name(),
		Cron:         c.Cron(),
		Timezone:     c.Timezone(),
		Description:  c.Description(),
		Paused:       c.Paused(),
		NextFireAt:   nextFireAt(c),
		Version:      c.Version(),
		UpdatedBy:    c.UpdatedBy(),
		UpdatedAt:    timestamppb.New(c.UpdatedAt()),
	}
}

// nextFireAt is the schedule's next fire, unset when it has none.
func nextFireAt(c *schedule.Schedule) *timestamppb.Timestamp {
	next, ok := c.NextFireAt()
	if !ok {
		return nil
	}
	return timestamppb.New(next)
}

func toTimestamps(ts []time.Time) []*timestamppb.Timestamp {
	out := make([]*timestamppb.Timestamp, 0, len(ts))
	for _, t := range ts {
		out = append(out, timestamppb.New(t))
	}
	return out
}
