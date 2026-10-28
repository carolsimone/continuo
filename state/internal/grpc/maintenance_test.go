package grpc

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/pkg/maintenance"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func requireMaintenanceRefusal(t *testing.T, err error, trailer metadata.MD) {
	t.Helper()
	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	require.Equal(t, maintenance.Message, status.Convert(err).Message())
	require.Equal(t, []string{"true"}, trailer.Get(maintenance.TrailerKey))
}

// dialRunService dials a server whose RunService is the unimplemented stub, so
// a call that reaches its handler answers Unimplemented.
func dialRunService(t *testing.T, maintenanceOn bool) runv1.RunServiceClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := NewServer(0, runv1.UnimplementedRunServiceServer{}, logger, maintenanceOn)
	require.NoError(t, err)
	go func() { _ = srv.Start() }()
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	conn, err := grpclib.NewClient(srv.Addr(), grpclib.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return runv1.NewRunServiceClient(conn)
}

// runServiceStarts are the RunService commands that start a run, each called
// once on c.
var runServiceStarts = map[string]func(c runv1.RunServiceClient, md *metadata.MD) error{
	"StartRun": func(c runv1.RunServiceClient, md *metadata.MD) error {
		_, err := c.StartRun(context.Background(), &runv1.StartRunRequest{RequestId: "x", ScheduleName: "daily"}, grpclib.Trailer(md))
		return err
	},
	"TriggerRerun": func(c runv1.RunServiceClient, md *metadata.MD) error {
		_, err := c.TriggerRerun(context.Background(), &runv1.TriggerRerunRequest{}, grpclib.Trailer(md))
		return err
	},
	"TriggerRebase": func(c runv1.RunServiceClient, md *metadata.MD) error {
		_, err := c.TriggerRebase(context.Background(), &runv1.TriggerRebaseRequest{}, grpclib.Trailer(md))
		return err
	},
	"TriggerSingleNodeRun": func(c runv1.RunServiceClient, md *metadata.MD) error {
		_, err := c.TriggerSingleNodeRun(context.Background(), &runv1.TriggerSingleNodeRunRequest{}, grpclib.Trailer(md))
		return err
	},
}

// scheduleEdits are the RunService commands that change a schedule, each
// called once on c.
var scheduleEdits = map[string]func(c runv1.RunServiceClient, md *metadata.MD) error{
	"UpsertSchedule": func(c runv1.RunServiceClient, md *metadata.MD) error {
		_, err := c.UpsertSchedule(context.Background(), &runv1.UpsertScheduleRequest{ScheduleName: "daily", Cron: "0 1 * * *", Timezone: "UTC"}, grpclib.Trailer(md))
		return err
	},
	"DeleteSchedule": func(c runv1.RunServiceClient, md *metadata.MD) error {
		_, err := c.DeleteSchedule(context.Background(), &runv1.DeleteScheduleRequest{ScheduleName: "daily"}, grpclib.Trailer(md))
		return err
	},
}

// Every gated method is a command that starts a run or changes a schedule.
func TestMaintenance_GatesExactlyTheStartsAndTheScheduleEdits(t *testing.T) {
	require.Len(t, MaintenanceGatedMethods, len(runServiceStarts)+len(scheduleEdits),
		"every gated method has a case in runServiceStarts or scheduleEdits")
}

// The RunService commands that start a run are refused during maintenance
// before their handler runs; its cancels and PreviewRebase reach the handler,
// which here answers Unimplemented.
func TestMaintenance_RefusesRunServiceCommands(t *testing.T) {
	c := dialRunService(t, true)
	for name, call := range runServiceStarts {
		t.Run(name, func(t *testing.T) {
			var md metadata.MD
			requireMaintenanceRefusal(t, call(c, &md), md)
		})
	}

	ctx := context.Background()
	reached := map[string]func(md *metadata.MD) error{
		"CancelRun": func(md *metadata.MD) error {
			_, err := c.CancelRun(ctx, &runv1.CancelRunRequest{}, grpclib.Trailer(md))
			return err
		},
		"CancelActiveRun": func(md *metadata.MD) error {
			_, err := c.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{}, grpclib.Trailer(md))
			return err
		},
		"PreviewRebase": func(md *metadata.MD) error {
			_, err := c.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{}, grpclib.Trailer(md))
			return err
		},
	}
	for name, call := range reached {
		t.Run(name, func(t *testing.T) {
			var md metadata.MD
			err := call(&md)
			require.Equal(t, codes.Unimplemented, status.Code(err), "%s reaches its handler during maintenance: %v", name, err)
			require.Empty(t, md.Get(maintenance.TrailerKey))
		})
	}
}

// UpsertSchedule and DeleteSchedule are refused during maintenance before
// their handler runs; PreviewSchedule and ListSchedules are reads and reach
// the handler, which here answers Unimplemented.
func TestMaintenance_RefusesScheduleEdits(t *testing.T) {
	c := dialRunService(t, true)
	for name, call := range scheduleEdits {
		t.Run(name, func(t *testing.T) {
			var md metadata.MD
			requireMaintenanceRefusal(t, call(c, &md), md)
		})
	}

	ctx := context.Background()
	reached := map[string]func(md *metadata.MD) error{
		"PreviewSchedule": func(md *metadata.MD) error {
			_, err := c.PreviewSchedule(ctx, &runv1.PreviewScheduleRequest{Cron: "0 1 * * *", Timezone: "UTC"}, grpclib.Trailer(md))
			return err
		},
		"ListSchedules": func(md *metadata.MD) error {
			_, err := c.ListSchedules(ctx, &runv1.ListSchedulesRequest{}, grpclib.Trailer(md))
			return err
		},
	}
	for name, call := range reached {
		t.Run(name, func(t *testing.T) {
			var md metadata.MD
			err := call(&md)
			require.Equal(t, codes.Unimplemented, status.Code(err), "%s reaches its handler during maintenance: %v", name, err)
			require.Empty(t, md.Get(maintenance.TrailerKey))
		})
	}
}

// With maintenance off a RunService command reaches its handler.
func TestMaintenance_OffReachesTheRunServiceHandler(t *testing.T) {
	c := dialRunService(t, false)
	for _, cases := range []map[string]func(c runv1.RunServiceClient, md *metadata.MD) error{runServiceStarts, scheduleEdits} {
		for name, call := range cases {
			var md metadata.MD
			require.Equal(t, codes.Unimplemented, status.Code(call(c, &md)), name)
		}
	}
}
