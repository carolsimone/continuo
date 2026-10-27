package grpc

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/pkg/maintenance"
	"github.com/carolsimone/continuo/state/internal/grpc/handlers"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func dialState(t *testing.T, maintenanceOn bool) statev1.StateServiceClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Only RerunHandler and PreviewRebaseHandler are real: their argument
	// validation answers before their use case is touched, which is all
	// reaching a handler needs.
	srv, err := NewServer(0, nil, nil, nil, handlers.NewRerunHandler(nil, nil, logger), nil, nil,
		handlers.NewPreviewRebaseHandler(nil, logger), nil, nil, runv1.UnimplementedRunServiceServer{}, logger, maintenanceOn)
	require.NoError(t, err)
	go func() { _ = srv.Start() }()
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	conn, err := grpclib.NewClient(srv.Addr(), grpclib.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return statev1.NewStateServiceClient(conn)
}

func requireMaintenanceRefusal(t *testing.T, err error, trailer metadata.MD) {
	t.Helper()
	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	require.Equal(t, maintenance.Message, status.Convert(err).Message())
	require.Equal(t, []string{"true"}, trailer.Get(maintenance.TrailerKey))
}

// Every RPC that starts a run is refused during maintenance before its handler
// runs (the handlers here are nil, so reaching one would panic).
func TestMaintenance_RefusesEveryRunStartingRPC(t *testing.T) {
	c := dialState(t, true)
	ctx := context.Background()
	calls := map[string]func(md *metadata.MD) error{
		"TriggerSchedule": func(md *metadata.MD) error {
			_, err := c.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: "daily"}, grpclib.Trailer(md))
			return err
		},
		"ActivateSchedule": func(md *metadata.MD) error {
			_, err := c.ActivateSchedule(ctx, &statev1.ActivateScheduleRequest{}, grpclib.Trailer(md))
			return err
		},
		"TriggerRerun": func(md *metadata.MD) error {
			_, err := c.TriggerRerun(ctx, &statev1.TriggerRerunRequest{SourceRunId: "x"}, grpclib.Trailer(md))
			return err
		},
		"TriggerRebase": func(md *metadata.MD) error {
			_, err := c.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{SourceRunId: "x"}, grpclib.Trailer(md))
			return err
		},
		"TriggerSingleNodeRun": func(md *metadata.MD) error {
			_, err := c.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{}, grpclib.Trailer(md))
			return err
		},
	}
	require.Len(t, MaintenanceGatedMethods, len(calls)+len(runServiceStarts), "every gated method has a case here or in runServiceStarts")
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			var md metadata.MD
			requireMaintenanceRefusal(t, call(&md), md)
		})
	}
}

func TestMaintenance_OffReachesTheHandler(t *testing.T) {
	c := dialState(t, false)
	_, err := c.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "the handler's own validation answers when maintenance is off")
}

// PreviewRebase is a read: maintenance mode does not refuse it.
func TestMaintenance_PreviewRebaseReachesTheHandler(t *testing.T) {
	c := dialState(t, true)
	var md metadata.MD
	_, err := c.PreviewRebase(context.Background(), &statev1.PreviewRebaseRequest{}, grpclib.Trailer(&md))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "the handler's own validation answers during maintenance")
	require.Empty(t, md.Get(maintenance.TrailerKey))
}

// dialRunService dials a server whose RunService is the unimplemented stub, so
// a call that reaches its handler answers Unimplemented.
func dialRunService(t *testing.T, maintenanceOn bool) runv1.RunServiceClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := NewServer(0, nil, nil, nil, nil, nil, nil, nil, nil, nil, runv1.UnimplementedRunServiceServer{}, logger, maintenanceOn)
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

// With maintenance off a RunService command reaches its handler.
func TestMaintenance_OffReachesTheRunServiceHandler(t *testing.T) {
	c := dialRunService(t, false)
	for name, call := range runServiceStarts {
		var md metadata.MD
		require.Equal(t, codes.Unimplemented, status.Code(call(c, &md)), name)
	}
}
