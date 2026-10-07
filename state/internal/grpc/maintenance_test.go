package grpc

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/pkg/maintenance"
	"github.com/carolsimone/continuo/state/internal/grpc/handlers"
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
	// Only RerunHandler is real: its argument validation answers before its
	// use case is touched, which is all the "off" case needs.
	srv, err := NewServer(0, nil, nil, nil, handlers.NewRerunHandler(nil, nil, logger), nil, nil, nil, logger, maintenanceOn)
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
	require.Len(t, MaintenanceGatedMethods, len(calls), "every gated method has a case here")
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
