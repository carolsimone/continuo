package grpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	releasev1 "github.com/carolsimone/continuo/release-controller/api/release/v1"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeRunRepo answers Get from a map and fails it for the id errID.
type fakeRunRepo struct {
	repository.RunRepository
	runs  map[string]*pipeline.Run
	errID string
}

func (f fakeRunRepo) Get(_ context.Context, id string) (*pipeline.Run, error) {
	if id == f.errID {
		return nil, errors.New("database down")
	}
	return f.runs[id], nil
}

type fakeUoW struct {
	uow.UnitOfWork
	runs repository.RunRepository
}

func (f fakeUoW) RunRepo() repository.RunRepository { return f.runs }

func designNotice() release.Notice {
	return release.Notice{
		Kind: "seed_reload", NodeID: "analytics.seed_fx_rates_eur", Service: "finance",
		EngineFamily: warehouse.SchemaBound, Change: seedreload.ChangeColumns,
		Columns: release.ColumnChanges{Added: []string{"source"}},
		Load:    seedreload.Load{Operation: model.OperationRun, DropsTable: true, Reason: "columns changed"},
		Rebuilds: []seedreload.Rebuild{
			{Key: "analytics.fx_rates_current", Materialized: "view", Schedule: "hourly"},
		},
		RefreshOnSchedule: []seedreload.Boundary{{Key: "analytics.fx_transactions_eur", Materialized: "table", Schedule: "daily"}},
		Warehouse: release.WarehouseFindings{Checked: true, Completeness: "exact",
			OutsideContinuo: []release.OutsideObject{{Object: "bi.fx_rates_report", Kind: "view", Owner: "bi"}}},
		Prediction: "stops", StopReason: "unowned_dependents",
	}
}

func candidateWithNotices(id string, notices ...release.Notice) *pipeline.Run {
	r := pipeline.NewCandidate(id, "finance", "tag", false, false, "o/r", "sha", release.ManifestKindDbt, time.Unix(1, 0).UTC())
	r.SetNotices(notices)
	return r
}

// startServer serves the fake runs on a free port and returns a client of it.
func startServer(t *testing.T, repo fakeRunRepo) releasev1.ReleaseQueryClient {
	t.Helper()
	srv, err := NewReleaseQueryServer(0, func() uow.UnitOfWork { return fakeUoW{runs: repo} }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	go func() { _ = srv.Start() }()
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return releasev1.NewReleaseQueryClient(conn)
}

func TestGetReleaseNotices_ReturnsTheStoredNotices(t *testing.T) {
	c := startServer(t, fakeRunRepo{runs: map[string]*pipeline.Run{
		"rel-a1b2c3d-201": candidateWithNotices("rel-a1b2c3d-201", designNotice()),
	}})

	resp, err := c.GetReleaseNotices(context.Background(), &releasev1.GetReleaseNoticesRequest{ReleaseId: "rel-a1b2c3d-201"})

	require.NoError(t, err)
	assert.Equal(t, "rel-a1b2c3d-201", resp.ReleaseId)
	assert.Equal(t, string(pipeline.StatusReceived), resp.Status)
	require.Len(t, resp.Notices, 1)
	n := resp.Notices[0]
	assert.Equal(t, "seed_reload", n.Kind)
	assert.Equal(t, "analytics.seed_fx_rates_eur", n.NodeId)
	assert.Equal(t, "finance", n.Service)
	assert.Equal(t, "schema_bound", n.EngineFamily)
	assert.Equal(t, []string{"source"}, n.Columns.Added)
	assert.Equal(t, "run", n.Load.Operation)
	assert.True(t, n.Load.DropsTable)
	assert.Equal(t, "columns changed", n.Load.Reason)
	require.Len(t, n.Rebuilds, 1)
	assert.Equal(t, "analytics.fx_rates_current", n.Rebuilds[0].NodeId)
	assert.Equal(t, "run", n.Rebuilds[0].Operation)
	require.Len(t, n.RefreshOnSchedule, 1)
	assert.Equal(t, "daily", n.RefreshOnSchedule[0].Schedule)
	assert.True(t, n.Warehouse.Checked)
	assert.Equal(t, "exact", n.Warehouse.Completeness)
	assert.Equal(t, "bi.fx_rates_report", n.Warehouse.OutsideContinuo[0].Object)
	assert.Equal(t, "stops", n.Prediction)
	assert.Equal(t, "unowned_dependents", n.StopReason)
}

func TestGetReleaseNotices_ACandidateWithoutNoticesAnswersNone(t *testing.T) {
	c := startServer(t, fakeRunRepo{runs: map[string]*pipeline.Run{"rel-none-notice": candidateWithNotices("rel-none-notice")}})

	resp, err := c.GetReleaseNotices(context.Background(), &releasev1.GetReleaseNoticesRequest{ReleaseId: "rel-none-notice"})

	require.NoError(t, err)
	assert.Equal(t, "rel-none-notice", resp.ReleaseId)
	assert.Empty(t, resp.Notices)
}

func TestGetReleaseNotices_Refusals(t *testing.T) {
	verification := pipeline.NewVerification("verify-1", "finance", "tag", "rel-orig", 1, "", release.ManifestKindDbt, time.Unix(1, 0).UTC())
	verification.SetNotices([]release.Notice{designNotice()})
	c := startServer(t, fakeRunRepo{runs: map[string]*pipeline.Run{"verify-1": verification}, errID: "rel-broken"})
	ctx := context.Background()

	_, err := c.GetReleaseNotices(ctx, &releasev1.GetReleaseNoticesRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = c.GetReleaseNotices(ctx, &releasev1.GetReleaseNoticesRequest{ReleaseId: "rel-none"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	// A verification run's id is NOT_FOUND too, as GET /releases/{id} answers 404 for it.
	_, err = c.GetReleaseNotices(ctx, &releasev1.GetReleaseNoticesRequest{ReleaseId: "verify-1"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	// A failed read is the server's failure, not a missing release.
	_, err = c.GetReleaseNotices(ctx, &releasev1.GetReleaseNoticesRequest{ReleaseId: "rel-broken"})
	assert.Equal(t, codes.Internal, status.Code(err))
}

// Start returns nil once Shutdown stops a serving server, and an error
// wrapping grpc.ErrServerStopped when Shutdown came first; main exits on any
// other error of Start.
func TestReleaseQueryServer_StartAfterShutdownReturnsErrServerStopped(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	newUoW := func() uow.UnitOfWork { return fakeUoW{} }

	serving, err := NewReleaseQueryServer(0, newUoW, logger)
	require.NoError(t, err)
	served := make(chan error, 1)
	go func() { served <- serving.Start() }()
	require.Eventually(t, func() bool {
		conn, err := grpc.NewClient(serving.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return false
		}
		defer conn.Close()
		_, err = releasev1.NewReleaseQueryClient(conn).GetReleaseNotices(context.Background(), &releasev1.GetReleaseNoticesRequest{})
		return status.Code(err) != codes.Unavailable
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, serving.Shutdown(context.Background()))
	assert.NoError(t, <-served)

	stopped, err := NewReleaseQueryServer(0, newUoW, logger)
	require.NoError(t, err)
	require.NoError(t, stopped.Shutdown(context.Background()))
	assert.ErrorIs(t, stopped.Start(), grpc.ErrServerStopped)
}
