package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger logs errors only, to stderr.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fakeTopologyRepo records the calls the handler makes, in order.
type fakeTopologyRepo struct {
	calls   []string
	live    topology.Pointer
	loaded  bool
	saveErr error
	saved   []topology.Release
	moved   []topology.Pointer
}

var _ repository.TopologyRepository = (*fakeTopologyRepo)(nil)

func (f *fakeTopologyRepo) LockTopology(context.Context) error {
	f.calls = append(f.calls, "lock")
	return nil
}

func (f *fakeTopologyRepo) SaveRelease(_ context.Context, rel topology.Release) (bool, error) {
	f.calls = append(f.calls, "save")
	if f.saveErr != nil {
		return false, f.saveErr
	}
	f.saved = append(f.saved, rel)
	return f.loaded, nil
}

func (f *fakeTopologyRepo) LivePointer(context.Context, string) (topology.Pointer, error) {
	f.calls = append(f.calls, "live")
	return f.live, nil
}

func (f *fakeTopologyRepo) LivePointers(context.Context) ([]topology.Pointer, error) {
	return nil, errors.New("LivePointers is not used by the handlers")
}

func (f *fakeTopologyRepo) SetLivePointer(_ context.Context, _ string, p topology.Pointer, _ time.Time) error {
	f.calls = append(f.calls, "move")
	f.moved = append(f.moved, p)
	return nil
}

func (f *fakeTopologyRepo) ReleaseNodes(context.Context, string, string, string) ([]topology.Node, error) {
	return nil, nil
}

func (f *fakeTopologyRepo) ScheduleGraph(context.Context, string, string, string) (topology.Graph, error) {
	return topology.Graph{}, nil
}
func (f *fakeTopologyRepo) Graph(context.Context, string, string, []topology.NodeKey) (topology.Graph, error) {
	return topology.Graph{}, nil
}

func (f *fakeTopologyRepo) NodeContentHash(context.Context, string, string, topology.NodeKey) (string, bool, error) {
	return "", false, nil
}

func (f *fakeTopologyRepo) PruneReleases(context.Context, time.Time) (int64, error) { return 0, nil }

// fakeMaterializationRepo records DeleteAbsentFrom calls as "tenant/release".
type fakeMaterializationRepo struct{ deletedFor []string }

var _ repository.MaterializationRepository = (*fakeMaterializationRepo)(nil)

func (f *fakeMaterializationRepo) List(context.Context, string) (map[topology.NodeKey]seeds.Materialization, error) {
	return nil, nil
}

func (f *fakeMaterializationRepo) RecordAttempt(context.Context, string, topology.NodeKey, string, uuid.UUID, time.Time) error {
	return nil
}

func (f *fakeMaterializationRepo) RecordBuilt(context.Context, string, seeds.Built) error { return nil }

func (f *fakeMaterializationRepo) DeleteAbsentFrom(_ context.Context, tenantID, releaseID string) (int64, error) {
	f.deletedFor = append(f.deletedFor, tenantID+"/"+releaseID)
	return 0, nil
}

// fakeArtifactReader returns artifact or err and remembers the URI it read.
type fakeArtifactReader struct {
	artifact ports.TopologyArtifact
	err      error
	uri      string
}

func (f *fakeArtifactReader) Load(_ context.Context, uri, _ string) (ports.TopologyArtifact, error) {
	f.uri = uri
	return f.artifact, f.err
}

// recordingSeedReconciler records the triggers it receives and returns err.
type recordingSeedReconciler struct {
	triggers []ports.PromotionTrigger
	err      error
}

func (r *recordingSeedReconciler) AfterPromotion(_ context.Context, _ uow.UnitOfWork, trigger ports.PromotionTrigger) error {
	r.triggers = append(r.triggers, trigger)
	return r.err
}

func promotedRelease(releaseID string, seq int64) events.ReleasePromoted {
	return events.ReleasePromoted{
		TenantID:       "default",
		ReleaseID:      releaseID,
		PromotionSeq:   seq,
		TopologyURI:    "s3://continuo/tenants/default/topologies/" + releaseID + "/topology.json.gz",
		TopologySHA256: "abc",
		ChangedNodeIDs: []string{"raw.orders"},
	}
}

func seedSpec(table string) topology.NodeSpec {
	zero := 0
	return topology.NodeSpec{
		UniqueID: "raw." + table, ServiceName: "svc", SchemaName: "raw", TableName: table,
		NodeType: "dbt-seed", TestCount: &zero, ContentHash: "h-" + table, ImageTag: "svc:1",
	}
}

func promotedUoW(topo *fakeTopologyRepo, mats *fakeMaterializationRepo) *uow.FakeUnitOfWork {
	return &uow.FakeUnitOfWork{TopologyRepo: topo, MaterializationRepo: mats}
}

func TestReleasePromotedHandler_LoadFillsTheNodes(t *testing.T) {
	reader := &fakeArtifactReader{artifact: ports.TopologyArtifact{
		TenantID: "default", ReleaseID: "rel-1", Nodes: []topology.NodeSpec{seedSpec("orders")},
	}}
	h := handlers.NewReleasePromotedHandler(reader, nil, testLogger())

	in, err := h.Load(context.Background(), handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-1", 1)})
	require.NoError(t, err)
	assert.Equal(t, promotedRelease("rel-1", 1), in.Promotion)
	assert.Equal(t, []topology.NodeSpec{seedSpec("orders")}, in.Nodes)
	assert.Equal(t, promotedRelease("rel-1", 1).TopologyURI, reader.uri)
}

func TestReleasePromotedHandler_LoadClassifiesArtifactFailures(t *testing.T) {
	outage := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	cases := []struct {
		name      string
		reader    *fakeArtifactReader
		permanent bool
		cause     error
	}{
		{"a corrupt artifact is permanent",
			&fakeArtifactReader{err: fmt.Errorf("%w: checksum mismatch", ports.ErrTopologyArtifactCorrupt)}, true, ports.ErrTopologyArtifactCorrupt},
		{"an artifact of another release is permanent",
			&fakeArtifactReader{artifact: ports.TopologyArtifact{TenantID: "default", ReleaseID: "rel-other"}}, true, pkgevents.ErrPermanent},
		{"a missing artifact is retried",
			&fakeArtifactReader{err: fmt.Errorf("%w: s3://continuo/k", ports.ErrTopologyArtifactNotFound)}, false, ports.ErrTopologyArtifactNotFound},
		{"an outage keeps its cause",
			&fakeArtifactReader{err: fmt.Errorf("read: %w", outage)}, false, outage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := handlers.NewReleasePromotedHandler(tc.reader, nil, testLogger())
			_, err := h.Load(context.Background(), handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-1", 1)})
			require.Error(t, err)
			assert.Equal(t, tc.permanent, errors.Is(err, pkgevents.ErrPermanent))
			assert.ErrorIs(t, err, tc.cause)
		})
	}
}

func TestReleasePromotedHandler_FirstPromotionMovesThePointer(t *testing.T) {
	topo := &fakeTopologyRepo{loaded: true}
	mats := &fakeMaterializationRepo{}
	reconciler := &recordingSeedReconciler{}
	h := handlers.NewReleasePromotedHandler(&fakeArtifactReader{}, reconciler, testLogger())
	msgProcID := uuid.New()
	in := handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-1", 3), Nodes: []topology.NodeSpec{seedSpec("orders")}}

	require.NoError(t, h.Handle(context.Background(), promotedUoW(topo, mats), in, msgProcID))

	assert.Equal(t, []string{"lock", "save", "live", "move"}, topo.calls, "the lock is taken before anything is written")
	require.Len(t, topo.saved, 1)
	assert.Equal(t, topology.KeyOf("raw", "orders"), topo.saved[0].Nodes[0].Key)
	assert.Equal(t, []topology.Pointer{{ReleaseID: "rel-1", PromotionSeq: 3}}, topo.moved)
	assert.Equal(t, []string{"default/rel-1"}, mats.deletedFor)
	assert.Equal(t, []ports.PromotionTrigger{{
		TenantID: "default", ReleaseID: "rel-1", PromotionSeq: 3, Outcome: topology.PointerMoved,
		FirstPromotion: true, ChangedNodeIDs: []string{"raw.orders"}, MessageProcessingID: msgProcID,
	}}, reconciler.triggers)
}

func TestReleasePromotedHandler_HigherSeqMovesALivePointer(t *testing.T) {
	topo := &fakeTopologyRepo{live: topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 3}, loaded: true}
	mats := &fakeMaterializationRepo{}
	reconciler := &recordingSeedReconciler{}
	h := handlers.NewReleasePromotedHandler(&fakeArtifactReader{}, reconciler, testLogger())

	require.NoError(t, h.Handle(context.Background(), promotedUoW(topo, mats),
		handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-2", 4)}, uuid.New()))

	assert.Equal(t, []topology.Pointer{{ReleaseID: "rel-2", PromotionSeq: 4}}, topo.moved)
	assert.Equal(t, []string{"default/rel-2"}, mats.deletedFor)
	require.Len(t, reconciler.triggers, 1)
	assert.Equal(t, topology.PointerMoved, reconciler.triggers[0].Outcome)
	assert.False(t, reconciler.triggers[0].FirstPromotion)
}

// A redelivery, an older promotion and another release under the live seq are
// acknowledged and leave the pointer and the materializations alone.
func TestReleasePromotedHandler_OtherOutcomesLeaveThePointer(t *testing.T) {
	cases := []struct {
		name      string
		live      topology.Pointer
		releaseID string
		seq       int64
		want      topology.PointerOutcome
	}{
		{"a redelivery", topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 3}, "rel-1", 3, topology.PointerRedelivered},
		{"an older promotion", topology.Pointer{ReleaseID: "rel-2", PromotionSeq: 5}, "rel-1", 4, topology.PointerStale},
		{"another release under the live seq", topology.Pointer{ReleaseID: "rel-2", PromotionSeq: 5}, "rel-1", 5, topology.PointerConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topo := &fakeTopologyRepo{live: tc.live}
			mats := &fakeMaterializationRepo{}
			reconciler := &recordingSeedReconciler{}
			h := handlers.NewReleasePromotedHandler(&fakeArtifactReader{}, reconciler, testLogger())

			err := h.Handle(context.Background(), promotedUoW(topo, mats),
				handlers.ReleasePromotedInput{Promotion: promotedRelease(tc.releaseID, tc.seq)}, uuid.New())
			require.NoError(t, err)

			assert.Equal(t, []string{"lock", "save", "live"}, topo.calls)
			assert.Empty(t, topo.moved)
			assert.Empty(t, mats.deletedFor)
			require.Len(t, reconciler.triggers, 1)
			assert.Equal(t, tc.want, reconciler.triggers[0].Outcome)
			assert.False(t, reconciler.triggers[0].FirstPromotion)
		})
	}
}

func TestReleasePromotedHandler_ChecksumConflictIsPermanent(t *testing.T) {
	topo := &fakeTopologyRepo{saveErr: fmt.Errorf("%w: release rel-1", repository.ErrReleaseChecksumConflict)}
	reconciler := &recordingSeedReconciler{}
	h := handlers.NewReleasePromotedHandler(&fakeArtifactReader{}, reconciler, testLogger())

	err := h.Handle(context.Background(), promotedUoW(topo, &fakeMaterializationRepo{}),
		handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-1", 2)}, uuid.New())
	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.ErrorIs(t, err, repository.ErrReleaseChecksumConflict)
	assert.Equal(t, []string{"lock", "save"}, topo.calls)
	assert.Empty(t, reconciler.triggers)
}

func TestReleasePromotedHandler_DuplicateNodeKeyIsPermanent(t *testing.T) {
	topo := &fakeTopologyRepo{}
	h := handlers.NewReleasePromotedHandler(&fakeArtifactReader{}, nil, testLogger())
	clash := seedSpec("orders")
	clash.UniqueID, clash.SchemaName, clash.TableName = "RAW.Orders", "RAW", "Orders"

	err := h.Handle(context.Background(), promotedUoW(topo, &fakeMaterializationRepo{}),
		handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-1", 1), Nodes: []topology.NodeSpec{seedSpec("orders"), clash}},
		uuid.New())
	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.ErrorIs(t, err, topology.ErrDuplicateNodeKey)
	assert.Empty(t, topo.calls, "nothing is locked or written")
}

func TestReleasePromotedHandler_StoreFailureIsRetried(t *testing.T) {
	boom := errors.New("connection reset by peer")
	topo := &fakeTopologyRepo{saveErr: boom}
	h := handlers.NewReleasePromotedHandler(&fakeArtifactReader{}, nil, testLogger())

	err := h.Handle(context.Background(), promotedUoW(topo, &fakeMaterializationRepo{}),
		handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-1", 1)}, uuid.New())
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
}

func TestReleasePromotedHandler_RunsWithoutASeedReconciler(t *testing.T) {
	topo := &fakeTopologyRepo{loaded: true}
	h := handlers.NewReleasePromotedHandler(&fakeArtifactReader{}, nil, testLogger())

	require.NoError(t, h.Handle(context.Background(), promotedUoW(topo, &fakeMaterializationRepo{}),
		handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-1", 1)}, uuid.New()))
	assert.Len(t, topo.moved, 1)
}

func TestReleasePromotedHandler_SeedReconcilerFailureFailsTheMessage(t *testing.T) {
	boom := errors.New("plan the seeds")
	h := handlers.NewReleasePromotedHandler(&fakeArtifactReader{}, &recordingSeedReconciler{err: boom}, testLogger())

	err := h.Handle(context.Background(), promotedUoW(&fakeTopologyRepo{loaded: true}, &fakeMaterializationRepo{}),
		handlers.ReleasePromotedInput{Promotion: promotedRelease("rel-1", 1)}, uuid.New())
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
}
