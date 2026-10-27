package topologyretention_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/topologyretention"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var pruneNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

type pruneClock struct{}

func (pruneClock) Now() time.Time { return pruneNow }

// pruneTopology records the lock and the prune cutoff, in call order.
type pruneTopology struct {
	calls   []string
	cutoff  time.Time
	pruned  int64
	pruneEr error
}

var _ repository.TopologyRepository = (*pruneTopology)(nil)

func (f *pruneTopology) LockTopology(context.Context) error {
	f.calls = append(f.calls, "lock")
	return nil
}

func (f *pruneTopology) PruneReleases(_ context.Context, loadedBefore time.Time) (int64, error) {
	f.calls = append(f.calls, "prune")
	f.cutoff = loadedBefore
	return f.pruned, f.pruneEr
}

func (f *pruneTopology) SaveRelease(context.Context, topology.Release) (bool, error) {
	return false, errors.New("SaveRelease is not used by the pruner")
}

func (f *pruneTopology) LivePointer(context.Context, string) (topology.Pointer, error) {
	return topology.Pointer{}, errors.New("LivePointer is not used by the pruner")
}

func (f *pruneTopology) LivePointers(context.Context) ([]topology.Pointer, error) {
	return nil, errors.New("LivePointers is not used by the pruner")
}

func (f *pruneTopology) SetLivePointer(context.Context, string, topology.Pointer, time.Time) error {
	return errors.New("SetLivePointer is not used by the pruner")
}

func (f *pruneTopology) ReleaseNodes(context.Context, string, string, string) ([]topology.Node, error) {
	return nil, errors.New("ReleaseNodes is not used by the pruner")
}

func (f *pruneTopology) ScheduleGraph(context.Context, string, string, string) (topology.Graph, error) {
	return topology.Graph{}, errors.New("ScheduleGraph is not used by the pruner")
}
func (f *pruneTopology) Graph(context.Context, string, string, []topology.NodeKey) (topology.Graph, error) {
	return topology.Graph{}, errors.New("Graph is not used by the pruner")
}

func (f *pruneTopology) NodeContentHash(context.Context, string, string, topology.NodeKey) (string, bool, error) {
	return "", false, errors.New("NodeContentHash is not used by the pruner")
}

func newPruner(topo *pruneTopology) (*topologyretention.Pruner, *uow.FakeUnitOfWork) {
	u := &uow.FakeUnitOfWork{TopologyRepo: topo}
	u.SetClock(pruneClock{})
	return topologyretention.NewPruner(func() uow.UnitOfWork { return u }, slog.New(slog.NewTextHandler(io.Discard, nil))), u
}

// Prune takes the topology lock before it deletes anything, deletes the
// releases loaded more than retention ago, and commits.
func TestPruner_LocksThenPrunesBeforeTheCutoff(t *testing.T) {
	topo := &pruneTopology{pruned: 3}
	p, u := newPruner(topo)

	n, err := p.Prune(context.Background(), 7*24*time.Hour, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, []string{"lock", "prune"}, topo.calls)
	assert.True(t, topo.cutoff.Equal(pruneNow.Add(-7*24*time.Hour)))
	assert.Equal(t, 1, u.CommitCalled)
	assert.Zero(t, u.RollbackCalled)
}

// A failed prune rolls back and reports the error to the sweeper.
func TestPruner_RollsBackAFailedPrune(t *testing.T) {
	boom := errors.New("connection reset by peer")
	p, u := newPruner(&pruneTopology{pruneEr: boom})

	_, err := p.Prune(context.Background(), time.Hour, 1000)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.Zero(t, u.CommitCalled)
	assert.Equal(t, 1, u.RollbackCalled)
}
