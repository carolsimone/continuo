package neo4jinfra

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spyScheduleGraphProvider counts how many times the expensive shape query
// (GetScheduleGraph) is actually executed, so the cache can be proven to fetch
// the topology shape once per generation.
type spyScheduleGraphProvider struct {
	mu              sync.Mutex
	graphCalls      int
	graphBySchedule map[string]*domain.ScheduleGraph
}

func (s *spyScheduleGraphProvider) GetScheduleGraph(_ context.Context, scheduleName string) (*domain.ScheduleGraph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.graphCalls++
	if g, ok := s.graphBySchedule[scheduleName]; ok {
		return g, nil
	}
	return &domain.ScheduleGraph{}, nil
}

func (s *spyScheduleGraphProvider) ListRuns(_ context.Context, _ string, _, _ int) ([]*domain.RunSummary, int, error) {
	return nil, 0, nil
}

func (s *spyScheduleGraphProvider) ListScheduleTopologies(_ context.Context) ([]*domain.ScheduleTopologySummary, error) {
	return nil, nil
}
func (s *spyScheduleGraphProvider) GetNodeAncestry(_ context.Context, _ string, _ int) ([]*domain.NodeAncestor, error) {
	return nil, nil
}
func (s *spyScheduleGraphProvider) GetNode(_ context.Context, _, _, _ string) (*domain.NodeMeta, error) {
	return nil, nil
}
func (s *spyScheduleGraphProvider) GetNodeLocation(_ context.Context, _ string) (*domain.NodeLocation, error) {
	return nil, nil
}

// stubGeneration returns a generation the test controls between calls.
type stubGeneration struct {
	mu  sync.Mutex
	gen int64
	err error
}

func (s *stubGeneration) GetGeneration(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen, s.err
}

func (s *stubGeneration) set(gen int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen = gen
}

func newTestCache(spy *spyScheduleGraphProvider, gen *stubGeneration) *CachingScheduleGraphReader {
	return NewCachingScheduleGraphReader(spy, gen, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func TestScheduleGraphCache_SameGeneration_FetchesOnce(t *testing.T) {
	spy := &spyScheduleGraphProvider{graphBySchedule: map[string]*domain.ScheduleGraph{
		"sched-a": {Nodes: []*domain.TableNode{{TableName: "t1"}}, TopologyGeneration: 7},
	}}
	gen := &stubGeneration{gen: 7}
	cache := newTestCache(spy, gen)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g, err := cache.GetScheduleGraph(ctx, "sched-a")
		require.NoError(t, err)
		require.Len(t, g.Nodes, 1)
	}
	assert.Equal(t, 1, spy.graphCalls, "topology shape must be fetched exactly once per generation")
}

func TestScheduleGraphCache_BumpedGeneration_Refetches(t *testing.T) {
	spy := &spyScheduleGraphProvider{graphBySchedule: map[string]*domain.ScheduleGraph{
		"sched-a": {Nodes: []*domain.TableNode{{TableName: "t1"}}},
	}}
	gen := &stubGeneration{gen: 1}
	cache := newTestCache(spy, gen)
	ctx := context.Background()

	_, err := cache.GetScheduleGraph(ctx, "sched-a")
	require.NoError(t, err)
	_, err = cache.GetScheduleGraph(ctx, "sched-a")
	require.NoError(t, err)
	assert.Equal(t, 1, spy.graphCalls, "same generation served from cache")

	// A manifest load bumps the generation; the next read must refetch.
	gen.set(2)
	_, err = cache.GetScheduleGraph(ctx, "sched-a")
	require.NoError(t, err)
	assert.Equal(t, 2, spy.graphCalls, "bumped generation refetches the shape")

	// The new generation is then itself cached.
	_, err = cache.GetScheduleGraph(ctx, "sched-a")
	require.NoError(t, err)
	assert.Equal(t, 2, spy.graphCalls, "new generation cached after refetch")
}

func TestScheduleGraphCache_DistinctSchedulesCachedSeparately(t *testing.T) {
	spy := &spyScheduleGraphProvider{}
	gen := &stubGeneration{gen: 1}
	cache := newTestCache(spy, gen)
	ctx := context.Background()

	_, _ = cache.GetScheduleGraph(ctx, "sched-a")
	_, _ = cache.GetScheduleGraph(ctx, "sched-b")
	_, _ = cache.GetScheduleGraph(ctx, "sched-a")
	_, _ = cache.GetScheduleGraph(ctx, "sched-b")
	assert.Equal(t, 2, spy.graphCalls, "each schedule fetched once and then cached")
}

func TestScheduleGraphCache_GenerationProbeError_BypassesCache(t *testing.T) {
	spy := &spyScheduleGraphProvider{}
	gen := &stubGeneration{gen: 1, err: errors.New("postgres down")}
	cache := newTestCache(spy, gen)
	ctx := context.Background()

	// When the generation probe fails, each call falls back to a direct read.
	_, err := cache.GetScheduleGraph(ctx, "sched-a")
	require.NoError(t, err)
	_, err = cache.GetScheduleGraph(ctx, "sched-a")
	require.NoError(t, err)
	assert.Equal(t, 2, spy.graphCalls, "probe failure degrades to uncached reads, never stale")
}

func TestScheduleGraphCache_EvictsBeyondMaxSize(t *testing.T) {
	spy := &spyScheduleGraphProvider{}
	gen := &stubGeneration{gen: 1}
	cache := newTestCache(spy, gen)
	cache.maxSize = 2
	ctx := context.Background()

	// Fill past capacity: a, b, c -> a is evicted.
	_, _ = cache.GetScheduleGraph(ctx, "a")
	_, _ = cache.GetScheduleGraph(ctx, "b")
	_, _ = cache.GetScheduleGraph(ctx, "c")
	assert.Equal(t, 3, spy.graphCalls)

	// b and c are still cached.
	_, _ = cache.GetScheduleGraph(ctx, "b")
	_, _ = cache.GetScheduleGraph(ctx, "c")
	assert.Equal(t, 3, spy.graphCalls, "b and c served from cache")

	// a was evicted, so it refetches.
	_, _ = cache.GetScheduleGraph(ctx, "a")
	assert.Equal(t, 4, spy.graphCalls, "evicted entry refetches")
}
