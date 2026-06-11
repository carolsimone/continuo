package neo4jinfra

import (
	"container/list"
	"context"
	"log/slog"
	"sync"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

// scheduleGraphCacheSize bounds the number of distinct
// (schedule_name, topology_generation) entries kept in memory. The shape of a
// schedule's topology is small (nodes + edges), so a handful of generations per
// schedule fits comfortably; the LRU evicts the least-recently-used entry past
// this bound.
const scheduleGraphCacheSize = 32

// ScheduleGraphProvider is the full read surface the cache decorates. It caches
// only GetScheduleGraph (the expensive, immutable topology-shape query) and
// passes ListRuns / ListScheduleTopologies straight through so the decorator
// can stand in for the repository wherever the schedule-and-run reader is
// required. Satisfied by *OrchestratorQueryRepository.
type ScheduleGraphProvider interface {
	GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error)
	ListRuns(ctx context.Context, scheduleName string, limit, offset int) ([]*domain.RunSummary, int, error)
	ListScheduleTopologies(ctx context.Context) ([]*domain.ScheduleTopologySummary, error)
}

// GenerationProvider returns the orchestrator's current topology_generation.
// The cache uses it to form the immutable cache key: a topology shape never
// changes for a fixed generation, and a generation bump (a new manifest load)
// produces a new key, naturally invalidating stale shapes without explicit
// eviction. Satisfied by adapters/postgres topologyStateRepository.GetGeneration.
type GenerationProvider interface {
	GetGeneration(ctx context.Context) (int64, error)
}

// CachingScheduleGraphReader decorates a ScheduleGraphProvider with a bounded
// LRU cache keyed by (schedule_name, topology_generation).
//
// CACHE BOUNDARY: only the IMMUTABLE topology SHAPE is cached. GetScheduleGraph
// returns nodes/edges that depend solely on the manifest topology at a given
// generation — it carries NO per-run status overlay. Run-graph reads
// (GetRunGraph), which DO overlay live :EXECUTES.status, are intentionally not
// routed through this cache and stay uncached so status changes are always
// fresh.
//
// On each call the decorator probes the current generation (a cheap Postgres
// single-row read) to build the key. A hit returns the cached shape without
// touching Neo4j; a miss runs the underlying shape query once and stores the
// result. Because the key embeds the generation, a bumped generation can never
// serve a stale shape.
type CachingScheduleGraphReader struct {
	inner      ScheduleGraphProvider
	generation GenerationProvider
	logger     *slog.Logger

	mu      sync.Mutex
	entries map[scheduleGraphKey]*list.Element
	lru     *list.List
	maxSize int
}

type scheduleGraphKey struct {
	scheduleName string
	generation   int64
}

type scheduleGraphEntry struct {
	key   scheduleGraphKey
	graph *domain.ScheduleGraph
}

// NewCachingScheduleGraphReader wraps inner with an LRU cache of bounded size.
func NewCachingScheduleGraphReader(inner ScheduleGraphProvider, generation GenerationProvider, logger *slog.Logger) *CachingScheduleGraphReader {
	return &CachingScheduleGraphReader{
		inner:      inner,
		generation: generation,
		logger:     logger,
		entries:    make(map[scheduleGraphKey]*list.Element),
		lru:        list.New(),
		maxSize:    scheduleGraphCacheSize,
	}
}

// GetScheduleGraph returns the schedule's topology shape, served from the cache
// when the current generation matches a cached entry. If the generation probe
// fails, the call falls back to a direct (uncached) read so a transient
// Postgres error degrades performance, not correctness.
func (c *CachingScheduleGraphReader) GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error) {
	gen, err := c.generation.GetGeneration(ctx)
	if err != nil {
		c.logger.Warn("schedule-graph cache: generation probe failed, bypassing cache",
			"schedule", scheduleName, "error", err)
		return c.inner.GetScheduleGraph(ctx, scheduleName)
	}

	key := scheduleGraphKey{scheduleName: scheduleName, generation: gen}
	if graph, ok := c.get(key); ok {
		return graph, nil
	}

	graph, err := c.inner.GetScheduleGraph(ctx, scheduleName)
	if err != nil {
		return nil, err
	}
	c.put(key, graph)
	return graph, nil
}

// ListRuns passes through to the underlying reader; run history is not cached.
func (c *CachingScheduleGraphReader) ListRuns(ctx context.Context, scheduleName string, limit, offset int) ([]*domain.RunSummary, int, error) {
	return c.inner.ListRuns(ctx, scheduleName, limit, offset)
}

// ListScheduleTopologies passes through to the underlying reader.
func (c *CachingScheduleGraphReader) ListScheduleTopologies(ctx context.Context) ([]*domain.ScheduleTopologySummary, error) {
	return c.inner.ListScheduleTopologies(ctx)
}

func (c *CachingScheduleGraphReader) get(key scheduleGraphKey) (*domain.ScheduleGraph, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(elem)
	return elem.Value.(*scheduleGraphEntry).graph, true
}

func (c *CachingScheduleGraphReader) put(key scheduleGraphKey, graph *domain.ScheduleGraph) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		c.lru.MoveToFront(elem)
		elem.Value.(*scheduleGraphEntry).graph = graph
		return
	}
	elem := c.lru.PushFront(&scheduleGraphEntry{key: key, graph: graph})
	c.entries[key] = elem
	for c.lru.Len() > c.maxSize {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.lru.Remove(oldest)
		delete(c.entries, oldest.Value.(*scheduleGraphEntry).key)
	}
}
