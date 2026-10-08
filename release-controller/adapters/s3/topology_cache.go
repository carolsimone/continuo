package s3

import (
	"container/list"
	"slices"
	"sync"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// topologyCache is a bounded, least-recently-used cache of decoded topologies
// keyed by artifact URI. An entry answers only for the checksum it was stored
// with, so a reference carrying another checksum is a miss and is read (and
// verified) again. Artifacts are immutable, so an entry never goes stale.
type topologyCache struct {
	mu    sync.Mutex
	size  int
	order *list.List // front = most recently used; values are *cachedTopology
	byURI map[string]*list.Element
}

type cachedTopology struct {
	uri    string
	sha256 string
	topo   release.Topology
}

// newTopologyCache returns a cache holding at most size topologies; size <= 0
// disables caching.
func newTopologyCache(size int) *topologyCache {
	return &topologyCache{size: size, order: list.New(), byURI: map[string]*list.Element{}}
}

// cloneTopology returns a deep copy of topo: the node slice and every node's
// slice field are copied, so the result shares no backing array with topo. A
// topology handed to a caller, and one held by the cache, must never alias.
// release.Node's reference-typed fields are enumerated by
// TestCloneTopology_CoversEveryReferenceField.
func cloneTopology(topo release.Topology) release.Topology {
	if topo == nil {
		return nil
	}
	out := make(release.Topology, len(topo))
	for i, n := range topo {
		n.UpstreamUniqueIDs = slices.Clone(n.UpstreamUniqueIDs)
		out[i] = n
	}
	return out
}

// get returns a deep copy of the cached topology for ref, if ref's checksum matches.
func (c *topologyCache) get(ref release.TopologyRef) (release.Topology, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byURI[ref.URI]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*cachedTopology)
	if entry.sha256 != ref.SHA256 {
		return nil, false
	}
	c.order.MoveToFront(el)
	return cloneTopology(entry.topo), true
}

// put stores a deep copy of topo under ref, evicting the least recently used entry
// when the cache is full.
func (c *topologyCache) put(ref release.TopologyRef, topo release.Topology) {
	if c.size <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := &cachedTopology{uri: ref.URI, sha256: ref.SHA256, topo: cloneTopology(topo)}
	if el, ok := c.byURI[ref.URI]; ok {
		el.Value = entry
		c.order.MoveToFront(el)
		return
	}
	c.byURI[ref.URI] = c.order.PushFront(entry)
	for c.order.Len() > c.size {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.byURI, oldest.Value.(*cachedTopology).uri)
	}
}
