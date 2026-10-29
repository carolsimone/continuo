// Package topology models a promoted release's topology as run-controller stores it:
// the release's nodes keyed by schema and table, the edges between them, and
// the pointer to the release that is live for a tenant.
package topology

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// NodeKey identifies a node within a release: "<schema>.<table>", lowercased.
type NodeKey string

// KeyOf returns the key of the node that builds schemaName.tableName.
func KeyOf(schemaName, tableName string) NodeKey {
	return NodeKey(strings.ToLower(schemaName + "." + tableName))
}

// NodeSpec is one node as a release's topology artifact describes it. The
// upstream nodes it reads are named by unique id.
type NodeSpec struct {
	UniqueID          string
	ServiceName       string
	SchemaName        string
	TableName         string
	NodeType          string
	TestCount         *int
	ContentHash       string
	ImageTag          string
	SecretRef         string
	ScheduleName      string
	TimeoutSeconds    *int
	UpstreamUniqueIDs []string
}

// Node is one node as stored for a release. TestCount is nil when the number
// of data tests on the node is unknown; TimeoutSeconds is nil when the node
// declares no task timeout.
type Node struct {
	Key            NodeKey
	UniqueID       string
	ServiceName    string
	SchemaName     string
	TableName      string
	NodeType       string
	TestCount      *int
	ContentHash    string
	ImageTag       string
	SecretRef      string
	ScheduleName   string
	TimeoutSeconds *int
}

// Edge records that Downstream reads Upstream. Both nodes belong to the same
// release, and neither is a dbt test.
type Edge struct {
	Upstream   NodeKey
	Downstream NodeKey
}

// Release is one promoted release's topology: its identity, the promotion seq
// it was announced under, the artifact it was read from, its nodes in key
// order and its edges in (upstream, downstream) order.
type Release struct {
	TenantID       string
	ReleaseID      string
	PromotionSeq   int64
	TopologyURI    string
	TopologySHA256 string
	Nodes          []Node
	Edges          []Edge
}

// ErrDuplicateNodeKey reports a release in which two nodes build the same
// relation. Such a release cannot be stored, however often it is delivered.
var ErrDuplicateNodeKey = errors.New("two nodes of one release share a node key")

// NewRelease builds the stored form of a release from its artifact's nodes.
// dbt tests are dropped: they are checked during validation and never run in
// production. An edge is kept when an upstream names another non-test node of
// the release; an upstream that names a test, or a node the release does not
// hold, is dropped, and a repeated upstream yields one edge. The checksum is
// lowercased, the form every reader compares.
func NewRelease(tenantID, releaseID string, seq int64, uri, sha256 string, specs []NodeSpec) (Release, error) {
	nodes := make([]Node, 0, len(specs))
	builtBy := make(map[NodeKey]string, len(specs))
	keyByUniqueID := make(map[string]NodeKey, len(specs))
	for _, s := range specs {
		if isTest(s) {
			continue
		}
		key := KeyOf(s.SchemaName, s.TableName)
		if other, taken := builtBy[key]; taken {
			return Release{}, fmt.Errorf("%w: %q is built by %q and %q", ErrDuplicateNodeKey, key, other, s.UniqueID)
		}
		builtBy[key] = s.UniqueID
		if _, seen := keyByUniqueID[s.UniqueID]; !seen {
			keyByUniqueID[s.UniqueID] = key
		}
		nodes = append(nodes, Node{
			Key:            key,
			UniqueID:       s.UniqueID,
			ServiceName:    s.ServiceName,
			SchemaName:     s.SchemaName,
			TableName:      s.TableName,
			NodeType:       s.NodeType,
			TestCount:      cloneCount(s.TestCount),
			ContentHash:    s.ContentHash,
			ImageTag:       s.ImageTag,
			SecretRef:      s.SecretRef,
			ScheduleName:   s.ScheduleName,
			TimeoutSeconds: cloneCount(s.TimeoutSeconds),
		})
	}

	edges := make([]Edge, 0)
	seen := make(map[Edge]bool)
	for _, s := range specs {
		if isTest(s) {
			continue
		}
		downstream := KeyOf(s.SchemaName, s.TableName)
		for _, id := range s.UpstreamUniqueIDs {
			upstream, ok := keyByUniqueID[id]
			if !ok {
				continue
			}
			e := Edge{Upstream: upstream, Downstream: downstream}
			if seen[e] {
				continue
			}
			seen[e] = true
			edges = append(edges, e)
		}
	}

	slices.SortFunc(nodes, func(a, b Node) int { return cmp.Compare(a.Key, b.Key) })
	slices.SortFunc(edges, func(a, b Edge) int {
		if c := cmp.Compare(a.Upstream, b.Upstream); c != 0 {
			return c
		}
		return cmp.Compare(a.Downstream, b.Downstream)
	})
	return Release{
		TenantID:       tenantID,
		ReleaseID:      releaseID,
		PromotionSeq:   seq,
		TopologyURI:    uri,
		TopologySHA256: strings.ToLower(sha256),
		Nodes:          nodes,
		Edges:          edges,
	}, nil
}

func isTest(s NodeSpec) bool { return s.NodeType == string(model.NodeTypeDbtTest) }

func cloneCount(n *int) *int {
	if n == nil {
		return nil
	}
	v := *n
	return &v
}

// Pointer names the release that is live for a tenant and the promotion seq it
// was applied under; TenantID names the tenant when the pointer was read from
// the store. The zero Pointer means no release is live yet.
type Pointer struct {
	TenantID     string
	ReleaseID    string
	PromotionSeq int64
}

// IsZero reports whether p names no release.
func (p Pointer) IsZero() bool { return p.ReleaseID == "" && p.PromotionSeq == 0 }

// PointerOutcome is what one promotion does to the live-release pointer.
type PointerOutcome int

const (
	// PointerMoved: the promotion is newer than the live one (or nothing is
	// live), so its release becomes the live release.
	PointerMoved PointerOutcome = iota + 1
	// PointerRedelivered: the live promotion arrived again; nothing changes.
	PointerRedelivered
	// PointerStale: the promotion is older than the live one; its release is
	// kept as history and the pointer stays.
	PointerStale
	// PointerConflict: another release arrived under the live promotion seq;
	// the pointer stays.
	PointerConflict
)

// String names the outcome for logs.
func (o PointerOutcome) String() string {
	switch o {
	case PointerMoved:
		return "moved"
	case PointerRedelivered:
		return "redelivered"
	case PointerStale:
		return "stale"
	case PointerConflict:
		return "conflict"
	default:
		return "unknown"
	}
}

// DecidePointer compares a promotion (releaseID, seq) with the live pointer.
// Delivery order is not promotion order, so only a higher seq moves the
// pointer.
func DecidePointer(live Pointer, releaseID string, seq int64) PointerOutcome {
	switch {
	case live.IsZero(), seq > live.PromotionSeq:
		return PointerMoved
	case seq < live.PromotionSeq:
		return PointerStale
	case releaseID == live.ReleaseID:
		return PointerRedelivered
	default:
		return PointerConflict
	}
}
