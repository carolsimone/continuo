package queries

import (
	"context"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
)

// ContractReader is the read surface over a tenant's contract history. Every
// method reads one tenant's graph only. A node's history is its promotion
// edges, one per change of its contract, each numbered by the node's counter.
// A retired node, and a node the newest promotion the live folder recorded did
// not carry (its service no longer releases ODCS contracts), keeps its history
// and its pointer, but none of its versions is current and it has no current
// contract.
type ContractReader interface {
	// Current returns the version the live node's pointer names, with its
	// stored document. A node with no current contract returns
	// contractversion.ErrNotFound.
	Current(ctx context.Context, tenantID, uniqueID string) (contractversion.VersionView, error)
	// History lists the node's versions, newest first, up to limit (every
	// version when limit is 0 or below), marking the current one. Its rows
	// carry no document. A node with no contract history has an empty list.
	History(ctx context.Context, tenantID, uniqueID string, limit int) ([]contractversion.VersionView, error)
	// ByVersion returns the node's version numbered version, with its stored
	// document. A version the node has no promotion edge for returns
	// contractversion.ErrNotFound.
	ByVersion(ctx context.Context, tenantID, uniqueID string, version int64) (contractversion.VersionView, error)
	// Live returns the current contract of every node whose contract
	// watermark is promotionSeq, sorted by unique id: the nodes that
	// promotion's bundle named and no newer promotion reached, whatever their
	// active flag. Every bundle names every python-odcs node of the release's
	// topology, so a node the promotion added is in the set, and a node it
	// dropped, whose watermark lags, is not.
	Live(ctx context.Context, tenantID string, promotionSeq int64) ([]contractversion.LiveContract, error)
	// LatestPromotionSeq returns the largest contract watermark of the
	// tenant's nodes: the newest promotion whose contracts are recorded. It
	// is 0 when no node has one.
	LatestPromotionSeq(ctx context.Context, tenantID string) (int64, error)
}
