package release

import (
	"fmt"
	"time"
)

// CurrentProd is the release production runs from: its id, the topology
// artifact it was promoted with, and the promotion seq it was last announced
// under. The seq only moves forward.
type CurrentProd struct {
	releaseID    string
	topology     TopologyRef
	promotionSeq int64
	updatedAt    time.Time
}

func NewCurrentProd() *CurrentProd { return &CurrentProd{} }

// RehydrateCurrentProd reconstructs a CurrentProd from persistence. Only
// repositories should call it.
func RehydrateCurrentProd(releaseID string, topology TopologyRef, promotionSeq int64, updatedAt time.Time) *CurrentProd {
	return &CurrentProd{releaseID: releaseID, topology: topology, promotionSeq: promotionSeq, updatedAt: updatedAt}
}

func (c *CurrentProd) ReleaseID() string     { return c.releaseID }
func (c *CurrentProd) Topology() TopologyRef { return c.topology }
func (c *CurrentProd) PromotionSeq() int64   { return c.promotionSeq }
func (c *CurrentProd) UpdatedAt() time.Time  { return c.updatedAt }

// Update points current_prod at a newly promoted release and its topology
// artifact, announced under promotionSeq. The per-service manifest locations
// are tracked in service_prod, not on current_prod.
func (c *CurrentProd) Update(releaseID string, topology TopologyRef, promotionSeq int64, now time.Time) error {
	if err := c.checkSeq(promotionSeq); err != nil {
		return err
	}
	c.releaseID = releaseID
	c.topology = topology
	c.promotionSeq = promotionSeq
	c.updatedAt = now
	return nil
}

// RecordArtifact attaches the topology artifact of the release current_prod
// already names: the one-time step for a release promoted before topology
// artifacts existed.
func (c *CurrentProd) RecordArtifact(topology TopologyRef) { c.topology = topology }

// Reannounce records a fresh promotion seq for the release current_prod
// already names. The release, its topology and the time current_prod last
// moved stay as they are.
func (c *CurrentProd) Reannounce(promotionSeq int64) error {
	if err := c.checkSeq(promotionSeq); err != nil {
		return err
	}
	c.promotionSeq = promotionSeq
	return nil
}

func (c *CurrentProd) checkSeq(promotionSeq int64) error {
	if promotionSeq <= c.promotionSeq {
		return fmt.Errorf("promotion seq %d does not follow current_prod's seq %d", promotionSeq, c.promotionSeq)
	}
	return nil
}
