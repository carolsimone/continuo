package release

import (
	"fmt"
	"time"
)

// LiveRelease is the release the install runs on now: its id, the topology
// artifact it was promoted with, and the promotion seq it was last announced
// under. The seq only moves forward. Nothing has been promoted while its
// release id is empty.
type LiveRelease struct {
	releaseID    string
	topology     TopologyRef
	promotionSeq int64
	updatedAt    time.Time
}

func NewLiveRelease() *LiveRelease { return &LiveRelease{} }

// RehydrateLiveRelease reconstructs a LiveRelease from persistence. Only
// repositories should call it.
func RehydrateLiveRelease(releaseID string, topology TopologyRef, promotionSeq int64, updatedAt time.Time) *LiveRelease {
	return &LiveRelease{releaseID: releaseID, topology: topology, promotionSeq: promotionSeq, updatedAt: updatedAt}
}

func (c *LiveRelease) ReleaseID() string     { return c.releaseID }
func (c *LiveRelease) Topology() TopologyRef { return c.topology }
func (c *LiveRelease) PromotionSeq() int64   { return c.promotionSeq }
func (c *LiveRelease) UpdatedAt() time.Time  { return c.updatedAt }

// Update makes a newly promoted release and its topology artifact the live
// release, announced under promotionSeq. Each project's manifest location is
// its production pointer, not part of the live release.
func (c *LiveRelease) Update(releaseID string, topology TopologyRef, promotionSeq int64, now time.Time) error {
	if err := c.checkSeq(promotionSeq); err != nil {
		return err
	}
	c.releaseID = releaseID
	c.topology = topology
	c.promotionSeq = promotionSeq
	c.updatedAt = now
	return nil
}

// Reannounce records a fresh promotion seq for the release that is already
// live. The release, its topology and the time the live release last moved
// stay as they are.
func (c *LiveRelease) Reannounce(promotionSeq int64) error {
	if err := c.checkSeq(promotionSeq); err != nil {
		return err
	}
	c.promotionSeq = promotionSeq
	return nil
}

func (c *LiveRelease) checkSeq(promotionSeq int64) error {
	if promotionSeq <= c.promotionSeq {
		return fmt.Errorf("promotion seq %d does not follow the live release's seq %d", promotionSeq, c.promotionSeq)
	}
	return nil
}
