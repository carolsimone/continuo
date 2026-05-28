package release

import "time"

type CurrentProd struct {
	releaseID        string
	topologySnapshot Topology
	updatedAt        time.Time
}

func NewCurrentProd() *CurrentProd { return &CurrentProd{} }

func RehydrateCurrentProd(releaseID string, topology Topology, updatedAt time.Time) *CurrentProd {
	return &CurrentProd{releaseID: releaseID, topologySnapshot: topology, updatedAt: updatedAt}
}

func (c *CurrentProd) ReleaseID() string          { return c.releaseID }
func (c *CurrentProd) TopologySnapshot() Topology { return c.topologySnapshot }
func (c *CurrentProd) UpdatedAt() time.Time       { return c.updatedAt }

func (c *CurrentProd) Update(releaseID string, topology Topology, now time.Time) {
	c.releaseID = releaseID
	c.topologySnapshot = topology
	c.updatedAt = now
}
