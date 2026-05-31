package release

import "time"

type CurrentProd struct {
	releaseID        string
	manifestsURI     string
	topologySnapshot Topology
	updatedAt        time.Time
}

func NewCurrentProd() *CurrentProd { return &CurrentProd{} }

func RehydrateCurrentProd(releaseID, manifestsURI string, topology Topology, updatedAt time.Time) *CurrentProd {
	return &CurrentProd{releaseID: releaseID, manifestsURI: manifestsURI, topologySnapshot: topology, updatedAt: updatedAt}
}

func (c *CurrentProd) ReleaseID() string          { return c.releaseID }
func (c *CurrentProd) ManifestsURI() string       { return c.manifestsURI }
func (c *CurrentProd) TopologySnapshot() Topology { return c.topologySnapshot }
func (c *CurrentProd) UpdatedAt() time.Time       { return c.updatedAt }

// Update points current_prod at a newly promoted release. manifestsURI is the
// S3 location that release's manifests were uploaded to; it becomes the
// defer-state base for the next release's validation, so it must be the exact
// URI submitted with the release rather than one reconstructed from a bucket
// guess.
func (c *CurrentProd) Update(releaseID, manifestsURI string, topology Topology, now time.Time) {
	c.releaseID = releaseID
	c.manifestsURI = manifestsURI
	c.topologySnapshot = topology
	c.updatedAt = now
}
