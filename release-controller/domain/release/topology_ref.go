package release

// TopologyRef locates the topology artifact of one check: the immutable, gzipped
// JSON object topology-controller writes once per check, the SHA-256 of its
// stored bytes, and how many nodes it holds. A check stores only this reference;
// the topology is read through it when a handler needs the nodes.
type TopologyRef struct {
	URI       string
	SHA256    string
	NodeCount int
}

// IsZero reports whether the reference names no artifact: the check has not been
// parsed yet, or its parse ended before a topology was recorded.
func (r TopologyRef) IsZero() bool { return r.URI == "" }
