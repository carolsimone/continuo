package release

// TopologyRef locates the topology artifact of one run: the immutable, gzipped
// JSON object topology-controller writes once per run, the SHA-256 of its
// stored bytes, and how many nodes it holds. A run stores only this reference;
// the topology is read through it when a handler needs the nodes.
type TopologyRef struct {
	URI       string
	SHA256    string
	NodeCount int
}

// IsZero reports whether the reference names no artifact: the run has not been
// parsed yet, or its parse ended before a topology was recorded.
func (r TopologyRef) IsZero() bool { return r.URI == "" }
