package release

// ContractBundleRef locates the contract bundle of one run: the immutable,
// gzipped JSON object topology-controller writes once per release that parses
// ok, and the SHA-256 of its stored bytes. A run stores only this reference;
// release.promoted:v1 carries it so a consumer reads the bundle through it. The
// zero value names no bundle: the run has not been parsed yet, its parse
// failed, or its producer wrote none.
type ContractBundleRef struct {
	URI    string
	SHA256 string
}
