package event

// RemediationRequested mirrors the remediation.requested:v2 wire payload the
// remediation classifier produces: one message per rejected release carrying
// every healable failing node. Only the fields the case base records are
// decoded; the heal agent's routing fields are not this consumer's concern.
type RemediationRequested struct {
	EventID       string
	Source        string
	ReleaseID     string
	CodeBundleURI string
	ClassifiedAt  string // RFC3339
	Nodes         []RemediationRequestedNode
}

// RemediationRequestedNode is one classified failure inside the batch.
type RemediationRequestedNode struct {
	NodeID         string
	Category       string
	ErrorSignature string
	Reason         string
	ErrorExcerpt   string
	DBTLogURI      string
}
