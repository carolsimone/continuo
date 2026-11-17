package event

// RemediationRequested mirrors the remediation.requested:v3 payload the
// remediation classifier produces: one message per rejected release and
// remediation round, carrying every failing node the classifier recorded,
// the ones no fix can reach included. Only the fields the case base records
// are mapped; the heal agent's routing fields are not this consumer's concern.
type RemediationRequested struct {
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
	// Unfixable is set when the classifier judged that no change to the
	// release's source fixes the failure. The case base keeps such a node out:
	// it holds the failures a fix proposal can learn from.
	Unfixable bool
}
