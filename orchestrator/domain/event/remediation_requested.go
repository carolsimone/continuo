package event

// RemediationRequested mirrors the remediation.requested:v2 wire payload the
// remediation classifier produces: one message per rejected release carrying
// every healable failing node. Only the fields the case base records are
// decoded; the heal agent's routing fields are not this consumer's concern.
type RemediationRequested struct {
	EventID       string                     `json:"event_id"`
	Source        string                     `json:"source"`
	ReleaseID     string                     `json:"release_id"`
	CodeBundleURI string                     `json:"code_bundle_uri"`
	ClassifiedAt  string                     `json:"classified_at"` // RFC3339
	Nodes         []RemediationRequestedNode `json:"nodes"`
}

// RemediationRequestedNode is one classified failure inside the batch.
type RemediationRequestedNode struct {
	NodeID         string `json:"node_id"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	Reason         string `json:"reason"`
	ErrorExcerpt   string `json:"error_excerpt"`
	DBTLogURI      string `json:"dbt_log_uri"`
}
