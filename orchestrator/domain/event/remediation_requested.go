package event

// RemediationRequested mirrors the remediation.requested:v1 wire payload the
// remediation classifier produces. Only the fields the case base records are
// decoded; the heal agent's routing fields (file paths, services) are not
// this consumer's concern.
type RemediationRequested struct {
	EventID        string `json:"event_id"`
	Source         string `json:"source"`
	ReleaseID      string `json:"release_id"`
	NodeID         string `json:"node_id"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	Reason         string `json:"reason"`
	ErrorExcerpt   string `json:"error_excerpt"`
	DBTLogURI      string `json:"dbt_log_uri"`
	CodeBundleURI  string `json:"code_bundle_uri"`
	ClassifiedAt   string `json:"classified_at"` // RFC3339
}
