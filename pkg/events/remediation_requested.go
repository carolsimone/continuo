package events

import (
	"time"
)

// RemediationRequestedSchemaVersion is the schema_version of
// remediation.requested:v3 entries.
const RemediationRequestedSchemaVersion = 1

// RemediationRequested is the payload of a remediation.requested:v3 entry: one
// batched heal trigger per rejected release and remediation round, carrying
// every failing node the classifier recorded. The entry's identity is the
// envelope's event_id. The same shape is what agent-remediation stores as a
// trigger's payload, and a payload stored with an event_id of its own
// (remediation.requested:v2) reads the same way.
type RemediationRequested struct {
	// Source is the pipeline stage that failed, a value of the
	// remediation_source vocabulary.
	Source string `json:"source"`
	// ReleaseID is the rejected release.
	ReleaseID string `json:"release_id"`
	// RemediationRound is the release's remediation round this trigger belongs
	// to: 1 for the rejection itself, higher for an operator's retry.
	RemediationRound int    `json:"remediation_round"`
	Repo             string `json:"repo"`
	CommitSHA        string `json:"commit_sha"`
	// CodeBundleURI locates the release's code-bundle document. Empty when the
	// release never parsed (a compile-stage or parse-stage failure).
	CodeBundleURI string `json:"code_bundle_uri,omitempty"`
	ClassifiedAt  string `json:"classified_at"`
	// Nodes are the failing nodes, the unfixable ones included.
	Nodes []RemediationRequestedNode `json:"nodes"`
}

// RemediationRequestedNode is one failing node of a RemediationRequested.
type RemediationRequestedNode struct {
	NodeID string `json:"node_id"`
	// RelationID is the contested physical relation of a duplicate_table
	// failure, distinct from NodeID. Empty for every other source.
	RelationID           string `json:"relation_id,omitempty"`
	Category             string `json:"category"`
	ErrorSignature       string `json:"error_signature"`
	Reason               string `json:"reason"`
	ErrorExcerpt         string `json:"error_excerpt,omitempty"`
	DBTLogURI            string `json:"dbt_log_uri"`
	CandidateArtifactURI string `json:"candidate_artifact_uri,omitempty"`
	FilePath             string `json:"file_path,omitempty"`
	Service              string `json:"service,omitempty"`
	NodeType             string `json:"node_type,omitempty"`
	OtherService         string `json:"other_service,omitempty"`
	OtherFilePath        string `json:"other_file_path,omitempty"`
	// ChangedAncestors are the node's transitive upstream ancestors whose
	// content changed in the rejected release.
	ChangedAncestors []RemediationRequestedAncestor `json:"changed_ancestors,omitempty"`
	// Unfixable marks a node the classifier judged no change to the release's
	// source fixes. Absent on a node a fix can reach.
	Unfixable *Unfixable `json:"unfixable,omitempty"`
}

// RemediationRequestedAncestor is one changed upstream of a failing node, with
// the file path and service the candidate topology declares for it. Depth is
// its distance from the failing node in the dependency graph.
type RemediationRequestedAncestor struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path,omitempty"`
	Service  string `json:"service,omitempty"`
	Depth    int    `json:"depth"`
}

// Unfixable is the classifier's verdict that no fix proposal can help a node.
// Reason is model.HandoverReasonClassifierInfra or
// model.HandoverReasonClassifierUnhealableParse; Detail is what actually
// failed: the failure's key error line, cleaned of credentials and email
// addresses, or the matched rule when no line exists.
type Unfixable struct {
	Reason string `json:"reason"`
	Detail string `json:"detail"`
}

// RemediationRequestedFields renders p as the Redis fields of one
// remediation.requested:v3 entry. eventID is the id of the outbox row that
// carries the entry. The payload always holds a nodes array, empty when p
// names none.
func RemediationRequestedFields(eventID, tenantID, producer string, occurredAt time.Time, p RemediationRequested) (map[string]any, error) {
	if p.Nodes == nil {
		p.Nodes = []RemediationRequestedNode{}
	}
	env := Envelope{
		EventID:       eventID,
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: RemediationRequestedSchemaVersion,
	}
	return env.Fields(p)
}
