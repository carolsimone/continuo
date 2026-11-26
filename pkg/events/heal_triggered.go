package events

import (
	"time"
)

// HealTriggeredSchemaVersion is the schema_version of
// heal.triggered:v1 entries.
const HealTriggeredSchemaVersion = 1

// HealTriggered is the payload of a heal.triggered:v1 entry: one
// batched heal trigger per rejected release and remediation round, carrying
// every failing node the classifier recorded. The entry's identity is the
// envelope's event_id. The same shape is what agent-remediation stores as a
// trigger's payload.
type HealTriggered struct {
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
	Nodes []HealTriggeredNode `json:"nodes"`
}

// HealTriggeredNode is one failing node of a HealTriggered.
type HealTriggeredNode struct {
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
	Project              string `json:"project,omitempty"`
	NodeType             string `json:"node_type,omitempty"`
	OtherProject         string `json:"other_project,omitempty"`
	OtherFilePath        string `json:"other_file_path,omitempty"`
	// ChangedAncestors are the node's transitive upstream ancestors whose
	// content changed in the rejected release.
	ChangedAncestors []HealTriggeredAncestor `json:"changed_ancestors,omitempty"`
	// Unfixable marks a node the classifier judged no change to the release's
	// source fixes. Absent on a node a fix can reach.
	Unfixable *Unfixable `json:"unfixable,omitempty"`
}

// HealTriggeredAncestor is one changed upstream of a failing node, with
// the file path and project the candidate topology declares for it. Depth is
// its distance from the failing node in the dependency graph.
type HealTriggeredAncestor struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path,omitempty"`
	Project  string `json:"project,omitempty"`
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

// HealTriggeredFields renders p as the Redis fields of one
// heal.triggered:v1 entry. eventID is the id of the outbox row that
// carries the entry. The payload always holds a nodes array, empty when p
// names none.
func HealTriggeredFields(eventID, tenantID, producer string, occurredAt time.Time, p HealTriggered) (map[string]any, error) {
	if p.Nodes == nil {
		p.Nodes = []HealTriggeredNode{}
	}
	env := Envelope{
		EventID:       eventID,
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: HealTriggeredSchemaVersion,
	}
	return env.Fields(p)
}
