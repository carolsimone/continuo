// Package event defines the remediation.requested:v1 trigger payload and the
// deterministic identifiers used for outbox dedup.
package event

import "github.com/google/uuid"

// EventType is the outbox event_type discriminator for remediation triggers.
const EventType = "remediation_requested"

// remediationEventNamespace seeds deterministic, content-addressable event ids
// so a redelivered failure does not produce a distinct downstream trigger.
var remediationEventNamespace = uuid.MustParse("b8c4d2e1-6f3a-4b9c-8d7e-1a2b3c4d5e6f")

// RemediationEventID maps (releaseID, nodeID) to a stable UUID. The agent's
// dedup keys off this, so identical (release, node) failures collapse to one
// logical trigger.
func RemediationEventID(releaseID, nodeID string) uuid.UUID {
	return uuid.NewSHA1(remediationEventNamespace, []byte(releaseID+"|"+nodeID))
}

// AggregateIDForRelease maps a release id to a stable UUID for use as the
// outbox Entry.AggregateID, so all triggers from one release share an
// aggregate id.
func AggregateIDForRelease(releaseID string) uuid.UUID {
	return uuid.NewSHA1(remediationEventNamespace, []byte("release:"+releaseID))
}

// RemediationRequested is the trigger emitted for each healable failing node.
// It is pointer-first: the full log stays behind DBTLogURI and the failing
// code behind CodeBundleURI. The one piece of error text it carries inline is
// ErrorExcerpt — the classifier's key error line, capped at 4 KiB — kept for
// the orchestrator's failure-precedent case base; the agent still reads and
// redacts the full log before any external-LLM call.
type RemediationRequested struct {
	EventID   string `json:"event_id"`
	Source    string `json:"source"`
	ReleaseID string `json:"release_id"`
	NodeID    string `json:"node_id"`
	// RelationID is the contested physical relation for a duplicate_table
	// trigger, distinct from NodeID (the target claimant's own unique_id) —
	// the two differ whenever the target carries an alias. Empty for every
	// other source.
	RelationID     string `json:"relation_id,omitempty"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	// Reason is the matched classifier rule, e.g. "logic:missing_object".
	Reason string `json:"reason"`
	// ErrorExcerpt is the classifier's key error line (capped at 4 KiB).
	ErrorExcerpt string `json:"error_excerpt,omitempty"`
	// CodeBundleURI locates the rejected release's code-bundle document.
	// Empty when parse never completed (compile-stage failures) — no bundle
	// exists for those.
	CodeBundleURI        string `json:"code_bundle_uri,omitempty"`
	DBTLogURI            string `json:"dbt_log_uri"`
	CandidateArtifactURI string `json:"candidate_artifact_uri,omitempty"`
	FilePath             string `json:"file_path,omitempty"`
	// Service is the owning dbt service name for the failing node. Set for
	// seed_build failures from the candidate topology so the agent can locate
	// the source file without a Ancestry lookup.
	Service string `json:"service,omitempty"`
	// NodeType is the target claimant's kind (dbt-model, dbt-seed,
	// dbt-snapshot, or python-model), set on duplicate-relation failures so the
	// agent can skip a python target — whose source is not a single readable
	// file — without a topology lookup of its own.
	NodeType string `json:"node_type,omitempty"`
	// OtherService and OtherFilePath locate the competing node that also
	// produces the contested relation (RelationID). Set on duplicate-relation
	// failures so the agent can name the relation's other producer without
	// reading its source. The path is carried because both claimants can
	// belong to one service, where the service name alone identifies nothing.
	OtherService  string `json:"other_service,omitempty"`
	OtherFilePath string `json:"other_file_path,omitempty"`
	Repo          string `json:"repo"`
	CommitSHA     string `json:"commit_sha"`
	ClassifiedAt  string `json:"classified_at"`
}
