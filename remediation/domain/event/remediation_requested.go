// Package event defines the remediation.requested:v2 trigger payload and the
// deterministic identifiers used for outbox dedup.
package event

import (
	"strconv"

	"github.com/google/uuid"
)

// EventType is the outbox event_type discriminator for remediation triggers.
const EventType = "remediation_requested"

// remediationEventNamespace seeds deterministic, content-addressable event ids
// so a redelivered failure does not produce a distinct downstream trigger.
var remediationEventNamespace = uuid.MustParse("b8c4d2e1-6f3a-4b9c-8d7e-1a2b3c4d5e6f")

// RemediationEventID maps (releaseID, round) to a stable UUID. One rejected
// release yields one batched trigger per remediation round, and the agent's
// dedup keys off this id, so a redelivered rejection collapses to one logical
// trigger. For round <= 1 the name is the release id alone; round >= 2
// appends "|round|N", so a human's "try again" mints a distinct id per round.
func RemediationEventID(releaseID string, round int) uuid.UUID {
	name := releaseID
	if round > 1 {
		name += "|round|" + strconv.Itoa(round)
	}
	return uuid.NewSHA1(remediationEventNamespace, []byte(name))
}

// AggregateIDForRelease maps a release id to a stable UUID for use as the
// outbox Entry.AggregateID, so all triggers from one release share an
// aggregate id.
func AggregateIDForRelease(releaseID string) uuid.UUID {
	return uuid.NewSHA1(remediationEventNamespace, []byte("release:"+releaseID))
}

// RemediationRequested is the batched trigger emitted once per rejected
// release: the release-level facts every fix needs, and one FailingNode per
// healable failure the classifier decided to emit. It is pointer-first: logs
// stay behind each node's DBTLogURI and code behind CodeBundleURI; the only
// inline error text is each node's ErrorExcerpt (capped at 4 KiB).
type RemediationRequested struct {
	EventID   string `json:"event_id"`
	Source    string `json:"source"`
	ReleaseID string `json:"release_id"`
	// RemediationRound is the release's remediation round this trigger belongs
	// to: 1 for the rejection itself, +1 per human "try again" on the release.
	RemediationRound int    `json:"remediation_round"`
	Repo             string `json:"repo"`
	CommitSHA        string `json:"commit_sha"`
	// CodeBundleURI locates the rejected release's code-bundle document; empty
	// when parse never completed (compile-stage failures).
	CodeBundleURI string `json:"code_bundle_uri,omitempty"`
	// Shadow is true when the rejected release was itself a fix-verification
	// release. Such rejections are recorded but never emitted, so a Shadow
	// trigger is never produced; the field travels for the case base.
	Shadow       bool          `json:"shadow"`
	ClassifiedAt string        `json:"classified_at"`
	Nodes        []FailingNode `json:"nodes"`
}

// FailingNode is one classified failure inside a batched trigger.
type FailingNode struct {
	NodeID string `json:"node_id"`
	// RelationID is the contested physical relation for a duplicate_table
	// failure, distinct from NodeID; empty for every other source.
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
	// ChangedAncestorIDs are the node's transitive upstream ancestors whose
	// content changed in the rejected release, stamped by release-controller
	// from the candidate topology. They let the agent group failures that share
	// a changed ancestor and target that ancestor with one fix.
	ChangedAncestorIDs []string `json:"changed_ancestor_ids,omitempty"`
}
