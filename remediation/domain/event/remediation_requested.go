// Package event defines the remediation trigger the classifier emits for a
// rejected release and the deterministic identifiers used for outbox dedup.
package event

import (
	"strconv"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// EventType is the outbox event_type discriminator for remediation triggers.
const EventType = "remediation_requested"

// remediationEventNamespace seeds deterministic, content-addressable event ids
// so a redelivered failure does not produce a distinct downstream trigger.
var remediationEventNamespace = uuid.MustParse("b8c4d2e1-6f3a-4b9c-8d7e-1a2b3c4d5e6f")

// RemediationEventID maps (releaseID, round) to a stable UUID. One rejected
// release yields one batched trigger per remediation round, and the trigger's
// outbox row id derives from this id, so a redelivered rejection collapses to
// one logical trigger. For round <= 1 the name is the release id alone;
// round >= 2 appends "|round|N", so a human's "try again" mints a distinct id
// per round.
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
// release and remediation round: the release-level facts every fix needs, and
// one FailingNode per failure the classifier recorded in this batch, the
// unfixable ones included. It is pointer-first: logs stay behind each node's
// DBTLogURI and code behind CodeBundleURI; the only inline error text is each
// node's ErrorExcerpt (capped at 4 KiB). Its identity is RemediationEventID of
// its release and round.
type RemediationRequested struct {
	Source    model.RemediationSource
	ReleaseID string
	// RemediationRound is the release's remediation round this trigger belongs
	// to: 1 for the rejection itself, +1 per human "try again" on the release.
	RemediationRound int
	Repo             string
	CommitSHA        string
	// CodeBundleURI locates the rejected release's code-bundle document; empty
	// when parse never completed (compile-stage failures).
	CodeBundleURI string
	ClassifiedAt  string
	Nodes         []FailingNode
}

// FailingNode is one classified failure inside a batched trigger.
type FailingNode struct {
	NodeID string
	// RelationID is the contested physical relation for a duplicate_table
	// failure, distinct from NodeID; empty for every other source.
	RelationID           string
	Category             string
	ErrorSignature       string
	Reason               string
	ErrorExcerpt         string
	DBTLogURI            string
	CandidateArtifactURI string
	FilePath             string
	Service              string
	NodeType             string
	OtherService         string
	OtherFilePath        string
	// ChangedAncestors are the node's transitive upstream ancestors whose
	// content changed in the rejected release, stamped by release-controller
	// from the candidate topology. They let the agent group failures that share
	// a changed ancestor and target that ancestor with one fix, and each carries
	// the file path and service the candidate declares for it so the fix edits
	// the ancestor where THIS release holds it.
	ChangedAncestors []ChangedAncestor
	// Unfixable is set when the classifier judged that no change to the
	// release's source fixes the failure; nil for a failure a fix can reach.
	Unfixable *Unfixable
}

// Unfixable is the classifier's verdict on a failing node no fix proposal can
// help: Reason is model.HandoverReasonClassifierInfra or
// model.HandoverReasonClassifierUnhealableParse, and Detail is what actually
// failed: the failure's key error line, or the matched rule when the
// classification holds no line.
type Unfixable struct {
	Reason model.HandoverReason
	Detail string
}

// ChangedAncestor is one changed upstream of a failing node, with the location
// the rejected release's candidate topology declares for it and its minimum
// upstream hop distance from the failing node.
type ChangedAncestor struct {
	NodeID   string
	FilePath string
	Service  string
	Depth    int
}
