package handlers

import (
	"sort"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

// Trigger is the decoded remediation.requested payload that drives one
// ProposeFix invocation: the release-level facts every fix needs, and one
// TriggerNode per failing node a fix can reach. One rejected
// release yields one trigger per remediation round, so a trigger names a whole
// failing set rather than a single node.
type Trigger struct {
	Source    string
	ReleaseID string
	// RemediationRound is the release's remediation round this trigger belongs to (1 for the rejection itself).
	RemediationRound int
	Repo             string
	CommitSHA        string
	// CodeBundleURI locates the release's code-bundle document, which carries
	// every parsed node's raw source. Empty for a compile-stage and a
	// parse-stage rejection, both of which precede the successful parse that
	// produces the bundle; every later rejection (duplicate_table included)
	// carries it.
	CodeBundleURI string
	// Nodes is the release's healable failing set, one entry per node.
	Nodes []TriggerNode
	// MessageID is the Redis Stream message ID of the inbound
	// remediation.requested:v3 message. It is the primary dedup key.
	MessageID string
	// OutboxEntryID, when non-nil, is the upstream pkg/outbox row UUID carried
	// in the message's outbox_entry_id field. It provides a secondary dedup axis
	// that catches the case where the classifier's outbox Processor crashed
	// between XADD and MarkProcessed and republished the same outbox row with a
	// fresh Redis message_id.
	OutboxEntryID *uuid.UUID
	// RawPayload is the raw bytes of the message payload, stored in the
	// message_processing row for audit/replay purposes and on a verifying
	// proposal so the attempt can be rebuilt once its verification runs answer.
	RawPayload []byte
}

// TriggerNode is one failing node inside a batched trigger, with the evidence
// its fix reads.
type TriggerNode struct {
	NodeID string
	// RelationID is the contested physical relation for a duplicate-relation
	// failure, distinct from NodeID (the target claimant's own unique_id) —
	// the two differ whenever the target already carries an alias. Empty for
	// every other source.
	RelationID     string
	ErrorSignature string
	Category       string
	// Reason is the classifier's finer-grained reason within Category; with
	// Category it forms the fallback precedent-lookup key when the exact
	// signature has no recorded matches.
	Reason string
	// ErrorExcerpt is the classifier's key error line for this failure (capped
	// at 4 KiB).
	ErrorExcerpt         string
	DBTLogURI            string
	CandidateArtifactURI string
	// FilePath is the offending dbt-project-relative source path. For compile
	// failures it is extracted from the dbt log. For validation, seed_build,
	// and duplicate_table failures it is threaded from the candidate topology
	// (OriginalFilePath on release.Node). Empty on a validation or seed_build
	// node falls back to the catalog graph's NodeLocator; a
	// duplicate_table node has no such fallback.
	FilePath string
	// Service is the owning dbt service name for the failing node. Set for
	// validation, seed_build, and duplicate_table failures from the candidate
	// topology. Empty for compile failures where NodeID acts as the service
	// discriminator.
	Service string
	// NodeType is the failing node's kind (dbt-model, dbt-seed,
	// dbt-snapshot, python-node, python-csv, or python-api), set on validation,
	// duplicate-relation and parse failures. It selects the Fixer for a
	// validation or parse failure — a python-node, whose source is not
	// a single readable file, is fixed in the ODCS contract file declaring it —
	// and lets the duplicate-table Fixer, and the parse Fixer for a
	// python-csv node, refuse a python node with a recorded reason and no
	// topology lookup of their own.
	NodeType string
	// OtherService and OtherFilePath locate the competing node that also
	// produces the contested relation (RelationID), set on a duplicate-relation
	// failure.
	OtherService  string
	OtherFilePath string
	// ChangedAncestors are the node's transitive upstream ancestors whose
	// content changed in the rejected release, each with the location that
	// release's candidate topology declares for it. They let the driver group
	// failures that share a changed ancestor, target that ancestor with one fix,
	// and edit it where THIS release holds it — an ancestor the release renamed
	// or moved is still at its old path in the promoted graph.
	ChangedAncestors []ChangedAncestor
}

// ChangedAncestor is one changed upstream of a failing node. FilePath and
// Service are the location the rejected release's candidate topology declares;
// both are empty on a rejection that carries no per-node topology, and the
// upstream fixer then falls back to the promoted graph. Depth is the ancestor's
// minimum upstream hop distance from the failing node (1 = direct upstream); 0
// on a payload that carries none.
type ChangedAncestor struct {
	NodeID   string
	FilePath string
	Service  string
	Depth    int
}

// NodeIDs returns the trigger's failing node ids, sorted. Every batched write
// derives its order from this — the resolved node set, the representative node,
// and the per-node outcomes — so it must not depend on the order the classifier
// listed the failures in. The trigger's own Nodes slice is left as delivered.
func (t Trigger) NodeIDs() []string {
	ids := make([]string, 0, len(t.Nodes))
	for _, n := range t.Nodes {
		ids = append(ids, n.NodeID)
	}
	sort.Strings(ids)
	return ids
}

// Services is the sorted set of services the trigger's failing nodes belong to.
func (t Trigger) Services() []string {
	names := make([]string, 0, len(t.Nodes))
	for _, n := range t.Nodes {
		names = append(names, n.Service)
	}
	return proposal.UnionServices(names)
}

// idempotencyKey identifies this inbound trigger for LLM response caching. It
// mirrors the message-processing dedup identity: the upstream OutboxEntryID when
// present (stable across a Redis republish of the same logical trigger with a
// fresh message id), otherwise the Redis MessageID. Two distinct triggers — for
// example successive remediation attempts for the same release — never share a
// key, so the cache dedupes redeliveries without suppressing legitimate retries.
// The cache also hashes each request, so the several model calls one trigger
// makes for its several clusters do not collide under this one key.
func (t Trigger) idempotencyKey() string {
	if t.OutboxEntryID != nil {
		return "outbox:" + t.OutboxEntryID.String()
	}
	return "msg:" + t.MessageID
}
