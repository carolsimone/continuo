package handlers

import (
	"encoding/json"
	"sort"

	"github.com/google/uuid"
)

// Trigger is the decoded remediation.requested:v2 payload that drives one
// ProposeFix invocation: the release-level facts every fix needs, and one
// TriggerNode per failing node the classifier judged healable. One rejected
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
	// every parsed node's raw source. Empty only for a compile-stage
	// rejection, which precedes the parse that produces the bundle; every
	// post-parse rejection (duplicate_table included) carries it.
	CodeBundleURI string
	// Shadow is true when the rejected release was itself a fix-verification
	// release. The classifier records such rejections but never emits them, so
	// a Shadow trigger is not produced; the field travels for completeness.
	Shadow bool
	// Nodes is the release's healable failing set, one entry per node.
	Nodes []TriggerNode
	// MessageID is the Redis Stream message ID of the inbound
	// remediation.requested:v2 message. It is the primary dedup key.
	MessageID string
	// OutboxEntryID, when non-nil, is the upstream pkg/outbox row UUID carried
	// in the message's outbox_entry_id field. It provides a secondary dedup axis
	// that catches the case where the classifier's outbox Processor crashed
	// between XADD and MarkProcessed and republished the same outbox row with a
	// fresh Redis message_id.
	OutboxEntryID *uuid.UUID
	// RawPayload is the raw bytes of the message payload, stored in the
	// message_processing row for audit/replay purposes and on a verifying
	// proposal so the attempt can be rebuilt once a shadow release answers.
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
	// node falls back to the orchestrator graph's NodeLocator; a
	// duplicate_table node has no such fallback.
	FilePath string
	// Service is the owning dbt service name for the failing node. Set for
	// validation, seed_build, and duplicate_table failures from the candidate
	// topology. Empty for compile failures where NodeID acts as the service
	// discriminator.
	Service string
	// NodeType is the failing node's kind (dbt-model, dbt-seed,
	// dbt-snapshot, python-model, or python-csv), set on validation and
	// duplicate-relation failures. It selects the Fixer for a validation
	// failure — a python node, whose source is not a single readable file,
	// is fixed in the contract yaml declaring it — and lets the
	// duplicate-table Fixer skip a python node without a topology lookup of
	// its own.
	NodeType string
	// OtherService and OtherFilePath locate the competing node that also
	// produces the contested relation (RelationID), set on a duplicate-relation
	// failure.
	OtherService  string
	OtherFilePath string
	// ChangedAncestorIDs are the node's transitive upstream ancestors whose
	// content changed in the rejected release. They let the driver group
	// failures that share a changed ancestor and target that ancestor with one
	// fix.
	ChangedAncestorIDs []string
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

// Subset returns this trigger narrowed to the named nodes: the same
// release-level header, only the nodes whose ids appear in ids (in the order
// this trigger holds them), and a RawPayload re-marshalled to the same wire
// shape the stream carries, so a caller storing it can replay the narrowed
// trigger through the adapter's decoder. Ids the trigger does not carry are
// ignored.
//
// The dedup identity of the message that delivered this trigger is deliberately
// dropped: a replayed subset is a new piece of work, and the caller replaying it
// supplies an identity of its own rather than reusing the one already claimed.
func (t Trigger) Subset(ids []string) Trigger {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	sub := t
	sub.MessageID = ""
	sub.OutboxEntryID = nil
	sub.Nodes = nil
	for _, n := range t.Nodes {
		if want[n.NodeID] {
			sub.Nodes = append(sub.Nodes, n)
		}
	}
	// A payload that cannot be marshalled would mean a node field this package
	// declares is not serializable, which the wire types below rule out.
	raw, err := json.Marshal(sub.wire())
	if err == nil {
		sub.RawPayload = raw
	}
	return sub
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

// TriggerWire is the remediation.requested:v2 wire shape: what the classifier
// publishes, what the stream adapter decodes, and what Subset re-marshals. It
// is declared once here so the decoder and the re-encoder cannot drift apart.
type TriggerWire struct {
	EventID   string `json:"event_id,omitempty"`
	Source    string `json:"source"`
	ReleaseID string `json:"release_id"`
	// RemediationRound is the release's remediation round this trigger belongs
	// to; missing or 0 means round 1 (the rejection itself).
	RemediationRound int    `json:"remediation_round"`
	Repo             string `json:"repo"`
	CommitSHA        string `json:"commit_sha"`
	// CodeBundleURI locates the release's code-bundle document; empty when
	// parse never completed (compile-stage failures).
	CodeBundleURI string `json:"code_bundle_uri,omitempty"`
	// Shadow marks a rejection of a fix-verification release.
	Shadow       bool              `json:"shadow"`
	ClassifiedAt string            `json:"classified_at,omitempty"`
	Nodes        []TriggerNodeWire `json:"nodes"`
}

// TriggerNodeWire is one failing node inside a TriggerWire.
type TriggerNodeWire struct {
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
	// content changed in the rejected release.
	ChangedAncestorIDs []string `json:"changed_ancestor_ids,omitempty"`
}

// wire projects the trigger back onto the wire shape it was decoded from.
func (t Trigger) wire() TriggerWire {
	nodes := make([]TriggerNodeWire, 0, len(t.Nodes))
	for _, n := range t.Nodes {
		nodes = append(nodes, TriggerNodeWire{
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			Category:             n.Category,
			ErrorSignature:       n.ErrorSignature,
			Reason:               n.Reason,
			ErrorExcerpt:         n.ErrorExcerpt,
			DBTLogURI:            n.DBTLogURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Service:              n.Service,
			NodeType:             n.NodeType,
			OtherService:         n.OtherService,
			OtherFilePath:        n.OtherFilePath,
			ChangedAncestorIDs:   n.ChangedAncestorIDs,
		})
	}
	return TriggerWire{
		Source:           t.Source,
		ReleaseID:        t.ReleaseID,
		RemediationRound: t.RemediationRound,
		Repo:             t.Repo,
		CommitSHA:        t.CommitSHA,
		CodeBundleURI:    t.CodeBundleURI,
		Shadow:           t.Shadow,
		Nodes:            nodes,
	}
}

// TriggerFromWire builds a Trigger from a decoded v2 payload. The dedup
// identity fields (MessageID, OutboxEntryID) and RawPayload belong to the
// message that delivered the payload rather than to the payload itself, so the
// caller sets them.
func TriggerFromWire(w TriggerWire) Trigger {
	nodes := make([]TriggerNode, 0, len(w.Nodes))
	for _, n := range w.Nodes {
		nodes = append(nodes, TriggerNode{
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			ErrorSignature:       n.ErrorSignature,
			Category:             n.Category,
			Reason:               n.Reason,
			ErrorExcerpt:         n.ErrorExcerpt,
			DBTLogURI:            n.DBTLogURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Service:              n.Service,
			NodeType:             n.NodeType,
			OtherService:         n.OtherService,
			OtherFilePath:        n.OtherFilePath,
			ChangedAncestorIDs:   n.ChangedAncestorIDs,
		})
	}
	return Trigger{
		Source:           w.Source,
		ReleaseID:        w.ReleaseID,
		RemediationRound: w.RemediationRound,
		Repo:             w.Repo,
		CommitSHA:        w.CommitSHA,
		CodeBundleURI:    w.CodeBundleURI,
		Shadow:           w.Shadow,
		Nodes:            nodes,
	}
}
