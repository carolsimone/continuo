package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
)

// ReleasePromotionRepository swaps a tenant's live Neo4j topology when a
// promotion is newer than the one the graph holds. A tenant's promotions apply
// one at a time, and an older promotion never reverts a newer one.
type ReleasePromotionRepository interface {
	// PromoteRelease applies the release's topology to tenantID's graph when
	// promotionSeq is greater than the tenant's live promotion's: every node the
	// release carries is live, every other node of the tenant is a retired node,
	// and release_id and promotionSeq are recorded on the tenant's :Meta. A
	// retired node stays in the graph with its versions, failures and fix PRs,
	// and loses only its outgoing dependencies. It returns PromotionApplied when
	// it swapped, PromotionRedelivered when the same promotion is already live,
	// and PromotionStale when a newer promotion is live; in the last two cases
	// the graph is untouched.
	PromoteRelease(
		ctx context.Context,
		tenantID string,
		releaseID string,
		promotionSeq int64,
		nodes []topology.ReleasePromotedTopologyNode,
		now time.Time,
	) (topology.PromotionOutcome, error)
}

// CodeVersionRepository writes a tenant's code-version history behind the
// :Node topology. Implementations decide what to write by comparing each
// incoming node's content_hash against the version the graph currently marks
// as current — never against a flag carried by the event — so any later
// release converges a graph that missed a write.
type CodeVersionRepository interface {
	// WriteBatch ingests one batch of a release's versions into tenantID's
	// graph. It does not wait for the release's topology swap: a node of the
	// batch the graph does not hold yet is created inactive from its entry in
	// in.Topology, which the swap makes live when it applies a release that
	// carries it. in.Topology holds the nodes the release's topology swap
	// writes; a batch node with no entry there is skipped.
	//
	// Every node the batch matches, and every shared-code unit its nodes use,
	// advances its watermark, code_version_promotion_seq, to the larger of its
	// value and in.PromotionSeq, even when its code is unchanged; its :CURRENT
	// moves only while the watermark equals in.PromotionSeq, so an older
	// promotion records its versions and leaves the pointer alone. Every write
	// MERGEs by identity: writing the same batch again writes nothing.
	//
	// Promoting a node's version also resolves the case base: every still-open
	// :Rejection of that node is forward-linked [:RESOLVED_BY] to the version
	// that just became current, under the same watermark guard as the pointer
	// move. This is the ordinary-case half of the convergence — the fix
	// promoted after the rejection was recorded; CaseBaseRepository's
	// RecordRejection covers the reverse order, back-linking a rejection to a
	// version already promoted before it arrived. RejectionsResolved counts the
	// links this write created.
	WriteBatch(ctx context.Context, tenantID string, in codeversion.WriteInput) (codeversion.WriteResult, error)
}

// CaseBaseRepository writes a tenant's failure-precedent case base. Every
// write is an idempotent MERGE on natural identity within the tenant, so
// redeliveries and out-of-order arrival converge: a proposal landing before
// its rejection creates a stub the rejection later fills. It links a
// rejection or a fix PR's edit to the tenant's :Node only when the node
// exists, a retired node included, and never creates a :Node: the topology
// group owns that lifecycle.
type CaseBaseRepository interface {
	// RecordRejection upserts the rejection and the tenant's signature hub
	// node, anchors it [:FAILED] to the node's :Node when one exists, and
	// back-links [:RESOLVED_BY] when a version newer than the rejection is
	// already recorded.
	RecordRejection(ctx context.Context, tenantID string, r casebase.Rejection) error
	// RecordProposal upserts the proposal, its [:PROPOSED] edge, and the PR
	// facts on the linked :PullRequest node (keyed by proposal_id + service).
	RecordProposal(ctx context.Context, tenantID string, p casebase.Proposal, pr casebase.PullRequest) error
	// RecordPullRequestOutcome stamps a fix PR's terminal state on its
	// :PullRequest node and, on a merged outcome, draws the case-base
	// provenance edges: [:RESOLVED_BY] from each resolved :Rejection to the
	// shared :Proposal (creating stub rejections when absent) and [:EDITED]
	// from that :Proposal to each edit's :Node (skipped when the :Node is
	// absent). A rejected outcome only stamps the terminal state. Idempotent
	// under redelivery.
	RecordPullRequestOutcome(ctx context.Context, tenantID string, o casebase.PullRequestOutcome) error
}

// ContractRepository writes a tenant's contract history behind the :Node
// topology: one immutable stored contract per (node, contract hash), one
// promotion edge for each promotion that changed a node's contract, and a
// pointer from each node to its current contract.
type ContractRepository interface {
	// ReadStates returns what the graph holds about the contract history of
	// each node of uniqueIDs, keyed by unique id: its contract watermark, its
	// current contract and counter, its newest counter and the earliest
	// counter of each contract it has held. A node the graph holds with no
	// contract has a zero state; a node the graph does not hold has no entry.
	ReadStates(ctx context.Context, tenantID string, uniqueIDs []string) (map[string]contractversion.NodeState, error)
	// Write applies a promotion's plan to tenantID's graph. A node of
	// plan.Versions the graph does not hold yet is created inactive, which the
	// topology swap makes live when it applies a release that carries it.
	// Every node of plan.Versions advances its watermark,
	// contract_promotion_seq, to the larger of its value and p.PromotionSeq;
	// each stored contract is created only when missing and never changed;
	// each edge of plan.Edges is recorded, and the node's pointer moved to it,
	// only while the node's watermark equals p.PromotionSeq, so an older
	// promotion never takes a node's pointer from a newer one.
	Write(ctx context.Context, tenantID string, p contractversion.Promotion, plan contractversion.WritePlan) (contractversion.WriteResult, error)
}
