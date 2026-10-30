package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
)

// ReleasePromotionRepository swaps a tenant's live Neo4j topology when a
// promotion is newer than the one the graph holds. Implementations MUST run the
// whole swap in one Neo4j transaction that first write-locks the tenant's
// :Meta {key:'current_release'} node and only then reads its promotion_seq, so
// concurrent promotions of a tenant serialise and an older one can never revert
// a newer one.
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

// CodeVersionRepository writes the code-version history behind the :Table
// topology. Implementations decide what to write by comparing each incoming
// node's content_hash against the version the graph currently marks as current
// — never against a flag carried by the event — so any later release converges a
// graph that missed a write.
type CodeVersionRepository interface {
	// WriteVersions ingests one release's versions. It is idempotent: replaying
	// the same input the second time writes nothing.
	//
	// Promoting a node's version also resolves the case base: every still-open
	// :Rejection of that node is forward-linked [:RESOLVED_BY] to the version
	// that just became current, under the same per-node watermark guard as the
	// pointer move. This is the ordinary-case half of the convergence — the
	// fix promoted after the rejection was recorded; CaseBaseRepository's
	// RecordRejection covers the reverse order, back-linking a rejection to a
	// version already promoted before it arrived. RejectionsResolved counts
	// the links this write created.
	WriteVersions(ctx context.Context, in codeversion.WriteInput) (codeversion.WriteResult, error)
}

// CaseBaseRepository writes the failure-precedent case base. Both writes are
// idempotent MERGEs on natural identity, so redeliveries and out-of-order
// arrival converge: a proposal landing before its rejection creates a stub the
// rejection later fills.
type CaseBaseRepository interface {
	// RecordRejection upserts the rejection and its signature hub node, anchors
	// it to the node's :Table when one exists (never creating a :Table — the
	// topology handler owns that lifecycle), and back-links [:RESOLVED_BY] when
	// a version newer than the rejection is already recorded.
	RecordRejection(ctx context.Context, r casebase.Rejection) error
	// RecordProposal upserts the proposal, its [:PROPOSED] edge, and the PR
	// facts on the linked :PullRequest node (keyed by proposal_id + service).
	RecordProposal(ctx context.Context, p casebase.Proposal, pr casebase.PullRequest) error
	// RecordPullRequestOutcome stamps a fix PR's terminal state on its
	// :PullRequest node and, on a merged outcome, draws the case-base
	// provenance edges: [:RESOLVED_BY] from each resolved :Rejection to the
	// shared :Proposal (creating stub rejections when absent) and [:EDITED]
	// from that :Proposal to each edit's :Table (skipped when the :Table is
	// absent, never creating one). A rejected outcome only stamps the terminal
	// state. Idempotent under redelivery.
	RecordPullRequestOutcome(ctx context.Context, o casebase.PullRequestOutcome) error
}
