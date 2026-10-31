package neo4jinfra

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/repository"
)

// recordRejectionCypher upserts one rejection of the tenant. In one statement
// it: fills the rejection's properties (plain SET, so a proposal-created stub
// is completed), MERGEs the tenant's signature hub and its edge, anchors
// [:FAILED] only when the tenant holds the node's :Node (FOREACH-guarded: a
// retired node still exists and takes the link, and this writer never creates
// a :Node), and back-links [:RESOLVED_BY] to a :NodeVersion when the fix
// already landed before this consumer caught up.
//
// The existing-edge guard is scoped to that target-label family: RESOLVED_BY
// is a shared relationship type, and a merged fix PR can draw its own
// RESOLVED_BY->(:Proposal) provenance edge (RecordPullRequestOutcome) before
// the rejection lands through this path, so that edge's presence must not
// suppress this own-timeline link. Two branches feed the back-link, and the
// first to match wins:
//   - the ordinary case links the OLDEST :NodeVersion whose own promoted_at is
//     after the rejection;
//   - the revert case covers a promotion that reuses an existing, older
//     version node (version nodes are immutable, so a revert's promoted_at
//     stays at its original, earlier value) — it is detected instead through
//     the node's [:CURRENT] edge, which the version writer always overwrites
//     with the promotion that put it there, and only applies when the
//     ordinary case found nothing.
//
// Neither branch can attribute a fix through a revert followed by a further
// promotion. The hub keeps the category and reason it was created with: a
// later rejection sharing its signature can carry another reason, so every
// category and reason lookup reads the :Rejection's own properties.
const recordRejectionCypher = `
MERGE (rej:Rejection {tenant_id: $tenant_id, release_id: $release_id, node_id: $node_id})
SET rej.stage = $stage,
    rej.category = $category,
    rej.reason = $reason,
    rej.error_excerpt = $error_excerpt,
    rej.dbt_log_uri = $dbt_log_uri,
    rej.at = $at,
    rej.raw_code = $raw_code,
    rej.content_hash = $content_hash,
    rej.stub = false
MERGE (sig:ErrorSignature {tenant_id: $tenant_id, signature: $signature})
ON CREATE SET sig.category = $category, sig.reason = $reason
MERGE (rej)-[:HAS_SIGNATURE]->(sig)
WITH rej
OPTIONAL MATCH (n:Node {tenant_id: $tenant_id, unique_id: $node_id})
FOREACH (_ IN CASE WHEN n IS NULL THEN [] ELSE [1] END |
  MERGE (n)-[:FAILED {release_id: $release_id}]->(rej))
WITH rej
OPTIONAL MATCH (rej)-[existing:RESOLVED_BY]->(:NodeVersion {tenant_id: $tenant_id})
WITH rej, existing
OPTIONAL MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: $node_id})
  WHERE existing IS NULL AND v.promoted_at > $at
WITH rej, existing, v ORDER BY v.promoted_at ASC LIMIT 1
OPTIONAL MATCH (n:Node {tenant_id: $tenant_id, unique_id: $node_id})-[cur:CURRENT]->(curV:NodeVersion {tenant_id: $tenant_id})
  WHERE existing IS NULL AND v IS NULL
    AND cur.promoted_at > $at AND curV.promoted_at <= $at
WITH rej, coalesce(v, curV) AS resolved
FOREACH (_ IN CASE WHEN resolved IS NULL THEN [] ELSE [1] END |
  MERGE (rej)-[:RESOLVED_BY]->(resolved))`

// recordProposalCypher upserts one proposal of the tenant, its [:PROPOSED]
// edge, and the PR facts on the linked :PullRequest. When the rejection has
// not landed yet, a stub :Rejection is MERGEd so the edge has an anchor; the
// stub's at is the proposal's opened_at (approximate) and RecordRejection
// later corrects it when the rejection itself arrives. The PR facts land on
// :PullRequest rather than inline on :Proposal, MERGEd on (proposal_id,
// service) so a redelivery converges onto the one PR node.
//
// The open event and the close event are consumed by independent groups, so
// either can land first. When the :PullRequest already exists — a replay, or a
// close that arrived before this open — ON MATCH fills the open-only facts
// (opened_by, opened_at) and backfills pr_url/pr_number only while they are
// still blank, but never touches pr_state: a close-first node already carries
// its terminal 'merged'/'rejected', and opening it must not reset that to
// 'open'.
//
// The [:PROPOSED] edge carries the proposing service, so a precedent read of a
// proposal that spans services can scope each rejection's PR facts to the
// service that proposed the fix for it: a proposal shared by several
// rejections links them all to the one :Proposal, but each rejection's PR
// facts live on its own service's :PullRequest, and the edge's service selects
// the right one.
const recordProposalCypher = `
MERGE (rej:Rejection {tenant_id: $tenant_id, release_id: $release_id, node_id: $node_id})
ON CREATE SET rej.at = $opened_at, rej.stub = true
MERGE (prop:Proposal {tenant_id: $tenant_id, proposal_id: $proposal_id})
MERGE (rej)-[pe:PROPOSED]->(prop)
SET pe.service = $service
MERGE (prop)-[:HAS_PR]->(pull:PullRequest {tenant_id: $tenant_id, proposal_id: $proposal_id, service: $service})
ON CREATE SET pull.pr_url = $pr_url,
              pull.pr_number = $pr_number,
              pull.pr_state = $pr_state,
              pull.opened_by = $opened_by,
              pull.opened_at = $opened_at
ON MATCH SET pull.opened_by = $opened_by,
             pull.opened_at = $opened_at,
             pull.pr_url = CASE WHEN pull.pr_url IS NULL OR pull.pr_url = '' THEN $pr_url ELSE pull.pr_url END,
             pull.pr_number = CASE WHEN pull.pr_number IS NULL OR pull.pr_number = 0 THEN $pr_number ELSE pull.pr_number END`

// recordPullRequestOutcomeCypher stamps a fix PR's terminal state on the
// tenant's :PullRequest — the node RecordProposal opened, matched through
// [:HAS_PR] on (proposal_id, service). When this close arrives before the PR's
// open event, the MERGE creates the :PullRequest and ON CREATE fills the
// pr_url/pr_number the close event also carries, so a close-first node is not
// left blank until the open event's later ON MATCH fills its opened_by and
// opened_at.
//
// It then draws the provenance edges $resolved and $edits name, which the
// caller leaves empty for a rejected outcome: [:RESOLVED_BY] from each
// resolved :Rejection to the shared :Proposal (a stub :Rejection is MERGEd
// when the rejection has not landed yet, as RecordProposal does), and
// [:EDITED] from that :Proposal to each edit's :Node, only when the tenant
// holds the node: a retired node still exists and takes the link, and this
// writer never creates a :Node. Every [:RESOLVED_BY] carries the same
// amended flag and the resolving service; every [:EDITED] carries its own
// edit's amended flag, diff and the service.
//
// FOREACH over $resolved keeps the single (pr) row even when the list is
// empty, so the [:EDITED] UNWIND that follows runs against one row rather than
// the per-resolved-node fan-out. Writes made before an empty UNWIND persist,
// so the :PullRequest state lands for an outcome with neither resolved nodes
// nor edits.
const recordPullRequestOutcomeCypher = `
MERGE (pr:Proposal {tenant_id: $tenant_id, proposal_id: $proposal_id})
MERGE (pr)-[:HAS_PR]->(pull:PullRequest {tenant_id: $tenant_id, proposal_id: $proposal_id, service: $service})
ON CREATE SET pull.pr_url = $pr_url, pull.pr_number = $pr_number
SET pull.pr_state = $outcome, pull.closed_at = $closed_at
WITH pr
FOREACH (nid IN $resolved |
  MERGE (rej:Rejection {tenant_id: $tenant_id, release_id: $release_id, node_id: nid})
  ON CREATE SET rej.at = $closed_at, rej.stub = true
  MERGE (rej)-[rb:RESOLVED_BY]->(pr)
  SET rb.amended = $amended_any, rb.service = $service)
WITH pr
UNWIND $edits AS e
OPTIONAL MATCH (n:Node {tenant_id: $tenant_id, unique_id: e.target_node_id})
FOREACH (_ IN CASE WHEN n IS NULL THEN [] ELSE [1] END |
  MERGE (pr)-[ed:EDITED {path: e.path}]->(n)
  SET ed.amended = e.amended, ed.diff = e.diff, ed.service = $service)`

// caseBaseRepository writes one tenant's case base on the open transaction of
// a unit-of-work Write. Every write is a MERGE on natural identity within the
// tenant, so redeliveries and out-of-order arrival converge without
// coordination.
type caseBaseRepository struct {
	run TenantRunner
}

var _ repository.CaseBaseRepository = (*caseBaseRepository)(nil)

// RecordRejection upserts one rejection of tenantID (recordRejectionCypher).
// It refuses a tenantID other than the transaction's.
func (r *caseBaseRepository) RecordRejection(ctx context.Context, tenantID string, rej casebase.Rejection) error {
	if err := r.run.requireTenant(tenantID); err != nil {
		return err
	}
	// The Neo4j Go driver names a time.Time's zone by its Location().String(),
	// and Neo4j rejects time.Local's "Local", so every time is sent in UTC.
	if err := execCypher(ctx, r.run, recordRejectionCypher, map[string]any{
		"release_id":    rej.ReleaseID,
		"node_id":       rej.NodeID,
		"stage":         rej.Stage,
		"category":      rej.Category,
		"reason":        rej.Reason,
		"signature":     rej.Signature,
		"error_excerpt": rej.ErrorExcerpt,
		"dbt_log_uri":   rej.DBTLogURI,
		"at":            rej.At.UTC(),
		"raw_code":      rej.RawCode,
		"content_hash":  rej.ContentHash,
	}); err != nil {
		return fmt.Errorf("record rejection %s/%s: %w", rej.ReleaseID, rej.NodeID, err)
	}
	return nil
}

// RecordProposal upserts one proposal of tenantID, its [:PROPOSED] edge and
// its PR facts (recordProposalCypher). It refuses a tenantID other than the
// transaction's.
func (r *caseBaseRepository) RecordProposal(ctx context.Context, tenantID string, p casebase.Proposal, pr casebase.PullRequest) error {
	if err := r.run.requireTenant(tenantID); err != nil {
		return err
	}
	if err := execCypher(ctx, r.run, recordProposalCypher, map[string]any{
		"release_id":  p.ReleaseID,
		"node_id":     p.NodeID,
		"proposal_id": p.ProposalID,
		"service":     pr.Service,
		"pr_url":      pr.PrURL,
		"pr_number":   pr.PrNumber,
		"pr_state":    pr.State,
		"opened_by":   pr.OpenedBy,
		"opened_at":   pr.OpenedAt.UTC(),
	}); err != nil {
		return fmt.Errorf("record proposal %s: %w", p.ProposalID, err)
	}
	return nil
}

// RecordPullRequestOutcome stamps a fix PR's terminal state for tenantID and,
// on a merged outcome only, draws its provenance edges
// (recordPullRequestOutcomeCypher): a rejected outcome updates the terminal
// state and nothing else, whatever its payload carries. It refuses a tenantID
// other than the transaction's.
func (r *caseBaseRepository) RecordPullRequestOutcome(ctx context.Context, tenantID string, o casebase.PullRequestOutcome) error {
	if err := r.run.requireTenant(tenantID); err != nil {
		return err
	}
	resolved := []string{}
	edits := []map[string]any{}
	amendedAny := false
	if o.Outcome == "merged" {
		resolved = o.ResolvedNodeIDs
		for _, e := range o.Edits {
			edits = append(edits, map[string]any{
				"path":           e.Path,
				"target_node_id": e.TargetNodeID,
				"amended":        e.Amended,
				"diff":           e.Diff,
			})
			amendedAny = amendedAny || e.Amended
		}
	}
	if err := execCypher(ctx, r.run, recordPullRequestOutcomeCypher, map[string]any{
		"proposal_id": o.ProposalID,
		"release_id":  o.ReleaseID,
		"service":     o.Service,
		"outcome":     o.Outcome,
		"closed_at":   o.ClosedAt.UTC(),
		"pr_url":      o.PrURL,
		"pr_number":   o.PrNumber,
		"resolved":    resolved,
		"edits":       edits,
		"amended_any": amendedAny,
	}); err != nil {
		return fmt.Errorf("record pull request outcome %s/%s: %w", o.ReleaseID, o.ProposalID, err)
	}
	return nil
}
