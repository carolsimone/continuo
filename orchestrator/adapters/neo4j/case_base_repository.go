package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// CaseBaseRepository implements repository.CaseBaseRepository against Neo4j.
// Every write is a MERGE on natural identity so redeliveries and out-of-order
// arrival converge without coordination.
type CaseBaseRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

var _ repository.CaseBaseRepository = (*CaseBaseRepository)(nil)

// NewCaseBaseRepository constructs a CaseBaseRepository backed by the given
// Neo4j client.
func NewCaseBaseRepository(client Neo4jClient, logger *slog.Logger) *CaseBaseRepository {
	return &CaseBaseRepository{client: client, logger: logger}
}

// RecordRejection upserts one rejection. In one statement it: fills the
// rejection's properties (plain SET, so a proposal-created stub is completed),
// MERGEs the signature hub and its edge, anchors [:FAILED] only when the
// node's :Table exists (FOREACH-guarded — this writer must never mint a
// :Table, which would leak a non-promoted node into scheduler snapshots), and
// back-links [:RESOLVED_BY] to the OLDEST version promoted after the
// rejection when the fix already landed before this consumer caught up.
func (r *CaseBaseRepository) RecordRejection(ctx context.Context, rej casebase.Rejection) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	res, err := session.Run(ctx, `
		MERGE (rej:Rejection {release_id: $release_id, node_id: $node_id})
		SET rej.stage = $stage,
		    rej.category = $category,
		    rej.reason = $reason,
		    rej.error_excerpt = $error_excerpt,
		    rej.dbt_log_uri = $dbt_log_uri,
		    rej.at = $at,
		    rej.raw_code = $raw_code,
		    rej.content_hash = $content_hash,
		    rej.stub = false
		MERGE (sig:ErrorSignature {signature: $signature})
		ON CREATE SET sig.category = $category, sig.reason = $reason
		MERGE (rej)-[:HAS_SIGNATURE]->(sig)
		WITH rej
		OPTIONAL MATCH (t:Table {unique_id: $node_id})
		FOREACH (_ IN CASE WHEN t IS NULL THEN [] ELSE [1] END |
		  MERGE (t)-[:FAILED {release_id: $release_id}]->(rej))
		WITH rej
		OPTIONAL MATCH (rej)-[existing:RESOLVED_BY]->()
		WITH rej, existing
		OPTIONAL MATCH (v:NodeVersion {unique_id: $node_id})
		  WHERE existing IS NULL AND v.promoted_at > $at
		WITH rej, v ORDER BY v.promoted_at ASC LIMIT 1
		FOREACH (_ IN CASE WHEN v IS NULL THEN [] ELSE [1] END |
		  MERGE (rej)-[:RESOLVED_BY]->(v))
	`, map[string]any{
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
	})
	if err != nil {
		return fmt.Errorf("record rejection %s/%s: %w", rej.ReleaseID, rej.NodeID, err)
	}
	if _, err := res.Consume(ctx); err != nil {
		return fmt.Errorf("record rejection %s/%s: %w", rej.ReleaseID, rej.NodeID, err)
	}
	return nil
}

// RecordProposal upserts one proposal and its [:PROPOSED] edge. When the
// rejection has not landed yet, a stub :Rejection is MERGEd so the edge has an
// anchor; the stub's at is the proposal's opened_at (approximate) and
// RecordRejection later corrects it when the rejection itself arrives.
func (r *CaseBaseRepository) RecordProposal(ctx context.Context, p casebase.Proposal) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	res, err := session.Run(ctx, `
		MERGE (rej:Rejection {release_id: $release_id, node_id: $node_id})
		ON CREATE SET rej.at = $opened_at, rej.stub = true
		MERGE (pr:Proposal {proposal_id: $proposal_id})
		ON CREATE SET pr.pr_url = $pr_url,
		              pr.pr_number = $pr_number,
		              pr.pr_state = 'open',
		              pr.opened_by = $opened_by,
		              pr.opened_at = $opened_at
		MERGE (rej)-[:PROPOSED]->(pr)
	`, map[string]any{
		"release_id":  p.ReleaseID,
		"node_id":     p.NodeID,
		"proposal_id": p.ProposalID,
		"pr_url":      p.PrURL,
		"pr_number":   p.PrNumber,
		"opened_by":   p.OpenedBy,
		"opened_at":   p.OpenedAt.UTC(),
	})
	if err != nil {
		return fmt.Errorf("record proposal %s: %w", p.ProposalID, err)
	}
	if _, err := res.Consume(ctx); err != nil {
		return fmt.Errorf("record proposal %s: %w", p.ProposalID, err)
	}
	return nil
}
