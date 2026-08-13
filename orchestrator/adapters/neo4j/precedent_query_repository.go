// File: orchestrator/adapters/neo4j/precedent_query_repository.go
//
// PrecedentQueryRepository reads the failure-precedent case base the write-side
// repository in case_base_repository.go produces. It issues read-only Cypher
// exclusively.
package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// PrecedentQueryRepository reads the failure-precedent case base.
type PrecedentQueryRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

var _ queries.PrecedentReader = (*PrecedentQueryRepository)(nil)

// NewPrecedentQueryRepository constructs a PrecedentQueryRepository backed by
// the given Neo4j client.
func NewPrecedentQueryRepository(client Neo4jClient, logger *slog.Logger) *PrecedentQueryRepository {
	return &PrecedentQueryRepository{client: client, logger: logger}
}

// Precedents returns rejections matching the signature — or, when signature is
// empty, the (category, reason) pair — resolved-first then newest, capped at
// limit. The identity query ranks and LIMITs on scalar properties only; code
// bodies are fetched in a second batched query for the survivors, so a large
// case base never pays to load code it is about to discard. includeCode
// controls the failing raw_code only; resolving/prior version bodies are
// always fetched because the caller renders the resolution diff from them.
func (r *PrecedentQueryRepository) Precedents(
	ctx context.Context,
	signature, category, reason string,
	limit int32,
	includeCode bool,
) ([]casebase.PrecedentView, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	idsResult, err := session.Run(ctx, `
		MATCH (sig:ErrorSignature)
		WHERE ($signature <> '' AND sig.signature = $signature)
		   OR ($signature = '' AND sig.category = $category AND sig.reason = $reason)
		MATCH (rej:Rejection)-[:HAS_SIGNATURE]->(sig)
		OPTIONAL MATCH (rej)-[:RESOLVED_BY]->(res:NodeVersion)
		WITH rej, res IS NOT NULL AS resolved
		ORDER BY resolved DESC, rej.at DESC
		LIMIT $limit
		RETURN rej.release_id AS release_id, rej.node_id AS node_id
	`, map[string]any{
		"signature": signature, "category": category, "reason": reason,
		"limit": int64(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("precedent identity query: %w", err)
	}
	type key struct{ releaseID, nodeID string }
	var order []key
	var keys []map[string]any
	for idsResult.Next(ctx) {
		rec := idsResult.Record()
		rel, _ := recordString(rec, "release_id")
		nod, _ := recordString(rec, "node_id")
		order = append(order, key{rel, nod})
		keys = append(keys, map[string]any{"release_id": rel, "node_id": nod})
	}
	if err := idsResult.Err(); err != nil {
		return nil, fmt.Errorf("iterate precedent identities: %w", err)
	}
	if len(keys) == 0 {
		return []casebase.PrecedentView{}, nil
	}

	failingCode := `coalesce(rej.raw_code, '') AS raw_code,`
	if !includeCode {
		failingCode = `'' AS raw_code,`
	}
	detailResult, err := session.Run(ctx, `
		UNWIND $keys AS k
		MATCH (rej:Rejection {release_id: k.release_id, node_id: k.node_id})
		OPTIONAL MATCH (rej)-[:RESOLVED_BY]->(res:NodeVersion)
		OPTIONAL MATCH (prior:NodeVersion {unique_id: res.unique_id})
		  WHERE prior.promoted_at < res.promoted_at
		WITH rej, res, prior ORDER BY prior.promoted_at DESC
		WITH rej, res, head(collect(prior)) AS prior
		OPTIONAL MATCH (rej)-[:PROPOSED]->(p:Proposal)
		WITH rej, res, prior,
		     collect(p {.proposal_id, .pr_url, .pr_number, .pr_state}) AS proposals
		RETURN rej.release_id AS release_id, rej.node_id AS node_id,
		       coalesce(rej.stage, '') AS stage,
		       coalesce(rej.category, '') AS category,
		       coalesce(rej.reason, '') AS reason,
		       coalesce(rej.error_excerpt, '') AS error_excerpt,
		       coalesce(rej.dbt_log_uri, '') AS dbt_log_uri,
		       rej.at AS at,
		       `+failingCode+`
		       coalesce(rej.content_hash, '') AS content_hash,
		       res { .* } AS res, prior { .* } AS prior, proposals
	`, map[string]any{"keys": keys})
	if err != nil {
		return nil, fmt.Errorf("precedent detail query: %w", err)
	}

	byKey := make(map[key]casebase.PrecedentView, len(order))
	for detailResult.Next(ctx) {
		rec := detailResult.Record()
		rel, _ := recordString(rec, "release_id")
		nod, _ := recordString(rec, "node_id")
		v := casebase.PrecedentView{Rejection: casebase.Rejection{ReleaseID: rel, NodeID: nod}}
		v.Rejection.Stage, _ = recordString(rec, "stage")
		v.Rejection.Category, _ = recordString(rec, "category")
		v.Rejection.Reason, _ = recordString(rec, "reason")
		v.Rejection.Signature = signature // identity query matched it; "" on category+reason lookups is acceptable
		v.Rejection.ErrorExcerpt, _ = recordString(rec, "error_excerpt")
		v.Rejection.DBTLogURI, _ = recordString(rec, "dbt_log_uri")
		v.Rejection.RawCode, _ = recordString(rec, "raw_code")
		v.Rejection.ContentHash, _ = recordString(rec, "content_hash")
		if at, ok := rec.Get("at"); ok && at != nil {
			if ts, ok := at.(time.Time); ok {
				v.Rejection.At = ts
			}
		}
		v.ResolvingVersion = versionViewFromProps(rec, "res")
		v.PriorVersion = versionViewFromProps(rec, "prior")
		if raw, ok := rec.Get("proposals"); ok {
			if list, ok := raw.([]any); ok {
				for _, item := range list {
					m, ok := item.(map[string]any)
					if !ok {
						continue
					}
					pv := casebase.ProposalView{}
					pv.ProposalID, _ = m["proposal_id"].(string)
					pv.PrURL, _ = m["pr_url"].(string)
					if n, ok := m["pr_number"].(int64); ok {
						pv.PrNumber = int(n)
					}
					pv.PrState, _ = m["pr_state"].(string)
					v.Proposals = append(v.Proposals, pv)
				}
			}
		}
		byKey[key{rel, nod}] = v
	}
	if err := detailResult.Err(); err != nil {
		return nil, fmt.Errorf("iterate precedent details: %w", err)
	}

	out := make([]casebase.PrecedentView, 0, len(order))
	for _, k := range order { // identity query's order is authoritative
		if v, ok := byKey[k]; ok {
			out = append(out, v)
		}
	}
	return out, nil
}

// versionViewFromProps maps a `node { .* }` projection column — a Neo4j
// property map, not a set of flat named columns — to a VersionView; nil when
// the column is null (no such version). A precedent row carries two version
// columns (the resolving version and the one it superseded) side by side, so
// each is projected as its own property map rather than as flat columns like
// nodeVersionColumns produces elsewhere: two flat column sets in one row
// would collide on names such as content_hash.
func versionViewFromProps(rec *neo4j.Record, column string) *codeversion.VersionView {
	raw, ok := rec.Get(column)
	if !ok || raw == nil {
		return nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	v := &codeversion.VersionView{}
	v.UniqueID, _ = m["unique_id"].(string)
	if seq, ok := m["version_seq"].(int64); ok {
		v.VersionSeq = seq
	}
	v.ContentHash, _ = m["content_hash"].(string)
	v.SourceHash, _ = m["source_hash"].(string)
	v.SharedCodeHash, _ = m["shared_code_hash"].(string)
	v.ConfigHash, _ = m["config_hash"].(string)
	v.Runtime, _ = m["runtime"].(string)
	v.RawCode, _ = m["raw_code"].(string)
	v.CompiledCode, _ = m["compiled_code"].(string)
	v.CompiledTruncated, _ = m["compiled_truncated"].(bool)
	v.ConfigJSON, _ = m["config_json"].(string)
	v.Repo, _ = m["repo"].(string)
	v.CommitSHA, _ = m["commit_sha"].(string)
	v.ReleaseID, _ = m["release_id"].(string)
	if at, ok := m["promoted_at"].(time.Time); ok {
		v.PromotedAt = at
	}
	v.Healed, _ = m["healed"].(bool)
	v.Backfilled, _ = m["backfilled"].(bool)
	return v
}
