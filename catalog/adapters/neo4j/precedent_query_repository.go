// File: catalog/adapters/neo4j/precedent_query_repository.go
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

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/service/queries"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// PrecedentQueryRepository reads the failure-precedent case base. Every read
// runs in one read transaction bound to the tenant it is asked for, and
// reaches only that tenant's nodes.
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

// precedentIdentitiesCypher ranks the tenant's matching rejections,
// resolved first then newest, on scalar properties only, and LIMITs them.
const precedentIdentitiesCypher = `
MATCH (rej:Rejection {tenant_id: $tenant_id})-[:HAS_SIGNATURE]->(sig:ErrorSignature {tenant_id: $tenant_id})
WHERE ($signature <> '' AND sig.signature = $signature)
   OR ($signature = '' AND rej.category = $category AND rej.reason = $reason)
OPTIONAL MATCH (rej)-[:RESOLVED_BY]->(res)
WHERE res:NodeVersion OR res:Proposal
WITH rej, sig.signature AS signature, count(res) > 0 AS resolved
ORDER BY resolved DESC, rej.at DESC
LIMIT $limit
RETURN rej.release_id AS release_id, rej.node_id AS node_id, signature`

// precedentDetailsCypher reads each listed rejection with its resolving
// version, the version that resolving version superseded, whether the
// resolving version is the current version of its live :Node, and its fix
// PRs. It ends with the failing code's column, which the caller appends.
//
// A proposal spanning several services is shared by their rejections, and
// each carries its own :PullRequest. The [:PROPOSED] edge stamps the
// proposing service, so the PR join is scoped to it: a rejection surfaces
// only its own service's PR facts, never a sibling service's. An edge with
// no service falls back to matching any of the proposal's PRs.
const precedentDetailsCypher = `
UNWIND $keys AS k
MATCH (rej:Rejection {tenant_id: $tenant_id, release_id: k.release_id, node_id: k.node_id})
OPTIONAL MATCH (rej)-[rb:RESOLVED_BY]->(res:NodeVersion {tenant_id: $tenant_id})
WITH rej, k.signature AS signature, rb, res
  ORDER BY res.promoted_at ASC, res.content_hash ASC
WITH rej, signature, head(collect(rb)) AS rb, head(collect(res)) AS res
WITH rej, signature, res, coalesce(rb.promoted_at, res.promoted_at) AS resolved_at
OPTIONAL MATCH (prior:NodeVersion {tenant_id: $tenant_id, unique_id: res.unique_id})
  WHERE prior.promoted_at < resolved_at
OPTIONAL MATCH (n:Node {tenant_id: $tenant_id, unique_id: res.unique_id})-[:CURRENT]->(cur:NodeVersion {tenant_id: $tenant_id})
  WHERE coalesce(n.active, true)
WITH rej, signature, res, prior,
     (cur IS NOT NULL AND res IS NOT NULL AND cur.content_hash = res.content_hash) AS res_is_current
  ORDER BY prior.promoted_at DESC
WITH rej, signature, res, res_is_current, head(collect(prior)) AS prior
OPTIONAL MATCH (rej)-[prop:PROPOSED]->(p:Proposal {tenant_id: $tenant_id})
OPTIONAL MATCH (p)-[:HAS_PR]->(pl:PullRequest {tenant_id: $tenant_id})
  WHERE pl.service = prop.service OR prop.service IS NULL
WITH rej, signature, res, prior, res_is_current,
     collect(DISTINCT p {
       .proposal_id,
       pr_url:    coalesce(pl.pr_url, p.pr_url),
       pr_number: coalesce(pl.pr_number, p.pr_number),
       pr_state:  coalesce(pl.pr_state, p.pr_state)
     }) AS proposals
RETURN rej.release_id AS release_id, rej.node_id AS node_id,
       signature,
       coalesce(rej.stage, '') AS stage,
       coalesce(rej.category, '') AS category,
       coalesce(rej.reason, '') AS reason,
       coalesce(rej.error_excerpt, '') AS error_excerpt,
       coalesce(rej.dbt_log_uri, '') AS dbt_log_uri,
       rej.at AS at,
       coalesce(rej.content_hash, '') AS content_hash,
       res { .* } AS res, prior { .* } AS prior, res_is_current, proposals,
       exists { (rej)-[:RESOLVED_BY]->(:Proposal {tenant_id: $tenant_id}) } AS resolved_by_proposal,
       `

// precedentEditsCypher reads the edited-node provenance of each listed
// rejection a merged PR resolved: for each rejection RESOLVED_BY a
// :Proposal, its [:EDITED] edges to the touched :Node, a retired node
// included. For an amended edit, it selects the promoted :NodeVersion
// straddling the PR's close — the first version promoted after closed_at
// (merged) and the newest before it (prior) — so the service can render the
// merged-truth diff.
//
// A proposal spanning several services is shared by their rejections, but
// each RESOLVED_BY edge carries the resolving service, so the walk is scoped
// to it: a rejection resolved by one service's PR surfaces only that
// service's edits and reads only that service's :PullRequest closed_at. An
// edge with no service falls back to the whole-proposal walk.
const precedentEditsCypher = `
UNWIND $keys AS k
MATCH (rej:Rejection {tenant_id: $tenant_id, release_id: k.release_id, node_id: k.node_id})
      -[rb:RESOLVED_BY]->(rp:Proposal {tenant_id: $tenant_id})
MATCH (rp)-[ed:EDITED]->(t:Node {tenant_id: $tenant_id})
  WHERE ed.service = rb.service OR rb.service IS NULL
OPTIONAL MATCH (rp)-[:HAS_PR]->(pl:PullRequest {tenant_id: $tenant_id})
  WHERE pl.pr_state = 'merged' AND (pl.service = rb.service OR rb.service IS NULL)
OPTIONAL MATCH (mv:NodeVersion {tenant_id: $tenant_id, unique_id: t.unique_id})
  WHERE ed.amended AND pl.closed_at IS NOT NULL AND mv.promoted_at > pl.closed_at
WITH rej, ed, t, pl, mv ORDER BY mv.promoted_at ASC
WITH rej, ed, t, pl, head(collect(mv)) AS merged
OPTIONAL MATCH (pv:NodeVersion {tenant_id: $tenant_id, unique_id: t.unique_id})
  WHERE merged IS NOT NULL AND pv.promoted_at < merged.promoted_at
WITH rej, ed, t, merged, pv ORDER BY pv.promoted_at DESC
RETURN rej.release_id AS release_id, rej.node_id AS node_id,
       t.unique_id AS edited_node, ed.path AS path, ed.amended AS amended,
       ed.diff AS diff, merged, head(collect(pv)) AS prior`

// Precedents returns the tenant's rejections matching the signature — or,
// when signature is empty, the (category, reason) pair — resolved-first then
// newest, capped at limit. The identity query ranks and LIMITs on scalar
// properties only; code bodies are fetched in a second batched query for the
// survivors, so a large case base never pays to load code it is about to
// discard. includeCode controls the failing raw_code only; resolving/prior
// version bodies are always fetched because the caller renders the
// resolution diff from them. The three queries run in one read transaction.
//
// The (category, reason) fallback filters on the :Rejection's own category and
// reason, not the :ErrorSignature hub's — the hub's are first-seen metadata
// (see RecordRejection), so two rejections sharing one signature can carry
// different reasons, and only the rejection's own property is guaranteed
// current.
//
// A rejection can carry two [:RESOLVED_BY] edges when the two writers race
// (the versions consumer's forward-link and the rejections consumer's
// back-link), so the detail query picks one deterministically — oldest
// resolving version by promoted_at, tie-broken by content_hash — before
// projecting anything from it, the same "oldest version that could have
// fixed it" rule the back-link itself applies. The diff baseline against
// that resolving version prefers the edge's own promoted_at (stamped by the
// forward-link) over the version node's, since a reverted-to version node
// keeps its original, earlier promoted_at. The resolving version is current
// only while its node is live: a retired node keeps its :CURRENT edge, but
// none of its versions is current.
func (r *PrecedentQueryRepository) Precedents(
	ctx context.Context,
	tenantID, signature, category, reason string,
	limit int32,
	includeCode bool,
) ([]casebase.PrecedentView, error) {
	failingCode := `coalesce(rej.raw_code, '') AS raw_code`
	if !includeCode {
		failingCode = `'' AS raw_code`
	}
	detailsQuery := precedentDetailsCypher + failingCode

	type key struct{ releaseID, nodeID string }
	type read struct {
		order  []key
		byKey  map[key]casebase.PrecedentView
		edited map[key][]casebase.EditedView
	}
	got, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (read, error) {
		out := read{byKey: map[key]casebase.PrecedentView{}, edited: map[key][]casebase.EditedView{}}
		idsResult, err := run.Run(ctx, precedentIdentitiesCypher, map[string]any{
			"signature": signature, "category": category, "reason": reason,
			"limit": int64(limit),
		})
		if err != nil {
			return out, fmt.Errorf("precedent identity query: %w", err)
		}
		var keys []map[string]any
		for idsResult.Next(ctx) {
			rec := idsResult.Record()
			rel, _ := recordString(rec, "release_id")
			nod, _ := recordString(rec, "node_id")
			sig, _ := recordString(rec, "signature")
			out.order = append(out.order, key{rel, nod})
			keys = append(keys, map[string]any{"release_id": rel, "node_id": nod, "signature": sig})
		}
		if err := idsResult.Err(); err != nil {
			return out, fmt.Errorf("iterate precedent identities: %w", err)
		}
		if len(keys) == 0 {
			return out, nil
		}

		detailResult, err := run.Run(ctx, detailsQuery, map[string]any{"keys": keys})
		if err != nil {
			return out, fmt.Errorf("precedent detail query: %w", err)
		}
		for detailResult.Next(ctx) {
			v := precedentViewFromRecord(detailResult.Record())
			out.byKey[key{v.Rejection.ReleaseID, v.Rejection.NodeID}] = v
		}
		if err := detailResult.Err(); err != nil {
			return out, fmt.Errorf("iterate precedent details: %w", err)
		}

		editedResult, err := run.Run(ctx, precedentEditsCypher, map[string]any{"keys": keys})
		if err != nil {
			return out, fmt.Errorf("precedent edited-provenance query: %w", err)
		}
		for editedResult.Next(ctx) {
			rec := editedResult.Record()
			rel, _ := recordString(rec, "release_id")
			nod, _ := recordString(rec, "node_id")
			e := casebase.EditedView{}
			e.NodeID, _ = recordString(rec, "edited_node")
			e.Path, _ = recordString(rec, "path")
			e.Amended = recordBool(rec, "amended")
			e.Diff, _ = recordString(rec, "diff")
			e.MergedVersion = versionViewFromNode(rec, "merged")
			e.MergedPrior = versionViewFromNode(rec, "prior")
			k := key{rel, nod}
			out.edited[k] = append(out.edited[k], e)
		}
		if err := editedResult.Err(); err != nil {
			return out, fmt.Errorf("iterate precedent edited provenance: %w", err)
		}
		return out, nil
	})
	if err != nil {
		return nil, fmt.Errorf("Precedents: %w", err)
	}

	out := make([]casebase.PrecedentView, 0, len(got.order))
	for _, k := range got.order { // identity query's order is authoritative
		if v, ok := got.byKey[k]; ok {
			v.Edited = got.edited[k]
			out = append(out, v)
		}
	}
	return out, nil
}

// precedentViewFromRecord builds a PrecedentView, without its edited-node
// provenance, from one row of precedentDetailsCypher.
func precedentViewFromRecord(rec *neo4j.Record) casebase.PrecedentView {
	rel, _ := recordString(rec, "release_id")
	nod, _ := recordString(rec, "node_id")
	v := casebase.PrecedentView{Rejection: casebase.Rejection{ReleaseID: rel, NodeID: nod}}
	v.Rejection.Stage, _ = recordString(rec, "stage")
	v.Rejection.Category, _ = recordString(rec, "category")
	v.Rejection.Reason, _ = recordString(rec, "reason")
	v.Rejection.Signature, _ = recordString(rec, "signature") // the row's own signature, not the caller's selector
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
	if v.ResolvingVersion != nil {
		v.ResolvingVersion.IsCurrent = recordBool(rec, "res_is_current")
	}
	v.PriorVersion = versionViewFromProps(rec, "prior")
	v.ResolvedByProposal = recordBool(rec, "resolved_by_proposal")
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
	return v
}

// versionViewFromNode maps a bare-node column (a `neo4j.Node`, as returned by
// `RETURN merged` / `head(collect(pv))`) to a VersionView, reading its property
// bag; nil when the column is null. The edited-provenance query returns the
// straddling versions as whole nodes rather than `{ .* }` projections, so this
// unwraps the node's Props before delegating to the shared mapper.
func versionViewFromNode(rec *neo4j.Record, column string) *codeversion.VersionView {
	raw, ok := rec.Get(column)
	if !ok || raw == nil {
		return nil
	}
	node, ok := raw.(neo4j.Node)
	if !ok {
		return nil
	}
	return versionViewFromMap(node.Props)
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
	return versionViewFromMap(m)
}

// versionViewFromMap maps a property bag — from either a `{ .* }` projection or
// a bare node's Props — to a VersionView. It never returns nil for a non-nil
// map: an empty map yields a zero-valued VersionView, so callers that already
// know the column was non-null get a value back.
func versionViewFromMap(m map[string]any) *codeversion.VersionView {
	if m == nil {
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
