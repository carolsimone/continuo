// File: catalog/adapters/neo4j/code_version_query_repository.go
//
// CodeVersionQueryRepository implements queries.CodeVersionReader by reading
// the :NodeVersion/:CodeUnitVersion graph the write-side repository in
// code_version_repository.go produces. It issues read-only Cypher exclusively.
package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"

	"github.com/carolsimone/continuo/catalog/domain"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/service/queries"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// CodeVersionQueryRepository implements queries.CodeVersionReader against
// Neo4j. Every read runs in one read transaction bound to the tenant it is
// asked for, and reaches only that tenant's nodes.
//
// A node's versions are enumerated directly through the (:NodeVersion
// {tenant_id, unique_id}) index — no edge between them is followed — and
// ordered by promoted_at, never version_seq:
//
//   - promoted_at is when the release that introduced this code was
//     promoted — chronologically true however late the event was ingested.
//   - version_seq is a stable per-node handle for addressing one of a node's
//     versions, not an ordering: it is assigned max+1 at ingestion time, so a
//     late-arriving OLDER release receives the HIGHEST seq. Sorting by it
//     would put stale code at the top of a "newest first" view; a late
//     older promotion, whose version is recorded without becoming current,
//     creates exactly this case.
//
// A node's version rows are matched by unique_id directly and joined against
// its :Node's :CURRENT pointer only to compute is_current. Every read of
// :CURRENT from a :Node goes through a live node: a retired node keeps its
// :CURRENT edge as the record of its last version, but none of its versions
// is current, it has no current version and its current version uses no
// shared code. Its full history is still listed. promoted_at sorting happens
// over a per-node result set that is small by construction: versions grow
// with edits, not with releases.
type CodeVersionQueryRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

// Compile-time assertion that the adapter satisfies the read port.
var _ queries.CodeVersionReader = (*CodeVersionQueryRepository)(nil)

// NewCodeVersionQueryRepository constructs a CodeVersionQueryRepository
// backed by the given Neo4j client.
func NewCodeVersionQueryRepository(client Neo4jClient, logger *slog.Logger) *CodeVersionQueryRepository {
	return &CodeVersionQueryRepository{client: client, logger: logger}
}

// nodeVersionColumns projects one :NodeVersion row. It assumes the query
// binds `v` to the version and `cur` to the node's current version (or null
// when there is none), and is shared by every query that reads this shape so
// the column list cannot drift between them. includeCode false keeps
// raw_code/compiled_code off the wire entirely — a version's compiled_code
// alone can run to 256 KiB, so this must control what Neo4j returns, not just
// what the caller keeps, or a "light" request still pays the transport cost.
func nodeVersionColumns(includeCode bool) string {
	code := `v.raw_code AS raw_code, v.compiled_code AS compiled_code,`
	if !includeCode {
		code = `'' AS raw_code, '' AS compiled_code,`
	}
	return `v.version_seq AS version_seq, v.content_hash AS content_hash,
       v.source_hash AS source_hash, v.shared_code_hash AS shared_code_hash,
       v.config_hash AS config_hash, v.runtime AS runtime,
       ` + code + `
       coalesce(v.compiled_truncated, false) AS compiled_truncated,
       coalesce(v.config_json, '{}') AS config_json,
       v.repo AS repo, v.commit_sha AS commit_sha, v.release_id AS release_id,
       v.promoted_at AS promoted_at, coalesce(v.healed, false) AS healed,
       coalesce(v.backfilled, false) AS backfilled,
       (cur IS NOT NULL AND cur.content_hash = v.content_hash) AS is_current`
}

// liveCurrentCypher binds cur to the :CURRENT version of the tenant's live
// :Node $uid, or to null when the node is retired, has no current version or
// does not exist. It is the head of every query that marks is_current.
const liveCurrentCypher = `
OPTIONAL MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})-[:CURRENT]->(cur:NodeVersion {tenant_id: $tenant_id})
WHERE coalesce(n.active, true)
WITH cur
`

// NodeVersions enumerates a node's versions by unique_id, ordered
// promoted_at DESC, up to limit. includeCode false omits raw_code/
// compiled_code from the Cypher projection itself, so a "light" request
// never pulls those bodies out of Neo4j.
func (r *CodeVersionQueryRepository) NodeVersions(ctx context.Context, tenantID, uniqueID string, limit int32, includeCode bool) ([]codeversion.VersionView, error) {
	query := liveCurrentCypher + `
MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: $uid})
RETURN ` + nodeVersionColumns(includeCode) + `
ORDER BY v.promoted_at DESC
LIMIT $limit`
	type read struct {
		versions []codeversion.VersionView
		known    bool
	}
	got, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (read, error) {
		versions, err := collectVersions(ctx, run, query, map[string]any{"uid": uniqueID, "limit": int64(limit)}, uniqueID)
		if err != nil || len(versions) > 0 {
			return read{versions: versions, known: true}, err
		}
		// No :NodeVersion carries this unique_id. That is a known node with an
		// empty history when the tenant holds its :Node, and an unknown node
		// otherwise.
		known, err := nodeKnown(ctx, run, uniqueID)
		return read{versions: versions, known: known}, err
	})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.NodeVersions: %w", err)
	}
	if !got.known {
		return nil, fmt.Errorf("CodeVersionQueryRepository.NodeVersions: node %s: %w", uniqueID, domain.ErrNodeNotFound)
	}
	return got.versions, nil
}

// CurrentNodeVersion returns the single version the node runs now — the
// :CURRENT target of its live :Node — as a 0-or-1 slice. Unlike
// NodeVersions' newest-first ordering, this is revert-safe: after a revert
// the current version is not the newest. An unknown node returns
// ErrNodeNotFound; a known node with no current version returns an empty
// slice, and so does a retired node: it keeps its :CURRENT edge as the record
// of its last version, but that version no longer runs.
func (r *CodeVersionQueryRepository) CurrentNodeVersion(ctx context.Context, tenantID, uniqueID string, includeCode bool) ([]codeversion.VersionView, error) {
	query := `
MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})-[:CURRENT]->(v:NodeVersion {tenant_id: $tenant_id})
WHERE coalesce(n.active, true)
WITH v, v AS cur
RETURN ` + nodeVersionColumns(includeCode) + `
LIMIT 1`
	type read struct {
		versions []codeversion.VersionView
		known    bool
	}
	got, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (read, error) {
		versions, err := collectVersions(ctx, run, query, map[string]any{"uid": uniqueID}, uniqueID)
		if err != nil || len(versions) > 0 {
			return read{versions: versions, known: true}, err
		}
		known, err := nodeKnown(ctx, run, uniqueID)
		return read{versions: versions, known: known}, err
	})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.CurrentNodeVersion: %w", err)
	}
	if !got.known {
		return nil, fmt.Errorf("CodeVersionQueryRepository.CurrentNodeVersion: node %s: %w", uniqueID, domain.ErrNodeNotFound)
	}
	return got.versions, nil
}

// collectVersions runs a query projected by nodeVersionColumns and reads
// every row.
func collectVersions(ctx context.Context, run TenantRunner, query string, params map[string]any, uniqueID string) ([]codeversion.VersionView, error) {
	result, err := run.Run(ctx, query, params)
	if err != nil {
		return nil, err
	}
	versions := make([]codeversion.VersionView, 0)
	for result.Next(ctx) {
		versions = append(versions, versionViewFromRecord(result.Record(), uniqueID))
	}
	return versions, result.Err()
}

// nodeKnownCypher reports whether the tenant holds the node: its :Node, live
// or retired, or at least one :NodeVersion of it.
const nodeKnownCypher = `
OPTIONAL MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})
WITH count(n) > 0 AS has_node
OPTIONAL MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: $uid})
WITH has_node, count(v) > 0 AS has_version
RETURN has_node OR has_version AS known`

// nodeKnown reports whether uniqueID is a recorded node of the run's tenant:
// the tenant holds its :Node, live or retired, or at least one of its
// :NodeVersion rows. This is the known/unknown boundary every code-version
// RPC applies.
func nodeKnown(ctx context.Context, run TenantRunner, uniqueID string) (bool, error) {
	return singleBool(ctx, run, nodeKnownCypher, map[string]any{"uid": uniqueID}, "known")
}

// VersionsBySeq returns the two named versions of one node. A missing seq —
// whether the node has no such version or is unknown entirely — reports
// ErrNodeNotFound naming the seq that could not be found.
func (r *CodeVersionQueryRepository) VersionsBySeq(ctx context.Context, tenantID, uniqueID string, fromSeq, toSeq int64) (codeversion.VersionView, codeversion.VersionView, error) {
	query := liveCurrentCypher + `
MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: $uid})
WHERE v.version_seq IN [$from_seq, $to_seq]
RETURN ` + nodeVersionColumns(true)
	versions, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) ([]codeversion.VersionView, error) {
		return collectVersions(ctx, run, query, map[string]any{"uid": uniqueID, "from_seq": fromSeq, "to_seq": toSeq}, uniqueID)
	})
	if err != nil {
		return codeversion.VersionView{}, codeversion.VersionView{}, fmt.Errorf("CodeVersionQueryRepository.VersionsBySeq: %w", err)
	}
	bySeq := make(map[int64]codeversion.VersionView, 2)
	for _, v := range versions {
		bySeq[v.VersionSeq] = v
	}

	from, ok := bySeq[fromSeq]
	if !ok {
		return codeversion.VersionView{}, codeversion.VersionView{}, fmt.Errorf(
			"CodeVersionQueryRepository.VersionsBySeq: node %s has no version %d: %w", uniqueID, fromSeq, domain.ErrNodeNotFound)
	}
	to, ok := bySeq[toSeq]
	if !ok {
		return codeversion.VersionView{}, codeversion.VersionView{}, fmt.Errorf(
			"CodeVersionQueryRepository.VersionsBySeq: node %s has no version %d: %w", uniqueID, toSeq, domain.ErrNodeNotFound)
	}
	return from, to, nil
}

// ancestorVersionsCypher fetches each listed ancestor's current version (cur)
// plus the newest version other than cur. Rows are grouped by unique_id in Go
// rather than relying on the engine to preserve inter-group order; the
// intra-group order (cur first, then newest-other) comes from the CASE.
const ancestorVersionsCypher = `
UNWIND $ids AS uid
OPTIONAL MATCH (n:Node {tenant_id: $tenant_id, unique_id: uid})-[:CURRENT]->(cur:NodeVersion {tenant_id: $tenant_id})
WHERE coalesce(n.active, true)
WITH uid, cur
OPTIONAL MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: uid})
WHERE cur IS NULL OR v.content_hash <> cur.content_hash
WITH uid, cur, v
ORDER BY v.promoted_at DESC
WITH uid, cur, collect(v)[0..2] AS priorTop
WITH uid, cur, (CASE WHEN cur IS NULL THEN priorTop ELSE [cur] + priorTop END)[0..2] AS top
UNWIND (CASE WHEN size(top) = 0 THEN [null] ELSE top END) AS v
RETURN uid AS unique_id, v IS NOT NULL AS has_version, `

// Ancestors returns up to cap of the node's transitive upstreams within
// depth hops, most-recently-changed first, each with its two most relevant
// versions. Only the live topology is walked, deduplicated to the shortest
// depth at which an ancestor is reachable. depth < 1 walks nothing.
//
// "Most-relevant" and "most-recently-changed" are both computed from an
// ancestor's EFFECTIVE last-change time — max(its newest version's
// promoted_at, its :CURRENT edge's own promoted_at) — not from a version
// node's own promoted_at alone. A version node is immutable (see this file's
// package doc), so a revert that re-points :CURRENT at an existing older
// version leaves that version's promoted_at at its original creation time;
// only the :CURRENT edge records when the revert itself happened. Using the
// version's own timestamp here would rank a revert by its stale creation
// date and let the since filter miss it entirely. This ranking, the since
// filter, and the cap are all applied in idsQuery — before any version body
// is fetched — so a wide DAG never pays to load code for ancestors it is
// about to discard.
//
// The two versions returned per ancestor are, in order: the version :CURRENT
// points to (the "To" of its latest change, if any), then the newest other
// version by promoted_at (the "To"'s "From"). A revert therefore reports the
// actual B→A transition instead of the two versions' original creation
// order.
func (r *CodeVersionQueryRepository) Ancestors(ctx context.Context, tenantID, uniqueID string, depth int32, since time.Time, cap int32) ([]codeversion.AncestorVersions, error) {
	if depth < 1 {
		return []codeversion.AncestorVersions{}, nil
	}
	var sinceParam any
	if !since.IsZero() {
		sinceParam = since
	}

	// Cypher cannot parameterize the *1..N path-length bound, so interpolate
	// the caller-supplied depth (already an int32, not attacker string input).
	// Every node of the path is live, the start node included, so a retired
	// node has no ancestors.
	//
	// effective_at is computed once per ancestor from cheap scalar properties
	// only (no version body is read here); ranking uses
	// "(effective_at IS NULL) ASC, effective_at DESC" so an ancestor with no
	// recorded change ever sorts last regardless of the engine's default
	// null-ordering convention.
	idsQuery := fmt.Sprintf(`
MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})
OPTIONAL MATCH path = (n)-[:DEPENDS_ON*1..%d]->(anc:Node {tenant_id: $tenant_id})
WHERE ALL(m IN nodes(path) WHERE coalesce(m.active, true))
WITH anc, min(length(path)) AS depth
WHERE anc IS NOT NULL
OPTIONAL MATCH (anc)-[curEdge:CURRENT]->(:NodeVersion {tenant_id: $tenant_id})
OPTIONAL MATCH (av:NodeVersion {tenant_id: $tenant_id, unique_id: anc.unique_id})
WITH anc, depth, curEdge.promoted_at AS cur_edge_at, max(av.promoted_at) AS newest_version_at
WITH anc, depth,
     CASE
       WHEN cur_edge_at IS NULL THEN newest_version_at
       WHEN newest_version_at IS NULL THEN cur_edge_at
       WHEN cur_edge_at > newest_version_at THEN cur_edge_at
       ELSE newest_version_at
     END AS effective_at
WHERE $since IS NULL OR (effective_at IS NOT NULL AND effective_at >= $since)
RETURN anc.unique_id AS unique_id, depth AS depth
ORDER BY (effective_at IS NULL) ASC, effective_at DESC
LIMIT $cap`, depth)
	versionsQuery := ancestorVersionsCypher + nodeVersionColumns(true)

	type ancestorMeta struct {
		uniqueID string
		depth    int32
	}
	type read struct {
		metas    []ancestorMeta
		versions map[string][]codeversion.VersionView
	}
	got, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (read, error) {
		var out read
		idsResult, err := run.Run(ctx, idsQuery, map[string]any{"uid": uniqueID, "since": sinceParam, "cap": int64(cap)})
		if err != nil {
			return out, err
		}
		for idsResult.Next(ctx) {
			rec := idsResult.Record()
			id, _ := recordString(rec, "unique_id")
			out.metas = append(out.metas, ancestorMeta{uniqueID: id, depth: clampToInt32(toInt64(recordValue(rec, "depth")))})
		}
		if err := idsResult.Err(); err != nil || len(out.metas) == 0 {
			return out, err
		}

		ids := make([]string, len(out.metas))
		for i, m := range out.metas {
			ids[i] = m.uniqueID
		}
		versResult, err := run.Run(ctx, versionsQuery, map[string]any{"ids": ids})
		if err != nil {
			return out, err
		}
		out.versions = make(map[string][]codeversion.VersionView, len(ids))
		for versResult.Next(ctx) {
			rec := versResult.Record()
			id, _ := recordString(rec, "unique_id")
			if !recordBool(rec, "has_version") {
				continue
			}
			out.versions[id] = append(out.versions[id], versionViewFromRecord(rec, id))
		}
		return out, versResult.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.Ancestors: %w", err)
	}

	// metas is already most-recently-changed first — ranked, since-filtered,
	// and capped in idsQuery before any version body was fetched — so out
	// preserves that order by construction rather than by re-sorting on a
	// version's own promoted_at, which (per this method's doc) is not the
	// right metric for a reverted ancestor.
	out := make([]codeversion.AncestorVersions, 0, len(got.metas))
	for _, m := range got.metas {
		out = append(out, codeversion.AncestorVersions{
			UniqueID: m.uniqueID,
			Depth:    m.depth,
			Versions: got.versions[m.uniqueID],
		})
	}
	return out, nil
}

// UnitVersions enumerates a shared-code unit's versions, ordered promoted_at
// DESC. An unknown unit_id — no :CodeUnit and no :CodeUnitVersion row of the
// tenant — returns ErrUnitNotFound; a known unit with no recorded history
// returns an empty slice.
func (r *CodeVersionQueryRepository) UnitVersions(ctx context.Context, tenantID, unitID string, limit int32) ([]codeversion.UnitVersionView, error) {
	type read struct {
		versions []codeversion.UnitVersionView
		known    bool
	}
	got, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (read, error) {
		byUnit, err := unitVersionsBatch(ctx, run, []string{unitID}, limit)
		if err != nil || len(byUnit[unitID]) > 0 {
			return read{versions: byUnit[unitID], known: true}, err
		}
		known, err := singleBool(ctx, run, unitKnownCypher, map[string]any{"unit_id": unitID}, "known")
		return read{versions: []codeversion.UnitVersionView{}, known: known}, err
	})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.UnitVersions: %w", err)
	}
	if !got.known {
		return nil, fmt.Errorf("CodeVersionQueryRepository.UnitVersions: unit %s: %w", unitID, domain.ErrUnitNotFound)
	}
	return got.versions, nil
}

// unitKnownCypher reports whether the tenant holds the shared-code unit: a
// :CodeUnit node, or at least one :CodeUnitVersion (the write path always
// merges both together, but this checks both for the same defensive reason
// nodeKnownCypher does).
const unitKnownCypher = `
OPTIONAL MATCH (cu:CodeUnit {tenant_id: $tenant_id, unit_id: $unit_id})
WITH count(cu) > 0 AS has_unit
OPTIONAL MATCH (v:CodeUnitVersion {tenant_id: $tenant_id, unit_id: $unit_id})
WITH has_unit, count(v) > 0 AS has_version
RETURN has_unit OR has_version AS known`

// UnitVersionsBatch is UnitVersions for many units in one round trip, each
// capped independently at limit. It makes no known/unknown distinction —
// every requested id that resolves to at least one version appears in the
// result map, and one that resolves to none is simply absent — which is
// exactly what the node-selector path in GetCodeUnitVersions needs, since
// those ids are already known (they came from the node's own USES_CODE
// edges). unitIDs is sorted before being sent to Cypher purely so the query
// plan and its cost are deterministic across calls; it does not affect
// output order, since UnitVersions above and GetCodeUnitVersions both index
// the returned map by unit id rather than relying on row order.
func (r *CodeVersionQueryRepository) UnitVersionsBatch(ctx context.Context, tenantID string, unitIDs []string, limit int32) (map[string][]codeversion.UnitVersionView, error) {
	byUnit, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (map[string][]codeversion.UnitVersionView, error) {
		return unitVersionsBatch(ctx, run, unitIDs, limit)
	})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.UnitVersionsBatch: %w", err)
	}
	return byUnit, nil
}

// unitVersionsCypher lists each unit's versions newest first, up to $limit,
// marking the one its :CodeUnit's :CURRENT points at.
const unitVersionsCypher = `
UNWIND $unit_ids AS unit_id
OPTIONAL MATCH (cu:CodeUnit {tenant_id: $tenant_id, unit_id: unit_id})-[:CURRENT]->(cur:CodeUnitVersion {tenant_id: $tenant_id})
WITH unit_id, cur
OPTIONAL MATCH (v:CodeUnitVersion {tenant_id: $tenant_id, unit_id: unit_id})
WITH unit_id, cur, v
ORDER BY v.promoted_at DESC
WITH unit_id, cur, collect(v)[0..$limit] AS top
UNWIND (CASE WHEN size(top) = 0 THEN [null] ELSE top END) AS v
RETURN unit_id AS unit_id, v IS NOT NULL AS has_version,
       v.checksum AS checksum, v.source AS source, v.repo AS repo,
       v.commit_sha AS commit_sha, v.release_id AS release_id,
       v.promoted_at AS promoted_at,
       (cur IS NOT NULL AND v IS NOT NULL AND cur.checksum = v.checksum) AS is_current`

// unitVersionsBatch runs unitVersionsCypher for unitIDs on run.
func unitVersionsBatch(ctx context.Context, run TenantRunner, unitIDs []string, limit int32) (map[string][]codeversion.UnitVersionView, error) {
	ids := slices.Clone(unitIDs)
	slices.Sort(ids)
	res, err := run.Run(ctx, unitVersionsCypher, map[string]any{"unit_ids": ids, "limit": int64(limit)})
	if err != nil {
		return nil, err
	}
	result := make(map[string][]codeversion.UnitVersionView, len(ids))
	for res.Next(ctx) {
		rec := res.Record()
		unitID, _ := recordString(rec, "unit_id")
		if !recordBool(rec, "has_version") {
			continue
		}
		checksum, _ := recordString(rec, "checksum")
		source, _ := recordString(rec, "source")
		repo, _ := recordString(rec, "repo")
		commitSHA, _ := recordString(rec, "commit_sha")
		releaseID, _ := recordString(rec, "release_id")
		result[unitID] = append(result[unitID], codeversion.UnitVersionView{
			UnitID:     unitID,
			Checksum:   checksum,
			Source:     source,
			Repo:       repo,
			CommitSHA:  commitSHA,
			ReleaseID:  releaseID,
			PromotedAt: recordTime(rec, "promoted_at"),
			IsCurrent:  recordBool(rec, "is_current"),
		})
	}
	return result, res.Err()
}

// unitsForNodeCypher lists the units the current version of the tenant's live
// :Node uses.
const unitsForNodeCypher = `
MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})-[:CURRENT]->(:NodeVersion {tenant_id: $tenant_id})
      -[:USES_CODE]->(cv:CodeUnitVersion {tenant_id: $tenant_id})
WHERE coalesce(n.active, true)
RETURN DISTINCT cv.unit_id AS unit_id`

// UnitsForNode returns the units the node's current version uses. A node
// with no current version, a retired node included, returns an empty slice,
// not an error.
func (r *CodeVersionQueryRepository) UnitsForNode(ctx context.Context, tenantID, uniqueID string) ([]string, error) {
	units, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) ([]string, error) {
		result, err := run.Run(ctx, unitsForNodeCypher, map[string]any{"uid": uniqueID})
		if err != nil {
			return nil, err
		}
		units := make([]string, 0)
		for result.Next(ctx) {
			id, _ := recordString(result.Record(), "unit_id")
			units = append(units, id)
		}
		return units, result.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.UnitsForNode: %w", err)
	}
	return units, nil
}

// versionViewFromRecord builds a VersionView from one row projected by
// nodeVersionColumns.
func versionViewFromRecord(rec *neo4j.Record, uniqueID string) codeversion.VersionView {
	contentHash, _ := recordString(rec, "content_hash")
	sourceHash, _ := recordString(rec, "source_hash")
	sharedCodeHash, _ := recordString(rec, "shared_code_hash")
	configHash, _ := recordString(rec, "config_hash")
	runtime, _ := recordString(rec, "runtime")
	rawCode, _ := recordString(rec, "raw_code")
	compiledCode, _ := recordString(rec, "compiled_code")
	configJSON, _ := recordString(rec, "config_json")
	repo, _ := recordString(rec, "repo")
	commitSHA, _ := recordString(rec, "commit_sha")
	releaseID, _ := recordString(rec, "release_id")

	return codeversion.VersionView{
		UniqueID:          uniqueID,
		VersionSeq:        toInt64(recordValue(rec, "version_seq")),
		ContentHash:       contentHash,
		SourceHash:        sourceHash,
		SharedCodeHash:    sharedCodeHash,
		ConfigHash:        configHash,
		Runtime:           runtime,
		RawCode:           rawCode,
		CompiledCode:      compiledCode,
		CompiledTruncated: recordBool(rec, "compiled_truncated"),
		ConfigJSON:        configJSON,
		Repo:              repo,
		CommitSHA:         commitSHA,
		ReleaseID:         releaseID,
		PromotedAt:        recordTime(rec, "promoted_at"),
		Healed:            recordBool(rec, "healed"),
		Backfilled:        recordBool(rec, "backfilled"),
		IsCurrent:         recordBool(rec, "is_current"),
	}
}

// clampToInt32 converts a path-length value read back from Neo4j to int32,
// clamping rather than wrapping if it ever exceeded the int32 range. Actual
// path lengths never approach that range; the clamp is a safety net, not an
// expected code path.
func clampToInt32(v int64) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	if v < math.MinInt32 {
		return math.MinInt32
	}
	return int32(v)
}

// recordBool reads a boolean column, treating null or a type mismatch as false.
func recordBool(rec *neo4j.Record, key string) bool {
	v, _ := recordValue(rec, key).(bool)
	return v
}

// recordTime reads a temporal column, treating null or a type mismatch as the
// zero time. Neo4j's Go driver decodes a stored `datetime()` as time.Time.
func recordTime(rec *neo4j.Record, key string) time.Time {
	v, _ := recordValue(rec, key).(time.Time)
	return v
}
