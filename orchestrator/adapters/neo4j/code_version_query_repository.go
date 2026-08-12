// File: orchestrator/adapters/neo4j/code_version_query_repository.go
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
	"sort"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// CodeVersionQueryRepository implements queries.CodeVersionReader against
// Neo4j.
//
// Every chain walk orders by promoted_at, never version_seq, and never
// follows :PREVIOUS. All three encode order, but only promoted_at is
// trustworthy:
//
//   - promoted_at is when the release that introduced this code was
//     promoted — chronologically true however late the event was ingested.
//   - version_seq is assigned max+1 at ingestion time, so a late-arriving
//     OLDER release receives the HIGHEST seq. Sorting by it would put stale
//     code at the top of a "newest first" view; a graph-ahead write (an
//     unattached, backfilled history entry) creates exactly this case.
//   - :PREVIOUS is written only for a version the writing release actually
//     creates, which is what keeps the chain acyclic across a revert. A late
//     older release therefore has no :PREVIOUS link at all, and a revert
//     re-points :CURRENT at an existing version without touching its chain.
//     A pure :PREVIOUS walk both omits versions and cannot order them.
//
// A node's chain walk starts from its :Table's :CURRENT pointer only to
// compute is_current; the version rows themselves are matched by unique_id
// directly (node_version_uid backs that lookup), so a retired node — its
// :Table deleted, its versions still present — still returns its full
// history, just with is_current false on every row. promoted_at sorting
// happens over a per-node result set that is small by construction: versions
// grow with edits, not with releases.
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
// binds `v` to the version and `cur` to the node's :CURRENT version (or null
// when there is none), and is shared by every query that reads this shape so
// the column list cannot drift between them.
const nodeVersionColumns = `v.version_seq AS version_seq, v.content_hash AS content_hash,
       v.source_hash AS source_hash, v.shared_code_hash AS shared_code_hash,
       v.config_hash AS config_hash, v.runtime AS runtime,
       v.raw_code AS raw_code, v.compiled_code AS compiled_code,
       coalesce(v.compiled_truncated, false) AS compiled_truncated,
       coalesce(v.config_json, '{}') AS config_json,
       v.repo AS repo, v.commit_sha AS commit_sha, v.release_id AS release_id,
       v.promoted_at AS promoted_at, coalesce(v.healed, false) AS healed,
       coalesce(v.backfilled, false) AS backfilled,
       (cur IS NOT NULL AND cur.content_hash = v.content_hash) AS is_current`

// NodeVersions walks the chain from :CURRENT, newest first, up to limit.
func (r *CodeVersionQueryRepository) NodeVersions(ctx context.Context, uniqueID string, limit int32) ([]codeversion.VersionView, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		OPTIONAL MATCH (t:Table {unique_id: $uid})-[:CURRENT]->(cur:NodeVersion)
		WITH cur
		MATCH (v:NodeVersion {unique_id: $uid})
		RETURN ` + nodeVersionColumns + `
		ORDER BY v.promoted_at DESC
		LIMIT $limit
	`
	result, err := session.Run(ctx, query, map[string]any{"uid": uniqueID, "limit": int64(limit)})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.NodeVersions: %w", err)
	}
	versions := make([]codeversion.VersionView, 0)
	for result.Next(ctx) {
		versions = append(versions, versionViewFromRecord(result.Record(), uniqueID))
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.NodeVersions: %w", err)
	}
	if len(versions) > 0 {
		return versions, nil
	}

	// No :NodeVersion carries this unique_id. That is a known node with an
	// empty history when its :Table still exists, and an unknown node
	// otherwise.
	known, err := r.tableExists(ctx, session, uniqueID)
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.NodeVersions: %w", err)
	}
	if !known {
		return nil, fmt.Errorf("CodeVersionQueryRepository.NodeVersions: node %s: %w", uniqueID, domain.ErrNodeNotFound)
	}
	return versions, nil
}

// tableExists reports whether a :Table node exists for uniqueID, regardless
// of its active flag: a node mid-retirement is still a known node, just one
// whose chain walk above already returned rows if it had any.
func (r *CodeVersionQueryRepository) tableExists(ctx context.Context, session neo4j.SessionWithContext, uniqueID string) (bool, error) {
	result, err := session.Run(ctx, `MATCH (t:Table {unique_id: $uid}) RETURN count(t) > 0 AS known`,
		map[string]any{"uid": uniqueID})
	if err != nil {
		return false, err
	}
	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return false, err
		}
		return false, nil
	}
	return recordBool(result.Record(), "known"), result.Err()
}

// VersionsBySeq returns the two named versions of one node. A missing seq —
// whether the node has no such version or is unknown entirely — reports
// ErrNodeNotFound naming the seq that could not be found.
func (r *CodeVersionQueryRepository) VersionsBySeq(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (codeversion.VersionView, codeversion.VersionView, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		OPTIONAL MATCH (t:Table {unique_id: $uid})-[:CURRENT]->(cur:NodeVersion)
		WITH cur
		MATCH (v:NodeVersion {unique_id: $uid})
		WHERE v.version_seq IN [$from_seq, $to_seq]
		RETURN ` + nodeVersionColumns
	result, err := session.Run(ctx, query, map[string]any{"uid": uniqueID, "from_seq": fromSeq, "to_seq": toSeq})
	if err != nil {
		return codeversion.VersionView{}, codeversion.VersionView{}, fmt.Errorf("CodeVersionQueryRepository.VersionsBySeq: %w", err)
	}
	bySeq := make(map[int64]codeversion.VersionView, 2)
	for result.Next(ctx) {
		v := versionViewFromRecord(result.Record(), uniqueID)
		bySeq[v.VersionSeq] = v
	}
	if err := result.Err(); err != nil {
		return codeversion.VersionView{}, codeversion.VersionView{}, fmt.Errorf("CodeVersionQueryRepository.VersionsBySeq: %w", err)
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

// Ancestors returns the node's transitive upstreams up to depth, each with
// its two most recent versions, most-recently-changed first. Only the active
// topology is walked, deduplicated to the shortest depth at which an
// ancestor is reachable; a non-zero since excludes ancestors whose newest
// version predates it. depth < 1 walks nothing.
func (r *CodeVersionQueryRepository) Ancestors(ctx context.Context, uniqueID string, depth int32, since time.Time) ([]codeversion.AncestorVersions, error) {
	if depth < 1 {
		return []codeversion.AncestorVersions{}, nil
	}
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	// Cypher cannot parameterize the *1..N path-length bound, so interpolate
	// the caller-supplied depth (already an int32, not attacker string input).
	idsQuery := fmt.Sprintf(`
		MATCH (n:Table {unique_id: $uid})
		OPTIONAL MATCH path = (n)-[:DEPENDS_ON*1..%d]->(anc:Table)
		WHERE ALL(m IN nodes(path) WHERE COALESCE(m.active, true))
		WITH anc, min(length(path)) AS depth
		WHERE anc IS NOT NULL
		RETURN anc.unique_id AS unique_id, depth AS depth
	`, depth)
	idsResult, err := session.Run(ctx, idsQuery, map[string]any{"uid": uniqueID})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.Ancestors: %w", err)
	}
	type ancestorMeta struct {
		uniqueID string
		depth    int32
	}
	var metas []ancestorMeta
	for idsResult.Next(ctx) {
		rec := idsResult.Record()
		id, _ := recordString(rec, "unique_id")
		metas = append(metas, ancestorMeta{uniqueID: id, depth: clampToInt32(toInt64(recordValue(rec, "depth")))})
	}
	if err := idsResult.Err(); err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.Ancestors: %w", err)
	}
	if len(metas) == 0 {
		return []codeversion.AncestorVersions{}, nil
	}

	ids := make([]string, len(metas))
	for i, m := range metas {
		ids[i] = m.uniqueID
	}

	// One batched query fetches every ancestor's two newest versions. Rows
	// are grouped by unique_id in Go rather than relying on the engine to
	// preserve inter-group order; the intra-group order (newest first) comes
	// from sorting before the collect that builds each ancestor's top-2.
	versionsQuery := `
		UNWIND $ids AS uid
		OPTIONAL MATCH (t:Table {unique_id: uid})-[:CURRENT]->(cur:NodeVersion)
		WITH uid, cur
		OPTIONAL MATCH (v:NodeVersion {unique_id: uid})
		WITH uid, cur, v
		ORDER BY v.promoted_at DESC
		WITH uid, cur, collect(v)[0..2] AS top
		UNWIND (CASE WHEN size(top) = 0 THEN [null] ELSE top END) AS v
		RETURN uid AS unique_id, v IS NOT NULL AS has_version, ` + nodeVersionColumns
	versResult, err := session.Run(ctx, versionsQuery, map[string]any{"ids": ids})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.Ancestors: %w", err)
	}
	versByID := make(map[string][]codeversion.VersionView, len(ids))
	for versResult.Next(ctx) {
		rec := versResult.Record()
		id, _ := recordString(rec, "unique_id")
		if !recordBool(rec, "has_version") {
			continue
		}
		versByID[id] = append(versByID[id], versionViewFromRecord(rec, id))
	}
	if err := versResult.Err(); err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.Ancestors: %w", err)
	}

	out := make([]codeversion.AncestorVersions, 0, len(metas))
	for _, m := range metas {
		versions := versByID[m.uniqueID]
		if !since.IsZero() {
			if len(versions) == 0 || versions[0].PromotedAt.Before(since) {
				continue
			}
		}
		out = append(out, codeversion.AncestorVersions{
			UniqueID: m.uniqueID,
			Depth:    m.depth,
			Versions: versions,
		})
	}

	// Most-recently-changed first; an ancestor with no recorded version sorts
	// last, mirroring the unknown-provenance-last convention used for node
	// ancestry elsewhere in this repository.
	sort.SliceStable(out, func(i, j int) bool {
		hi, hj := len(out[i].Versions) > 0, len(out[j].Versions) > 0
		if !hi && !hj {
			return false
		}
		if !hi {
			return false
		}
		if !hj {
			return true
		}
		return out[i].Versions[0].PromotedAt.After(out[j].Versions[0].PromotedAt)
	})
	return out, nil
}

// UnitVersions walks a shared-code unit's chain, newest first.
func (r *CodeVersionQueryRepository) UnitVersions(ctx context.Context, unitID string, limit int32) ([]codeversion.UnitVersionView, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		OPTIONAL MATCH (cu:CodeUnit {unit_id: $unit_id})-[:CURRENT]->(cur:CodeUnitVersion)
		WITH cur
		MATCH (v:CodeUnitVersion {unit_id: $unit_id})
		RETURN v.checksum AS checksum, v.source AS source, v.repo AS repo,
		       v.commit_sha AS commit_sha, v.release_id AS release_id,
		       v.promoted_at AS promoted_at,
		       (cur IS NOT NULL AND cur.checksum = v.checksum) AS is_current
		ORDER BY v.promoted_at DESC
		LIMIT $limit
	`
	result, err := session.Run(ctx, query, map[string]any{"unit_id": unitID, "limit": int64(limit)})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.UnitVersions: %w", err)
	}
	versions := make([]codeversion.UnitVersionView, 0)
	for result.Next(ctx) {
		rec := result.Record()
		checksum, _ := recordString(rec, "checksum")
		source, _ := recordString(rec, "source")
		repo, _ := recordString(rec, "repo")
		commitSHA, _ := recordString(rec, "commit_sha")
		releaseID, _ := recordString(rec, "release_id")
		versions = append(versions, codeversion.UnitVersionView{
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
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.UnitVersions: %w", err)
	}
	return versions, nil
}

// UnitsForNode returns the units the node's current version uses. A node
// with no current version returns an empty slice, not an error.
func (r *CodeVersionQueryRepository) UnitsForNode(ctx context.Context, uniqueID string) ([]string, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		MATCH (t:Table {unique_id: $uid})-[:CURRENT]->(:NodeVersion)-[:USES_CODE]->(cv:CodeUnitVersion)
		RETURN DISTINCT cv.unit_id AS unit_id
	`
	result, err := session.Run(ctx, query, map[string]any{"uid": uniqueID})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.UnitsForNode: %w", err)
	}
	units := make([]string, 0)
	for result.Next(ctx) {
		id, _ := recordString(result.Record(), "unit_id")
		units = append(units, id)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.UnitsForNode: %w", err)
	}
	return units, nil
}

// RunExecutions returns runs that executed the node, newest first. Status and
// content_hash come from the :EXECUTES edge (the code that specific run
// executed); the rest comes from the :Run node.
func (r *CodeVersionQueryRepository) RunExecutions(ctx context.Context, uniqueID string, limit int32) ([]codeversion.RunExecution, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		MATCH (run:Run)-[e:EXECUTES]->(t:Table {unique_id: $uid})
		RETURN run.run_id AS run_id,
		       e.task_id AS task_id,
		       coalesce(e.status, 'PENDING') AS status,
		       run.schedule_name AS schedule_name,
		       coalesce(run.operation, '') AS operation,
		       coalesce(e.image_tag, '') AS image_tag,
		       coalesce(e.content_hash, '') AS content_hash,
		       run.created_at AS created_at,
		       run.completed_at AS completed_at
		ORDER BY run.created_at DESC
		LIMIT $limit
	`
	result, err := session.Run(ctx, query, map[string]any{"uid": uniqueID, "limit": int64(limit)})
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.RunExecutions: %w", err)
	}
	runs := make([]codeversion.RunExecution, 0)
	for result.Next(ctx) {
		rec := result.Record()
		runID, _ := recordString(rec, "run_id")
		taskID, _ := recordString(rec, "task_id")
		status, _ := recordString(rec, "status")
		scheduleName, _ := recordString(rec, "schedule_name")
		operation, _ := recordString(rec, "operation")
		imageTag, _ := recordString(rec, "image_tag")
		contentHash, _ := recordString(rec, "content_hash")
		runs = append(runs, codeversion.RunExecution{
			RunID:        runID,
			TaskID:       taskID,
			Status:       status,
			ScheduleName: scheduleName,
			Operation:    operation,
			ImageTag:     imageTag,
			ContentHash:  contentHash,
			CreatedAt:    recordTime(rec, "created_at"),
			CompletedAt:  recordTime(rec, "completed_at"),
		})
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("CodeVersionQueryRepository.RunExecutions: %w", err)
	}
	return runs, nil
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
