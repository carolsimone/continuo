package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// skippedSampleSize bounds how many node ids a skip warning names, so a
// release-wide mismatch logs a usable sample instead of thousands of ids.
const skippedSampleSize = 5

// createMissingNodesCypher creates every node of the batch the tenant's graph
// does not hold yet, inactive, with the properties its artifact entry gives
// it and the release's id; it has no retired_at, because it was never live.
// A node the graph holds is left as it is: the topology swap owns a node's
// properties and makes it live when it applies a release that carries it.
const createMissingNodesCypher = `
UNWIND $topology AS t
MERGE (n:Node {tenant_id: $tenant_id, unique_id: t.unique_id})
ON CREATE SET n.schema_name        = t.schema_name,
              n.table_name         = t.table_name,
              n.service_name       = t.service_name,
              n.node_type          = t.node_type,
              n.content_hash       = t.content_hash,
              n.test_count         = t.test_count,
              n.image_tag          = t.image_tag,
              n.secret_ref         = CASE WHEN t.secret_ref = '' THEN null ELSE t.secret_ref END,
              n.schedule_name      = t.schedule,
              n.original_file_path = t.original_file_path,
              n.release_id         = $release_id,
              n.active             = false`

// readNodeStatesCypher returns, per node of the batch, the hash of its current
// version and the highest version_seq of its versions. Only unique_id is a
// grouping key, so every other column stays inside an aggregate:
// head(collect(...)) collapses the at-most-one pointer row while max folds the
// version rows. A node with no versions yields a zero maximum.
const readNodeStatesCypher = `
UNWIND $unique_ids AS uid
MATCH (n:Node {tenant_id: $tenant_id, unique_id: uid})
OPTIONAL MATCH (n)-[:CURRENT]->(cv:NodeVersion {tenant_id: $tenant_id})
OPTIONAL MATCH (av:NodeVersion {tenant_id: $tenant_id, unique_id: uid})
RETURN uid                                     AS unique_id,
       head(collect(DISTINCT cv.content_hash)) AS current_hash,
       coalesce(max(av.version_seq), 0)        AS max_seq`

// advanceNodeWatermarkCypher sets each node's watermark to the larger of its
// value and the promotion's seq. Writing to the node takes its write lock, so
// every later guard of the transaction reads a value no concurrent writer can
// change underneath it.
const advanceNodeWatermarkCypher = `
UNWIND $unique_ids AS uid
MATCH (n:Node {tenant_id: $tenant_id, unique_id: uid})
SET n.code_version_promotion_seq = CASE
      WHEN n.code_version_promotion_seq IS NULL OR n.code_version_promotion_seq < $promotion_seq
      THEN $promotion_seq
      ELSE n.code_version_promotion_seq END`

// readUnitStatesCypher returns the checksum of each shared-code unit's current
// version; a unit never recorded yields none.
const readUnitStatesCypher = `
UNWIND $unit_ids AS uid
OPTIONAL MATCH (:CodeUnit {tenant_id: $tenant_id, unit_id: uid})-[:CURRENT]->(cv:CodeUnitVersion {tenant_id: $tenant_id})
RETURN uid AS unit_id, head(collect(DISTINCT cv.checksum)) AS current_checksum`

// advanceUnitWatermarkCypher MERGEs each unit and sets its watermark as
// advanceNodeWatermarkCypher does a node's. It is a statement of its own so
// the next one's created-node count is exactly the unit versions written.
const advanceUnitWatermarkCypher = `
UNWIND $units AS u
MERGE (cu:CodeUnit {tenant_id: $tenant_id, unit_id: u.unit_id})
SET cu.code_version_promotion_seq = CASE
      WHEN cu.code_version_promotion_seq IS NULL OR cu.code_version_promotion_seq < $promotion_seq
      THEN $promotion_seq
      ELSE cu.code_version_promotion_seq END`

// writeUnitVersionsCypher records each unit version once: every property is
// set on creation only.
const writeUnitVersionsCypher = `
UNWIND $units AS u
MERGE (v:CodeUnitVersion {tenant_id: $tenant_id, unit_id: u.unit_id, checksum: u.checksum})
ON CREATE SET v.source      = u.source,
              v.repo        = $repo,
              v.commit_sha  = $commit_sha,
              v.release_id  = $release_id,
              v.promoted_at = $promoted_at`

// moveUnitCurrentCypher points each unit at its new version while the unit's
// watermark is this promotion's seq.
const moveUnitCurrentCypher = `
UNWIND $unit_currents AS c
MATCH (cu:CodeUnit {tenant_id: $tenant_id, unit_id: c.unit_id})
WHERE cu.code_version_promotion_seq = $promotion_seq
MATCH (v:CodeUnitVersion {tenant_id: $tenant_id, unit_id: c.unit_id, checksum: c.checksum})
OPTIONAL MATCH (cu)-[old:CURRENT]->(:CodeUnitVersion {tenant_id: $tenant_id})
DELETE old
CREATE (cu)-[:CURRENT {promoted_at: $promoted_at, release_id: $release_id}]->(v)`

// writeNodeVersionsCypher records each node version once: every property is
// set on creation only, so a revert MERGEs onto the version that exists.
const writeNodeVersionsCypher = `
UNWIND $nodes AS n
MERGE (v:NodeVersion {tenant_id: $tenant_id, unique_id: n.unique_id, content_hash: n.content_hash})
ON CREATE SET v.source_hash        = n.source_hash,
              v.shared_code_hash   = n.shared_code_hash,
              v.config_hash        = n.config_hash,
              v.runtime            = n.runtime,
              v.raw_code           = n.raw_code,
              v.compiled_code      = n.compiled_code,
              v.compiled_truncated = n.compiled_truncated,
              v.config_json        = n.config_json,
              v.repo               = $repo,
              v.commit_sha         = $commit_sha,
              v.release_id         = $release_id,
              v.promoted_at        = $promoted_at,
              v.version_seq        = n.version_seq,
              v.healed             = n.healed,
              v.backfilled         = false`

// linkUsesCodeCypher draws each node version's edge to the shared-code
// versions it used.
const linkUsesCodeCypher = `
UNWIND $uses AS u
MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: u.unique_id, content_hash: u.content_hash})
MATCH (cv:CodeUnitVersion {tenant_id: $tenant_id, unit_id: u.unit_id, checksum: u.checksum})
MERGE (v)-[:USES_CODE]->(cv)`

// moveNodeCurrentCypher points each node at its new version while the node's
// watermark is this promotion's seq. The comparison and the replacement are
// one statement, behind the watermark's write lock, so an older promotion can
// never delete a newer pointer and put a stale one in its place.
const moveNodeCurrentCypher = `
UNWIND $currents AS c
MATCH (n:Node {tenant_id: $tenant_id, unique_id: c.unique_id})
WHERE n.code_version_promotion_seq = $promotion_seq
MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: c.unique_id, content_hash: c.content_hash})
OPTIONAL MATCH (n)-[old:CURRENT]->(:NodeVersion {tenant_id: $tenant_id})
DELETE old
CREATE (n)-[:CURRENT {promoted_at: $promoted_at, release_id: $release_id}]->(v)`

// linkResolvedRejectionsCypher links every still-open rejection of a node,
// recorded before this promotion, to the version that just became current:
// the fix for a recorded failure is the first version promoted after it.
// "Still open" looks only at [:RESOLVED_BY] edges to a :NodeVersion, because a
// merged fix PR draws its own [:RESOLVED_BY] to a :Proposal independently of
// any version write. The link is guarded on the same watermark as the pointer,
// so a stale redelivery resolves nothing. The edge is stamped on creation with
// this promotion's time and release, which the precedent detail diffs from,
// since a reverted-to version keeps its own earlier promoted_at; the MERGE
// pattern itself carries no property, so it matches on the edge type alone.
const linkResolvedRejectionsCypher = `
UNWIND $currents AS c
MATCH (n:Node {tenant_id: $tenant_id, unique_id: c.unique_id})
WHERE n.code_version_promotion_seq = $promotion_seq
MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: c.unique_id, content_hash: c.content_hash})
MATCH (rej:Rejection {tenant_id: $tenant_id, node_id: c.unique_id})
WHERE rej.at < $promoted_at AND NOT (rej)-[:RESOLVED_BY]->(:NodeVersion {tenant_id: $tenant_id})
MERGE (rej)-[rb:RESOLVED_BY]->(v)
ON CREATE SET rb.promoted_at = $promoted_at, rb.release_id = $release_id`

// codeVersionRepository writes one tenant's code versions on the open
// transaction of a unit-of-work Write, one batch per transaction.
//
// Its write predicate is graph-authoritative: a version is written when the
// incoming content_hash differs from the one on the node's current version.
// The promoted event's changed flags are not consulted: they cannot heal a
// write the graph missed, whereas comparing against the graph makes every
// later release converge it.
//
// Two invariants hold the model together:
//
//   - A version node is immutable. Every property is set under ON CREATE, and
//     a node that reverts to earlier code MERGEs onto the version that already
//     exists, so the graph holds one version per distinct (unique_id,
//     content_hash) ever observed. version_seq is a stable per-node handle
//     assigned max+1 at ingestion; it is not an ordering, since a late older
//     promotion still receives the highest value.
//   - Ordering is decided by the watermark code_version_promotion_seq, on the
//     :Node and on the :CodeUnit for a shared-code unit, never by a read in
//     Go. Every matched node, and every unit it uses, advances it to the
//     larger seq, one whose code is unchanged included, and every pointer
//     write is guarded on it inside the same statement, so an older promotion
//     records its history but never takes a pointer from a newer one.
type codeVersionRepository struct {
	run    TenantRunner
	logger *slog.Logger
}

var _ repository.CodeVersionRepository = (*codeVersionRepository)(nil)

// nodeState is the graph's current knowledge about one node of the batch.
type nodeState struct {
	currentHash string
	maxSeq      int64
}

// WriteBatch writes one batch of a release's versions for tenantID on the open
// transaction:
//
//  1. Skip each bundle node with no artifact entry, or whose entry is a
//     dbt-test, and log the ones skipped.
//  2. Create, inactive, every node of the batch the graph does not hold yet.
//  3. Read each node's current version and advance its watermark.
//  4. Record the shared code the nodes use, advance each unit's watermark and
//     move a changed unit's :CURRENT where its watermark is this promotion's
//     seq.
//  5. For every node whose code differs from its current version: record its
//     version and its :USES_CODE edges, then move its :CURRENT and resolve its
//     open rejections where the watermark is this promotion's seq.
//
// It refuses a tenantID other than the transaction's.
func (r *codeVersionRepository) WriteBatch(
	ctx context.Context,
	tenantID string,
	in codeversion.WriteInput,
) (codeversion.WriteResult, error) {
	var out codeversion.WriteResult
	if err := r.run.requireTenant(tenantID); err != nil {
		return out, err
	}

	nodes, entries := r.writableNodes(tenantID, in)
	if len(nodes) == 0 {
		return out, nil
	}
	uniqueIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		uniqueIDs = append(uniqueIDs, n.UniqueID)
	}
	// The Neo4j Go driver names a time.Time's zone by its Location().String(),
	// and Neo4j rejects time.Local's "Local", so every time is sent in UTC.
	promotedAt := in.PromotedAt.UTC()

	topo, _ := topologyParams(entries)
	created, err := runCounting(ctx, r.run, createMissingNodesCypher, map[string]any{
		"topology":   topo,
		"release_id": in.ReleaseID,
	}, "create the nodes the graph does not hold")
	if err != nil {
		return out, err
	}
	out.NodesCreated = created.NodesCreated()

	states, err := r.readNodeStates(ctx, uniqueIDs)
	if err != nil {
		return out, err
	}
	if _, err := runCounting(ctx, r.run, advanceNodeWatermarkCypher, map[string]any{
		"unique_ids":    uniqueIDs,
		"promotion_seq": in.PromotionSeq,
	}, "advance the :Node version watermark"); err != nil {
		return out, err
	}

	var (
		versions  []map[string]any
		currents  []map[string]any
		uses      []map[string]any
		unitIDSet = map[string]struct{}{}
	)
	for _, n := range nodes {
		// A unit an unchanged node uses advances its watermark too, as the node
		// does, so a late older promotion cannot take the unit's pointer.
		for _, ref := range n.UnitRefs {
			unitIDSet[ref.UnitID] = struct{}{}
		}
		st := states[n.UniqueID]
		if st.currentHash == n.ContentHash {
			continue // the graph already records this code
		}
		versions = append(versions, versionParams(n, st.maxSeq+1))
		currents = append(currents, map[string]any{"unique_id": n.UniqueID, "content_hash": n.ContentHash})
		for _, ref := range n.UnitRefs {
			uses = append(uses, map[string]any{
				"unique_id":    n.UniqueID,
				"content_hash": n.ContentHash,
				"unit_id":      ref.UnitID,
				"checksum":     ref.Checksum,
			})
		}
	}

	common := map[string]any{
		"repo":          in.Repo,
		"commit_sha":    in.CommitSHA,
		"release_id":    in.ReleaseID,
		"promoted_at":   promotedAt,
		"promotion_seq": in.PromotionSeq,
	}

	if out.UnitVersionsCreated, err = r.writeUnits(ctx, in.Units, unitIDSet, common); err != nil {
		return out, err
	}
	if len(versions) == 0 {
		return out, nil
	}

	written, err := runCounting(ctx, r.run, writeNodeVersionsCypher, with(common, "nodes", versions), "write :NodeVersion nodes")
	if err != nil {
		return out, err
	}
	out.NodeVersionsCreated = written.NodesCreated()

	if len(uses) > 0 {
		if _, err := runCounting(ctx, r.run, linkUsesCodeCypher, map[string]any{"uses": uses}, "link :USES_CODE edges"); err != nil {
			return out, err
		}
	}

	// Count what the database did: a node whose watermark is past this
	// promotion is skipped by the guard.
	moved, err := runCounting(ctx, r.run, moveNodeCurrentCypher, with(common, "currents", currents), "move :CURRENT pointers")
	if err != nil {
		return out, err
	}
	out.CurrentPointersMoved = moved.RelationshipsCreated()

	linked, err := runCounting(ctx, r.run, linkResolvedRejectionsCypher, with(common, "currents", currents), "link resolved rejections")
	if err != nil {
		return out, err
	}
	out.RejectionsResolved = linked.RelationshipsCreated()
	return out, nil
}

// writableNodes returns the batch's nodes that have an artifact entry other
// than a dbt-test, with those entries. A dbt-test is never a node of the
// graph, and a node the artifact does not carry is not one of the release's;
// both are skipped and logged.
func (r *codeVersionRepository) writableNodes(
	tenantID string,
	in codeversion.WriteInput,
) ([]codeversion.NodeVersion, []topology.ReleasePromotedTopologyNode) {
	nodes := make([]codeversion.NodeVersion, 0, len(in.Nodes))
	entries := make([]topology.ReleasePromotedTopologyNode, 0, len(in.Nodes))
	var skipped []string
	for _, n := range in.Nodes {
		entry, ok := in.Topology[n.UniqueID]
		if !ok || entry.NodeType == string(pkgModel.NodeTypeDbtTest) {
			skipped = append(skipped, n.UniqueID)
			continue
		}
		nodes = append(nodes, n)
		entries = append(entries, entry)
	}
	if len(skipped) > 0 {
		sample := skipped
		if len(sample) > skippedSampleSize {
			sample = sample[:skippedSampleSize]
		}
		r.logger.Warn("code bundle names nodes the release's topology does not write; their versions are skipped",
			"tenant_id", tenantID,
			"release_id", in.ReleaseID,
			"promotion_seq", in.PromotionSeq,
			"skipped_count", len(skipped),
			"sample", sample)
	}
	return nodes, entries
}

// writeUnits records the shared-code versions the batch's nodes use, advances
// each unit's watermark and moves the pointer of a unit whose current version
// differs, before the node versions are written, so every :USES_CODE edge
// finds its target. A unit the input does not carry is left out. It returns
// how many unit versions it created.
func (r *codeVersionRepository) writeUnits(
	ctx context.Context,
	units []codeversion.CodeUnitVersion,
	unitIDSet map[string]struct{},
	common map[string]any,
) (int, error) {
	if len(unitIDSet) == 0 {
		return 0, nil
	}
	var used []codeversion.CodeUnitVersion
	unitIDs := make([]string, 0, len(unitIDSet))
	for _, u := range units {
		if _, ok := unitIDSet[u.UnitID]; ok {
			used = append(used, u)
			unitIDs = append(unitIDs, u.UnitID)
		}
	}
	if len(used) == 0 {
		return 0, nil
	}
	current, err := r.readUnitStates(ctx, unitIDs)
	if err != nil {
		return 0, err
	}

	params := make([]map[string]any, 0, len(used))
	var unitCurrents []map[string]any
	for _, u := range used {
		params = append(params, map[string]any{"unit_id": u.UnitID, "checksum": u.Checksum, "source": u.Source})
		if current[u.UnitID] != u.Checksum {
			unitCurrents = append(unitCurrents, map[string]any{"unit_id": u.UnitID, "checksum": u.Checksum})
		}
	}

	if _, err := runCounting(ctx, r.run, advanceUnitWatermarkCypher, with(common, "units", params), "write :CodeUnit nodes"); err != nil {
		return 0, err
	}
	written, err := runCounting(ctx, r.run, writeUnitVersionsCypher, with(common, "units", params), "write :CodeUnitVersion nodes")
	if err != nil {
		return 0, err
	}
	if len(unitCurrents) > 0 {
		if _, err := runCounting(ctx, r.run, moveUnitCurrentCypher, with(common, "unit_currents", unitCurrents), "move :CodeUnit pointers"); err != nil {
			return 0, err
		}
	}
	return written.NodesCreated(), nil
}

// readNodeStates returns the state of each node of uniqueIDs the graph holds.
func (r *codeVersionRepository) readNodeStates(ctx context.Context, uniqueIDs []string) (map[string]nodeState, error) {
	res, err := r.run.Run(ctx, readNodeStatesCypher, map[string]any{"unique_ids": uniqueIDs})
	if err != nil {
		return nil, fmt.Errorf("read node version state: %w", err)
	}
	states := make(map[string]nodeState, len(uniqueIDs))
	for res.Next(ctx) {
		rec := res.Record()
		uid, _ := recordString(rec, "unique_id")
		hash, _ := recordString(rec, "current_hash")
		var maxSeq int64
		if v, ok := rec.Get("max_seq"); ok {
			maxSeq, _ = v.(int64)
		}
		states[uid] = nodeState{currentHash: hash, maxSeq: maxSeq}
	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("iterate node version state: %w", err)
	}
	return states, nil
}

// readUnitStates returns the checksum of each unit's current version, empty
// for a unit never recorded.
func (r *codeVersionRepository) readUnitStates(ctx context.Context, unitIDs []string) (map[string]string, error) {
	res, err := r.run.Run(ctx, readUnitStatesCypher, map[string]any{"unit_ids": unitIDs})
	if err != nil {
		return nil, fmt.Errorf("read shared-code version state: %w", err)
	}
	current := make(map[string]string, len(unitIDs))
	for res.Next(ctx) {
		rec := res.Record()
		id, _ := recordString(rec, "unit_id")
		checksum, _ := recordString(rec, "current_checksum")
		current[id] = checksum
	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("iterate shared-code version state: %w", err)
	}
	return current, nil
}

// versionParams renders one version's write parameters.
func versionParams(n codeversion.NodeVersion, seq int64) map[string]any {
	return map[string]any{
		"unique_id":          n.UniqueID,
		"content_hash":       n.ContentHash,
		"source_hash":        n.SourceHash,
		"shared_code_hash":   n.SharedCodeHash,
		"config_hash":        n.ConfigHash,
		"runtime":            n.Runtime,
		"raw_code":           n.RawCode,
		"compiled_code":      n.CompiledCode,
		"compiled_truncated": n.CompiledTruncated,
		"config_json":        n.ConfigJSON,
		"version_seq":        seq,
		"healed":             n.Healed,
	}
}

// with returns a copy of params with key set to value.
func with(params map[string]any, key string, value any) map[string]any {
	out := make(map[string]any, len(params)+1)
	for k, v := range params {
		out[k] = v
	}
	out[key] = value
	return out
}

// runCounting runs a write statement and returns its counters. Bolt surfaces
// a failure either from Run or only when the summary is pulled, so every
// statement is consumed and checked.
func runCounting(ctx context.Context, run TenantRunner, cypher string, params map[string]any, what string) (neo4j.Counters, error) {
	res, err := run.Run(ctx, cypher, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	summary, err := res.Consume(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	return summary.Counters(), nil
}

// recordString reads a string column, treating null as the empty string.
func recordString(rec *neo4j.Record, key string) (string, bool) {
	v, ok := rec.Get(key)
	if !ok || v == nil {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
