package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// versionsBatchSize bounds how many nodes one transaction carries. A baseline
// release ingests the whole estate at once, and a single unbounded transaction
// holding every node's source text would dominate the server's heap; batching
// also means a failure re-does one batch rather than all of them.
const versionsBatchSize = 100

// CodeVersionRepository implements repository.CodeVersionRepository against
// Neo4j.
//
// Its write predicate is graph-authoritative: a version is written when the
// incoming content_hash differs from the one on the node's current version. The
// promoted event's `changed` flags are deliberately not consulted — they cannot
// heal a write the graph missed, whereas comparing against the graph makes every
// later release converge it.
//
// Two invariants hold the model together:
//
//   - A version node is immutable. Every property is set under ON CREATE. A
//     node that reverts to earlier code MERGEs onto the version that already
//     exists rather than writing a new one, so the graph holds exactly one
//     version per distinct (unique_id, content_hash) ever observed. A node's
//     versions are enumerated directly through the (:NodeVersion {unique_id})
//     index — no fan-out edge between them is needed — and ordered by
//     promoted_at, the wall-clock time the release that introduced each one
//     was promoted.
//   - Ordering is decided by a per-node watermark — code_version_promoted_at on
//     the :Table, and on the :CodeUnit for a shared-code unit — never from a
//     pre-write read in Go. Every matched node advances its watermark, including
//     one whose code is unchanged, and every pointer write is guarded on it
//     inside the same statement. Writing the watermark takes the anchor node's
//     write lock, so two replicas promoting different releases serialise: an
//     older promotion can neither overwrite a newer pointer nor mark its version
//     current over newer code, while still recording its own history.
//     version_seq is a stable per-node handle for addressing one of a node's
//     versions, assigned max+1 at ingestion — it is NOT an ordering, since a
//     late-arriving older release still receives the highest value.
type CodeVersionRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

// Compile-time assertion that the adapter satisfies the domain port.
var _ repository.CodeVersionRepository = (*CodeVersionRepository)(nil)

// NewCodeVersionRepository constructs a CodeVersionRepository backed by the
// given Neo4j client.
func NewCodeVersionRepository(client Neo4jClient, logger *slog.Logger) *CodeVersionRepository {
	return &CodeVersionRepository{client: client, logger: logger}
}

// nodeState is the graph's current knowledge about one node, read at the start
// of its batch's transaction. It decides what to write; whether this release is
// new enough to move the pointer is decided in the database, not here.
type nodeState struct {
	currentHash string
	maxSeq      int64
}

// unitState is the same for one shared-code unit. Unit versions carry no
// sequence number; ordering comes from promoted_at on the version node itself.
type unitState struct {
	currentChecksum string
}

// WriteVersions ingests one release's code versions.
func (r *CodeVersionRepository) WriteVersions(
	ctx context.Context,
	in codeversion.WriteInput,
) (codeversion.WriteResult, error) {
	var out codeversion.WriteResult

	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	graphRelease, graphAt, err := r.readGraphRelease(ctx, session)
	if err != nil {
		return out, err
	}
	out.GraphReleaseID = graphRelease
	// A topology stamped later than this promotion means the swap has moved past
	// it. Nodes this release carried may already have been retired and deleted,
	// so waiting for them is futile — their history is recorded unattached below.
	out.GraphAhead = graphRelease != "" && graphRelease != in.ReleaseID &&
		graphAt.After(in.PromotedAt.UTC())

	unitsByID := make(map[string]codeversion.CodeUnitVersion, len(in.Units))
	for _, u := range in.Units {
		unitsByID[u.UnitID] = u
	}

	for start := 0; start < len(in.Nodes); start += versionsBatchSize {
		end := start + versionsBatchSize
		if end > len(in.Nodes) {
			end = len(in.Nodes)
		}

		batch, err := r.writeBatch(ctx, session, in, in.Nodes[start:end], unitsByID, out.GraphAhead)
		if err != nil {
			return out, err
		}
		out.NodeVersionsCreated += batch.NodeVersionsCreated
		out.UnitVersionsCreated += batch.UnitVersionsCreated
		out.CurrentPointersMoved += batch.CurrentPointersMoved
		out.UnmatchedNodeIDs = append(out.UnmatchedNodeIDs, batch.UnmatchedNodeIDs...)
	}

	r.logger.Info("code versions ingested",
		"release_id", in.ReleaseID,
		"bundle_nodes", len(in.Nodes),
		"node_versions_created", out.NodeVersionsCreated,
		"unit_versions_created", out.UnitVersionsCreated,
		"current_pointers_moved", out.CurrentPointersMoved,
		"unmatched_nodes", len(out.UnmatchedNodeIDs),
	)
	return out, nil
}

// readGraphRelease returns the release_id the topology currently reflects, or ""
// on a graph that has never been promoted to.
func (r *CodeVersionRepository) readGraphRelease(ctx context.Context, session neo4j.SessionWithContext) (string, time.Time, error) {
	res, err := session.Run(ctx, `
		OPTIONAL MATCH (m:Meta {key: 'current_release'})
		RETURN m.release_id AS release_id, m.updated_at AS updated_at
	`, nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read current release meta: %w", err)
	}
	var id string
	var at time.Time
	if res.Next(ctx) {
		rec := res.Record()
		if v, ok := rec.Get("release_id"); ok && v != nil {
			id, _ = v.(string)
		}
		if v, ok := rec.Get("updated_at"); ok && v != nil {
			at, _ = v.(time.Time)
		}
	}
	if err := res.Err(); err != nil {
		return "", time.Time{}, fmt.Errorf("iterate current release meta: %w", err)
	}
	return id, at, nil
}

// writeBatch applies one batch of nodes in a single explicit transaction: read
// the graph's state for the batch, decide what changed, then write the shared
// code the changed nodes reference, the node versions themselves, their
// shared-code edges, and finally the pointer moves.
func (r *CodeVersionRepository) writeBatch(
	ctx context.Context,
	session neo4j.SessionWithContext,
	in codeversion.WriteInput,
	nodes []codeversion.NodeVersion,
	unitsByID map[string]codeversion.CodeUnitVersion,
	graphAhead bool,
) (codeversion.WriteResult, error) {
	var out codeversion.WriteResult

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return out, fmt.Errorf("begin code-version tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	uniqueIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		uniqueIDs = append(uniqueIDs, n.UniqueID)
	}
	states, err := r.readNodeStates(ctx, tx, uniqueIDs)
	if err != nil {
		return out, err
	}

	promotedAt := in.PromotedAt.UTC()

	// Decide, per node, what the graph needs. A node with no :Table is reported
	// rather than written: the topology swap it trails has not created it yet.
	var (
		nodeParams []map[string]any
		currents   []map[string]any
		uses       []map[string]any
		unitIDSet  = map[string]struct{}{}
	)
	var watermarks []map[string]any
	var orphans []map[string]any
	for _, n := range nodes {
		st, matched := states[n.UniqueID]
		if !matched {
			out.UnmatchedNodeIDs = append(out.UnmatchedNodeIDs, n.UniqueID)
			if graphAhead {
				// The topology has already moved past this release, so this node's
				// :Table is gone for good and no amount of retrying will produce it.
				// Record the version unattached rather than losing the history: it
				// stays reachable by unique_id, and re-attaches if the node returns.
				orphans = append(orphans, versionParams(n, 0))
			}
			continue
		}
		// Every matched node advances the per-node watermark, even one whose code
		// is unchanged. Without that, a release that finds the hash already
		// recorded would leave the watermark behind, and a stale delivery of an
		// intermediate release could later satisfy the guard and drag :CURRENT
		// backwards onto superseded code.
		watermarks = append(watermarks, map[string]any{"unique_id": n.UniqueID})

		if st.currentHash == n.ContentHash {
			continue // the graph already records this code
		}

		nodeParams = append(nodeParams, versionParams(n, st.maxSeq+1))

		// Whether this release is new enough to own the pointer is decided in the
		// database under the watermark's write lock, not from this pre-write read:
		// two replicas processing different promotions would otherwise both pass a
		// Go-side check and the loser could overwrite the winner.
		currents = append(currents, map[string]any{
			"unique_id":    n.UniqueID,
			"content_hash": n.ContentHash,
		})
		for _, ref := range n.UnitRefs {
			unitIDSet[ref.UnitID] = struct{}{}
			uses = append(uses, map[string]any{
				"unique_id":    n.UniqueID,
				"content_hash": n.ContentHash,
				"unit_id":      ref.UnitID,
				"checksum":     ref.Checksum,
			})
		}
	}

	// Advance the per-node watermark first. Writing to the :Table takes a node
	// write lock that serialises concurrent promotions of the same node, and the
	// CASE keeps the newest promotion's timestamp: every later guard in this
	// transaction then reads a value no concurrent writer can change underneath
	// it. A node whose watermark is already at or past this release is left
	// alone, so its guards fail and this release writes no pointer for it.
	if len(watermarks) > 0 {
		if _, err := r.runCounted(ctx, tx, `
			UNWIND $watermarks AS w
			MATCH (t:Table {unique_id: w.unique_id})
			SET t.code_version_promoted_at = CASE
			      WHEN t.code_version_promoted_at IS NULL
			        OR t.code_version_promoted_at < $promoted_at
			      THEN $promoted_at
			      ELSE t.code_version_promoted_at END
		`, map[string]any{
			"watermarks":  watermarks,
			"promoted_at": promotedAt,
		}, "advance :Table code-version watermark"); err != nil {
			return out, err
		}
	}

	unitCreated, err := r.writeUnits(ctx, tx, in, unitIDSet, unitsByID, promotedAt)
	if err != nil {
		return out, err
	}
	out.UnitVersionsCreated = unitCreated

	if len(nodeParams) > 0 {
		created, err := r.runCounted(ctx, tx, `
			UNWIND $nodes AS n
			MATCH (t:Table {unique_id: n.unique_id})
			MERGE (v:NodeVersion {unique_id: n.unique_id, content_hash: n.content_hash})
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
			              v.backfilled         = false
		`, map[string]any{
			"nodes":       nodeParams,
			"repo":        in.Repo,
			"commit_sha":  in.CommitSHA,
			"release_id":  in.ReleaseID,
			"promoted_at": promotedAt,
		}, "write :NodeVersion nodes")
		if err != nil {
			return out, err
		}
		out.NodeVersionsCreated = created
	}

	if len(orphans) > 0 {
		// No :Table to hang these off, and none is coming. version_seq is derived
		// in-query from whatever versions the node already has, and backfilled
		// marks them as reconstructed rather than observed on a live topology.
		created, err := r.runCounted(ctx, tx, `
			UNWIND $orphans AS n
			OPTIONAL MATCH (prev:NodeVersion {unique_id: n.unique_id})
			WITH n, coalesce(max(prev.version_seq), 0) AS max_seq
			MERGE (v:NodeVersion {unique_id: n.unique_id, content_hash: n.content_hash})
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
			              v.version_seq        = max_seq + 1,
			              v.healed             = n.healed,
			              v.backfilled         = true
		`, map[string]any{
			"orphans":     orphans,
			"repo":        in.Repo,
			"commit_sha":  in.CommitSHA,
			"release_id":  in.ReleaseID,
			"promoted_at": promotedAt,
		}, "write unattached :NodeVersion history")
		if err != nil {
			return out, err
		}
		out.NodeVersionsCreated += created
	}

	if len(uses) > 0 {
		if _, err := r.runCounted(ctx, tx, `
			UNWIND $uses AS u
			MATCH (v:NodeVersion {unique_id: u.unique_id, content_hash: u.content_hash})
			MATCH (cv:CodeUnitVersion {unit_id: u.unit_id, checksum: u.checksum})
			MERGE (v)-[:USES_CODE]->(cv)
		`, map[string]any{"uses": uses}, "link :USES_CODE edges"); err != nil {
			return out, err
		}
	}

	if len(currents) > 0 {
		// The comparison and the replacement happen in one query, behind the same
		// watermark write lock taken above, so an older promotion can never delete
		// a newer pointer and put a stale one in its place.
		res, err := tx.Run(ctx, `
			UNWIND $currents AS c
			MATCH (t:Table {unique_id: c.unique_id})
			WHERE t.code_version_promoted_at = $promoted_at
			MATCH (v:NodeVersion {unique_id: c.unique_id, content_hash: c.content_hash})
			OPTIONAL MATCH (t)-[old:CURRENT]->(:NodeVersion)
			DELETE old
			CREATE (t)-[:CURRENT {promoted_at: $promoted_at, release_id: $release_id}]->(v)
		`, map[string]any{
			"currents":    currents,
			"promoted_at": promotedAt,
			"release_id":  in.ReleaseID,
		})
		if err != nil {
			return out, fmt.Errorf("move :CURRENT pointers: %w", err)
		}
		summary, err := res.Consume(ctx)
		if err != nil {
			return out, fmt.Errorf("move :CURRENT pointers: %w", err)
		}
		// Count what the database actually did: a node whose watermark had already
		// moved past this release is skipped by the guard.
		out.CurrentPointersMoved = summary.Counters().RelationshipsCreated()
	}

	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("commit code-version tx: %w", err)
	}
	return out, nil
}

// writeUnits records the shared-code versions the batch's written nodes
// reference and moves their pointers. It runs before the node writes so every
// :USES_CODE edge finds its target.
func (r *CodeVersionRepository) writeUnits(
	ctx context.Context,
	tx neo4j.ExplicitTransaction,
	in codeversion.WriteInput,
	unitIDSet map[string]struct{},
	unitsByID map[string]codeversion.CodeUnitVersion,
	promotedAt time.Time,
) (int, error) {
	if len(unitIDSet) == 0 {
		return 0, nil
	}
	unitIDs := make([]string, 0, len(unitIDSet))
	for id := range unitIDSet {
		unitIDs = append(unitIDs, id)
	}
	states, err := r.readUnitStates(ctx, tx, unitIDs)
	if err != nil {
		return 0, err
	}

	var (
		unitParams   []map[string]any
		unitCurrents []map[string]any
	)
	for _, id := range unitIDs {
		u, ok := unitsByID[id]
		if !ok {
			continue
		}
		st := states[id]
		unitParams = append(unitParams, map[string]any{
			"unit_id":  u.UnitID,
			"checksum": u.Checksum,
			"source":   u.Source,
		})
		if st.currentChecksum != u.Checksum {
			unitCurrents = append(unitCurrents, map[string]any{
				"unit_id":  u.UnitID,
				"checksum": u.Checksum,
			})
		}
	}
	if len(unitParams) == 0 {
		return 0, nil
	}

	// The unit identity and its version are merged in two statements rather than
	// one, so the second statement's created-node count is exactly the number of
	// unit versions written — a combined statement would fold each unit's first
	// :CodeUnit into the same count. The MERGE doubles as the watermark advance:
	// writing to the :CodeUnit takes its node write lock, which serialises
	// concurrent promotions of the same unit exactly as :Table does for a node.
	if _, err := r.runCounted(ctx, tx, `
		UNWIND $units AS u
		MERGE (cu:CodeUnit {unit_id: u.unit_id})
		SET cu.code_version_promoted_at = CASE
		      WHEN cu.code_version_promoted_at IS NULL
		        OR cu.code_version_promoted_at < $promoted_at
		      THEN $promoted_at
		      ELSE cu.code_version_promoted_at END
	`, map[string]any{
		"units":       unitParams,
		"promoted_at": promotedAt,
	}, "write :CodeUnit nodes"); err != nil {
		return 0, err
	}

	versionsCreated, err := r.runCounted(ctx, tx, `
		UNWIND $units AS u
		MERGE (v:CodeUnitVersion {unit_id: u.unit_id, checksum: u.checksum})
		ON CREATE SET v.source      = u.source,
		              v.repo        = $repo,
		              v.commit_sha  = $commit_sha,
		              v.release_id  = $release_id,
		              v.promoted_at = $promoted_at
	`, map[string]any{
		"units":       unitParams,
		"repo":        in.Repo,
		"commit_sha":  in.CommitSHA,
		"release_id":  in.ReleaseID,
		"promoted_at": promotedAt,
	}, "write :CodeUnitVersion nodes")
	if err != nil {
		return 0, err
	}

	if len(unitCurrents) > 0 {
		if _, err := r.runCounted(ctx, tx, `
			UNWIND $unit_currents AS c
			MATCH (cu:CodeUnit {unit_id: c.unit_id})
			WHERE cu.code_version_promoted_at = $promoted_at
			MATCH (v:CodeUnitVersion {unit_id: c.unit_id, checksum: c.checksum})
			OPTIONAL MATCH (cu)-[old:CURRENT]->(:CodeUnitVersion)
			DELETE old
			CREATE (cu)-[:CURRENT {promoted_at: $promoted_at, release_id: $release_id}]->(v)
		`, map[string]any{
			"unit_currents": unitCurrents,
			"promoted_at":   promotedAt,
			"release_id":    in.ReleaseID,
		}, "move :CodeUnit pointers"); err != nil {
			return 0, err
		}
	}

	return versionsCreated, nil
}

// versionParams renders one version's write parameters. Both the attached and
// the unattached write paths use it so their stored properties cannot drift
// apart; seq is ignored by the unattached path, which derives it in-query.
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

// readNodeStates returns, per requested unique_id that has a :Table, the hash of
// its current version and the highest sequence number in use. Ids absent from
// the result had no :Table.
func (r *CodeVersionRepository) readNodeStates(
	ctx context.Context,
	tx neo4j.ExplicitTransaction,
	uniqueIDs []string,
) (map[string]nodeState, error) {
	// Only unique_id is a grouping key, so every other column stays inside an
	// aggregate: head(collect(...)) collapses the at-most-one pointer row while
	// max folds the many version rows. A node with no versions yields a zero
	// maximum.
	res, err := tx.Run(ctx, `
		UNWIND $unique_ids AS uid
		MATCH (t:Table {unique_id: uid})
		OPTIONAL MATCH (t)-[cur:CURRENT]->(cv:NodeVersion)
		OPTIONAL MATCH (av:NodeVersion {unique_id: uid})
		RETURN uid                                     AS unique_id,
		       head(collect(DISTINCT cv.content_hash)) AS current_hash,
		       coalesce(max(av.version_seq), 0)        AS max_seq
	`, map[string]any{"unique_ids": uniqueIDs})
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

// readUnitStates is readNodeStates for shared-code units. A unit that has never
// been recorded simply yields a zero state.
func (r *CodeVersionRepository) readUnitStates(
	ctx context.Context,
	tx neo4j.ExplicitTransaction,
	unitIDs []string,
) (map[string]unitState, error) {
	res, err := tx.Run(ctx, `
		UNWIND $unit_ids AS uid
		OPTIONAL MATCH (cu:CodeUnit {unit_id: uid})-[cur:CURRENT]->(ccv:CodeUnitVersion)
		RETURN uid                                     AS unit_id,
		       head(collect(DISTINCT ccv.checksum))    AS current_checksum
	`, map[string]any{"unit_ids": unitIDs})
	if err != nil {
		return nil, fmt.Errorf("read shared-code version state: %w", err)
	}

	states := make(map[string]unitState, len(unitIDs))
	for res.Next(ctx) {
		rec := res.Record()
		id, _ := recordString(rec, "unit_id")
		checksum, _ := recordString(rec, "current_checksum")
		states[id] = unitState{currentChecksum: checksum}
	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("iterate shared-code version state: %w", err)
	}
	return states, nil
}

// runCounted executes a write statement and returns how many nodes it created.
// Bolt surfaces a failure either from Run or only when the summary is pulled, so
// every statement is consumed and checked.
func (r *CodeVersionRepository) runCounted(
	ctx context.Context,
	tx neo4j.ExplicitTransaction,
	cypher string,
	params map[string]any,
	what string,
) (int, error) {
	res, err := tx.Run(ctx, cypher, params)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	summary, err := res.Consume(ctx)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	return summary.Counters().NodesCreated(), nil
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
