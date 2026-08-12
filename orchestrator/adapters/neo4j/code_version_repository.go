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
//   - A version node is immutable. Every property, :PREVIOUS included, is set
//     under ON CREATE. A node that reverts to earlier code MERGEs onto the
//     version that already exists; rewriting that version's :PREVIOUS would
//     close the chain into a cycle and hang every chain walk.
//   - The pointer guard compares pointer-move times, held on the :CURRENT
//     relationship, not the target version's own promoted_at — after a revert
//     the target's timestamp is old, so comparing against it would let a stale
//     redelivery drag the pointer backwards.
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
// of its batch's transaction.
type nodeState struct {
	currentHash  string
	currentSince time.Time
	knownHashes  map[string]struct{}
	maxSeq       int64
}

// unitState is the same for one shared-code unit. Unit versions carry no
// sequence number: the :PREVIOUS chain is their only ordering.
type unitState struct {
	currentChecksum string
	currentSince    time.Time
	knownChecksums  map[string]struct{}
}

// WriteVersions ingests one release's code versions.
func (r *CodeVersionRepository) WriteVersions(
	ctx context.Context,
	in codeversion.WriteInput,
) (codeversion.WriteResult, error) {
	var out codeversion.WriteResult

	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	graphRelease, err := r.readGraphRelease(ctx, session)
	if err != nil {
		return out, err
	}
	out.GraphReleaseID = graphRelease

	unitsByID := make(map[string]codeversion.CodeUnitVersion, len(in.Units))
	for _, u := range in.Units {
		unitsByID[u.UnitID] = u
	}

	for start := 0; start < len(in.Nodes); start += versionsBatchSize {
		end := start + versionsBatchSize
		if end > len(in.Nodes) {
			end = len(in.Nodes)
		}

		batch, err := r.writeBatch(ctx, session, in, in.Nodes[start:end], unitsByID)
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
func (r *CodeVersionRepository) readGraphRelease(ctx context.Context, session neo4j.SessionWithContext) (string, error) {
	res, err := session.Run(ctx, `
		OPTIONAL MATCH (m:Meta {key: 'current_release'})
		RETURN m.release_id AS release_id
	`, nil)
	if err != nil {
		return "", fmt.Errorf("read current release meta: %w", err)
	}
	var id string
	if res.Next(ctx) {
		if v, ok := res.Record().Get("release_id"); ok && v != nil {
			id, _ = v.(string)
		}
	}
	if err := res.Err(); err != nil {
		return "", fmt.Errorf("iterate current release meta: %w", err)
	}
	return id, nil
}

// writeBatch applies one batch of nodes in a single explicit transaction: read
// the graph's state for the batch, decide what changed, then write the shared
// code the changed nodes reference, the node versions themselves, their chain
// and shared-code edges, and finally the pointer moves.
func (r *CodeVersionRepository) writeBatch(
	ctx context.Context,
	session neo4j.SessionWithContext,
	in codeversion.WriteInput,
	nodes []codeversion.NodeVersion,
	unitsByID map[string]codeversion.CodeUnitVersion,
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
		links      []map[string]any
		currents   []map[string]any
		uses       []map[string]any
		unitIDSet  = map[string]struct{}{}
	)
	for _, n := range nodes {
		st, matched := states[n.UniqueID]
		if !matched {
			out.UnmatchedNodeIDs = append(out.UnmatchedNodeIDs, n.UniqueID)
			continue
		}
		if st.currentHash == n.ContentHash {
			continue // the graph already records this code
		}

		_, exists := st.knownHashes[n.ContentHash]
		nodeParams = append(nodeParams, map[string]any{
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
			"version_seq":        st.maxSeq + 1,
			"healed":             n.Healed,
		})

		// :PREVIOUS is written only for a version this call creates, which is
		// what keeps the chain acyclic across a revert.
		if !exists && st.currentHash != "" {
			links = append(links, map[string]any{
				"unique_id":     n.UniqueID,
				"content_hash":  n.ContentHash,
				"previous_hash": st.currentHash,
			})
		}
		if st.currentSince.IsZero() || promotedAt.After(st.currentSince) {
			currents = append(currents, map[string]any{
				"unique_id":    n.UniqueID,
				"content_hash": n.ContentHash,
			})
		}
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

	if len(links) > 0 {
		if _, err := r.runCounted(ctx, tx, `
			UNWIND $links AS l
			MATCH (v:NodeVersion {unique_id: l.unique_id, content_hash: l.content_hash})
			MATCH (p:NodeVersion {unique_id: l.unique_id, content_hash: l.previous_hash})
			MERGE (v)-[:PREVIOUS]->(p)
		`, map[string]any{"links": links}, "chain :NodeVersion history"); err != nil {
			return out, err
		}
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
		if _, err := r.runCounted(ctx, tx, `
			UNWIND $currents AS c
			MATCH (t:Table {unique_id: c.unique_id})
			MATCH (v:NodeVersion {unique_id: c.unique_id, content_hash: c.content_hash})
			OPTIONAL MATCH (t)-[old:CURRENT]->(:NodeVersion)
			DELETE old
			CREATE (t)-[:CURRENT {promoted_at: $promoted_at, release_id: $release_id}]->(v)
		`, map[string]any{
			"currents":    currents,
			"promoted_at": promotedAt,
			"release_id":  in.ReleaseID,
		}, "move :CURRENT pointers"); err != nil {
			return out, err
		}
		out.CurrentPointersMoved = len(currents)
	}

	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("commit code-version tx: %w", err)
	}
	return out, nil
}

// writeUnits records the shared-code versions the batch's written nodes
// reference, chains them, and moves their pointers. It runs before the node
// writes so every :USES_CODE edge finds its target.
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
		unitLinks    []map[string]any
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
		if _, exists := st.knownChecksums[u.Checksum]; !exists && st.currentChecksum != "" {
			unitLinks = append(unitLinks, map[string]any{
				"unit_id":           u.UnitID,
				"checksum":          u.Checksum,
				"previous_checksum": st.currentChecksum,
			})
		}
		if st.currentChecksum != u.Checksum &&
			(st.currentSince.IsZero() || promotedAt.After(st.currentSince)) {
			unitCurrents = append(unitCurrents, map[string]any{
				"unit_id":  u.UnitID,
				"checksum": u.Checksum,
			})
		}
	}
	if len(unitParams) == 0 {
		return 0, nil
	}

	created, err := r.runCounted(ctx, tx, `
		UNWIND $units AS u
		MERGE (cu:CodeUnit {unit_id: u.unit_id})
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
	// The statement creates a :CodeUnit alongside each new version, so the raw
	// node count double-counts a unit's first version. Report versions only.
	versionsCreated := 0
	for _, p := range unitParams {
		if _, exists := states[p["unit_id"].(string)].knownChecksums[p["checksum"].(string)]; !exists {
			versionsCreated++
		}
	}
	if versionsCreated > created {
		versionsCreated = created
	}

	if len(unitLinks) > 0 {
		if _, err := r.runCounted(ctx, tx, `
			UNWIND $unit_links AS l
			MATCH (v:CodeUnitVersion {unit_id: l.unit_id, checksum: l.checksum})
			MATCH (p:CodeUnitVersion {unit_id: l.unit_id, checksum: l.previous_checksum})
			MERGE (v)-[:PREVIOUS]->(p)
		`, map[string]any{"unit_links": unitLinks}, "chain :CodeUnitVersion history"); err != nil {
			return 0, err
		}
	}

	if len(unitCurrents) > 0 {
		if _, err := r.runCounted(ctx, tx, `
			UNWIND $unit_currents AS c
			MATCH (cu:CodeUnit {unit_id: c.unit_id})
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

// readNodeStates returns, per requested unique_id that has a :Table, the hash of
// its current version, when that pointer was set, every hash already recorded
// for it, and the highest sequence number in use. Ids absent from the result had
// no :Table.
func (r *CodeVersionRepository) readNodeStates(
	ctx context.Context,
	tx neo4j.ExplicitTransaction,
	uniqueIDs []string,
) (map[string]nodeState, error) {
	// Only unique_id is a grouping key, so every other column stays inside an
	// aggregate: head(collect(...)) collapses the at-most-one pointer row while
	// collect/max fold the many version rows. collect drops nulls, so a node
	// with no versions yields an empty list and a zero maximum.
	res, err := tx.Run(ctx, `
		UNWIND $unique_ids AS uid
		MATCH (t:Table {unique_id: uid})
		OPTIONAL MATCH (t)-[cur:CURRENT]->(cv:NodeVersion)
		OPTIONAL MATCH (av:NodeVersion {unique_id: uid})
		RETURN uid                                     AS unique_id,
		       head(collect(DISTINCT cv.content_hash)) AS current_hash,
		       head(collect(DISTINCT cur.promoted_at)) AS current_since,
		       collect(DISTINCT av.content_hash)       AS known_hashes,
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
		since := recordTime(rec, "current_since")
		known := make(map[string]struct{})
		if v, ok := rec.Get("known_hashes"); ok {
			if list, ok := v.([]any); ok {
				for _, h := range list {
					if s, ok := h.(string); ok {
						known[s] = struct{}{}
					}
				}
			}
		}
		var maxSeq int64
		if v, ok := rec.Get("max_seq"); ok {
			maxSeq, _ = v.(int64)
		}
		states[uid] = nodeState{currentHash: hash, currentSince: since, knownHashes: known, maxSeq: maxSeq}
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
		OPTIONAL MATCH (av:CodeUnitVersion {unit_id: uid})
		RETURN uid                                     AS unit_id,
		       head(collect(DISTINCT ccv.checksum))    AS current_checksum,
		       head(collect(DISTINCT cur.promoted_at)) AS current_since,
		       collect(DISTINCT av.checksum)           AS known_checksums
	`, map[string]any{"unit_ids": unitIDs})
	if err != nil {
		return nil, fmt.Errorf("read shared-code version state: %w", err)
	}

	states := make(map[string]unitState, len(unitIDs))
	for res.Next(ctx) {
		rec := res.Record()
		id, _ := recordString(rec, "unit_id")
		checksum, _ := recordString(rec, "current_checksum")
		since := recordTime(rec, "current_since")
		known := make(map[string]struct{})
		if v, ok := rec.Get("known_checksums"); ok {
			if list, ok := v.([]any); ok {
				for _, c := range list {
					if s, ok := c.(string); ok {
						known[s] = struct{}{}
					}
				}
			}
		}
		states[id] = unitState{currentChecksum: checksum, currentSince: since, knownChecksums: known}
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

// recordTime reads a timestamp column. A missing or unexpected value yields the
// zero time, which the pointer guard reads as "no previous pointer" — the safe
// direction, since a version write that should have moved the pointer is healed
// by the next release while a skipped move would persist.
func recordTime(rec *neo4j.Record, key string) time.Time {
	v, ok := rec.Get(key)
	if !ok || v == nil {
		return time.Time{}
	}
	t, ok := v.(time.Time)
	if !ok {
		return time.Time{}
	}
	return t
}
