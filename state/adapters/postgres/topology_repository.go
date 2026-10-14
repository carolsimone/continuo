package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// topologyLockKey is the transaction-scoped advisory-lock key that serialises
// the live-release pointer step and the seed reconcile across handlers, loops
// and pods. No other advisory lock uses it.
const topologyLockKey int64 = 0x544F_504F // "TOPO"

// copyTopologyNodes and copyTopologyEdges stream a release's rows with COPY …
// FROM STDIN, one round trip per table.
const (
	copyTopologyNodes = `COPY topology_nodes (tenant_id, release_id, node_key, unique_id, service_name, schema_name, table_name, node_type, test_count, content_hash, image_tag, secret_ref, schedule_name) FROM STDIN`
	copyTopologyEdges = `COPY topology_edges (tenant_id, release_id, upstream_key, downstream_key) FROM STDIN`
)

// TopologyRepository implements repository.TopologyRepository over state's
// Postgres. Writes run in the unit of work's transaction; reads run in it when
// one is open, so they see its writes and the lock it holds.
type TopologyRepository struct {
	db *sqlx.DB
	tx *sqlx.Tx
}

// Compile-time assertion that the adapter satisfies the domain port.
var _ repository.TopologyRepository = (*TopologyRepository)(nil)

// NewTopologyRepository binds a TopologyRepository to tx; a nil tx runs every
// statement on the autocommit db.
func NewTopologyRepository(db *sqlx.DB, tx *sqlx.Tx) *TopologyRepository {
	return &TopologyRepository{db: db, tx: tx}
}

func (r *TopologyRepository) conn() sqlx.ExtContext {
	if r.tx != nil {
		return r.tx
	}
	return r.db
}

// LockTopology takes pg_advisory_xact_lock on the topology key; it is released
// when the transaction ends.
func (r *TopologyRepository) LockTopology(ctx context.Context) error {
	if r.tx == nil {
		return errors.New("LockTopology requires an active transaction")
	}
	if _, err := r.tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, topologyLockKey); err != nil {
		return fmt.Errorf("acquire the topology lock: %w", err)
	}
	return nil
}

// SaveRelease inserts the release row; only a new row loads the nodes and the
// edges. A concurrent insert of the same release waits on the first
// transaction and then finds the row stored.
func (r *TopologyRepository) SaveRelease(ctx context.Context, rel topology.Release) (bool, error) {
	if r.tx == nil {
		return false, errors.New("SaveRelease requires an active transaction")
	}
	res, err := r.tx.ExecContext(ctx, `
		INSERT INTO topology_releases (tenant_id, release_id, promotion_seq, topology_uri, topology_sha256)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, release_id) DO NOTHING`,
		rel.TenantID, rel.ReleaseID, rel.PromotionSeq, rel.TopologyURI, rel.TopologySHA256)
	if err != nil {
		return false, fmt.Errorf("insert topology release %s: %w", rel.ReleaseID, err)
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("insert topology release %s: %w", rel.ReleaseID, err)
	}
	if inserted == 0 {
		return false, r.reconcileStoredRelease(ctx, rel)
	}
	if err := copyRows(ctx, r.tx, copyTopologyNodes, nodeRows(rel)); err != nil {
		return false, fmt.Errorf("copy the nodes of release %s: %w", rel.ReleaseID, err)
	}
	if err := copyRows(ctx, r.tx, copyTopologyEdges, edgeRows(rel)); err != nil {
		return false, fmt.Errorf("copy the edges of release %s: %w", rel.ReleaseID, err)
	}
	return true, nil
}

// reconcileStoredRelease checks an incoming release against the stored row of
// the same release: a different checksum is a conflict, and a higher promotion
// seq is recorded. The row is locked first, so retention cannot delete it
// under the caller.
func (r *TopologyRepository) reconcileStoredRelease(ctx context.Context, rel topology.Release) error {
	var stored struct {
		SHA256       string `db:"topology_sha256"`
		PromotionSeq int64  `db:"promotion_seq"`
	}
	if err := r.tx.GetContext(ctx, &stored, `
		SELECT topology_sha256, promotion_seq FROM topology_releases
		WHERE tenant_id = $1 AND release_id = $2
		FOR UPDATE`, rel.TenantID, rel.ReleaseID); err != nil {
		return fmt.Errorf("read stored topology release %s: %w", rel.ReleaseID, err)
	}
	if stored.SHA256 != rel.TopologySHA256 {
		return fmt.Errorf("%w: release %s was loaded from sha256 %s and is announced with %s",
			repository.ErrReleaseChecksumConflict, rel.ReleaseID, stored.SHA256, rel.TopologySHA256)
	}
	if rel.PromotionSeq > stored.PromotionSeq {
		if _, err := r.tx.ExecContext(ctx, `
			UPDATE topology_releases SET promotion_seq = $3
			WHERE tenant_id = $1 AND release_id = $2`,
			rel.TenantID, rel.ReleaseID, rel.PromotionSeq); err != nil {
			return fmt.Errorf("raise the promotion seq of release %s: %w", rel.ReleaseID, err)
		}
	}
	return nil
}

func nodeRows(rel topology.Release) [][]any {
	rows := make([][]any, 0, len(rel.Nodes))
	for _, n := range rel.Nodes {
		var testCount any
		if n.TestCount != nil {
			testCount = int64(*n.TestCount)
		}
		rows = append(rows, []any{
			rel.TenantID, rel.ReleaseID, string(n.Key), n.UniqueID, n.ServiceName, n.SchemaName, n.TableName,
			n.NodeType, testCount, n.ContentHash, n.ImageTag, n.SecretRef, n.ScheduleName,
		})
	}
	return rows
}

func edgeRows(rel topology.Release) [][]any {
	rows := make([][]any, 0, len(rel.Edges))
	for _, e := range rel.Edges {
		rows = append(rows, []any{rel.TenantID, rel.ReleaseID, string(e.Upstream), string(e.Downstream)})
	}
	return rows
}

// copyRows streams rows through the COPY … FROM STDIN statement query inside
// tx. lib/pq sends the rows as one COPY; the final Exec without arguments
// flushes it and reports any row the server refused. No rows writes nothing.
func copyRows(ctx context.Context, tx *sqlx.Tx, query string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			_ = stmt.Close()
			return err
		}
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		_ = stmt.Close()
		return err
	}
	return stmt.Close()
}

// LivePointer reads the tenant's live_release row.
func (r *TopologyRepository) LivePointer(ctx context.Context, tenantID string) (topology.Pointer, error) {
	var row struct {
		ReleaseID    string `db:"release_id"`
		PromotionSeq int64  `db:"promotion_seq"`
	}
	err := sqlx.GetContext(ctx, r.conn(), &row,
		`SELECT release_id, promotion_seq FROM live_release WHERE tenant_id = $1`, tenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return topology.Pointer{}, nil
	}
	if err != nil {
		return topology.Pointer{}, fmt.Errorf("read the live release of tenant %s: %w", tenantID, err)
	}
	return topology.Pointer{ReleaseID: row.ReleaseID, PromotionSeq: row.PromotionSeq}, nil
}

// SetLivePointer upserts the tenant's live_release row.
func (r *TopologyRepository) SetLivePointer(ctx context.Context, tenantID string, p topology.Pointer, at time.Time) error {
	if _, err := r.conn().ExecContext(ctx, `
		INSERT INTO live_release (tenant_id, release_id, promotion_seq, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (tenant_id) DO UPDATE
		SET release_id = EXCLUDED.release_id,
		    promotion_seq = EXCLUDED.promotion_seq,
		    updated_at = EXCLUDED.updated_at`,
		tenantID, p.ReleaseID, p.PromotionSeq, at); err != nil {
		return fmt.Errorf("make %s the live release of tenant %s: %w", p.ReleaseID, tenantID, err)
	}
	return nil
}

type topologyNodeRow struct {
	NodeKey      string        `db:"node_key"`
	UniqueID     string        `db:"unique_id"`
	ServiceName  string        `db:"service_name"`
	SchemaName   string        `db:"schema_name"`
	TableName    string        `db:"table_name"`
	NodeType     string        `db:"node_type"`
	TestCount    sql.NullInt32 `db:"test_count"`
	ContentHash  string        `db:"content_hash"`
	ImageTag     string        `db:"image_tag"`
	SecretRef    string        `db:"secret_ref"`
	ScheduleName string        `db:"schedule_name"`
}

func (row topologyNodeRow) toDomain() topology.Node {
	var testCount *int
	if row.TestCount.Valid {
		n := int(row.TestCount.Int32)
		testCount = &n
	}
	return topology.Node{
		Key:          topology.NodeKey(row.NodeKey),
		UniqueID:     row.UniqueID,
		ServiceName:  row.ServiceName,
		SchemaName:   row.SchemaName,
		TableName:    row.TableName,
		NodeType:     row.NodeType,
		TestCount:    testCount,
		ContentHash:  row.ContentHash,
		ImageTag:     row.ImageTag,
		SecretRef:    row.SecretRef,
		ScheduleName: row.ScheduleName,
	}
}

// ReleaseNodes reads a release's nodes in byte order of the key.
func (r *TopologyRepository) ReleaseNodes(ctx context.Context, tenantID, releaseID, nodeType string) ([]topology.Node, error) {
	var rows []topologyNodeRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT node_key, unique_id, service_name, schema_name, table_name, node_type, test_count,
		       content_hash, image_tag, secret_ref, schedule_name
		FROM topology_nodes
		WHERE tenant_id = $1 AND release_id = $2 AND ($3::text = '' OR node_type = $3::text)
		ORDER BY node_key COLLATE "C"`, tenantID, releaseID, nodeType); err != nil {
		return nil, fmt.Errorf("read the nodes of release %s: %w", releaseID, err)
	}
	nodes := make([]topology.Node, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, row.toDomain())
	}
	return nodes, nil
}

// topologyNodeColumns lists the topology_nodes columns topologyNodeRow scans.
const topologyNodeColumns = `node_key, unique_id, service_name, schema_name, table_name, node_type, test_count,
		       content_hash, image_tag, secret_ref, schedule_name`

// ScheduleGraph reads the schedule's nodes through idx_topology_nodes_schedule
// and the edges into them, then the dbt-seed nodes among the upstreams those
// edges name outside the schedule. Each read is an index lookup by key, so the
// cost follows the schedule's size whatever the table's statistics say.
func (r *TopologyRepository) ScheduleGraph(ctx context.Context, tenantID, releaseID, scheduleName string) (topology.Graph, error) {
	var rows []topologyNodeRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT `+topologyNodeColumns+`
		FROM topology_nodes
		WHERE tenant_id = $1 AND release_id = $2 AND schedule_name = $3`,
		tenantID, releaseID, scheduleName); err != nil {
		return topology.Graph{}, fmt.Errorf("read the nodes of schedule %s in release %s: %w", scheduleName, releaseID, err)
	}
	if len(rows) == 0 {
		return topology.Graph{}, nil
	}
	edges, err := r.edgesInto(ctx, tenantID, releaseID, rows)
	if err != nil {
		return topology.Graph{}, err
	}
	held := make(map[string]bool, len(rows))
	for _, row := range rows {
		held[row.NodeKey] = true
	}
	var outside []string
	for _, e := range edges {
		if !held[string(e.Upstream)] {
			held[string(e.Upstream)] = true
			outside = append(outside, string(e.Upstream))
		}
	}
	if len(outside) > 0 {
		var seeds []topologyNodeRow
		if err := sqlx.SelectContext(ctx, r.conn(), &seeds, `
			SELECT `+topologyNodeColumns+`
			FROM topology_nodes
			WHERE tenant_id = $1 AND release_id = $2 AND node_key = ANY($3) AND node_type = $4`,
			tenantID, releaseID, pq.Array(outside), string(model.NodeTypeDbtSeed)); err != nil {
			return topology.Graph{}, fmt.Errorf("read the seeds schedule %s reads in release %s: %w", scheduleName, releaseID, err)
		}
		rows = append(rows, seeds...)
	}
	return graphOf(rows, edges), nil
}

// Graph reads the release's nodes with a key in keys through the primary key,
// then the edges into them.
func (r *TopologyRepository) Graph(ctx context.Context, tenantID, releaseID string, keys []topology.NodeKey) (topology.Graph, error) {
	if len(keys) == 0 {
		return topology.Graph{}, nil
	}
	wanted := make([]string, 0, len(keys))
	for _, k := range keys {
		wanted = append(wanted, string(k))
	}
	var rows []topologyNodeRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT `+topologyNodeColumns+`
		FROM topology_nodes
		WHERE tenant_id = $1 AND release_id = $2 AND node_key = ANY($3)`,
		tenantID, releaseID, pq.Array(wanted)); err != nil {
		return topology.Graph{}, fmt.Errorf("read %d nodes of release %s: %w", len(wanted), releaseID, err)
	}
	if len(rows) == 0 {
		return topology.Graph{}, nil
	}
	edges, err := r.edgesInto(ctx, tenantID, releaseID, rows)
	if err != nil {
		return topology.Graph{}, err
	}
	return graphOf(rows, edges), nil
}

// edgesInto reads every edge of the release whose downstream is one of rows,
// joining the keys as a set so each is one probe of
// idx_topology_edges_downstream.
func (r *TopologyRepository) edgesInto(ctx context.Context, tenantID, releaseID string, rows []topologyNodeRow) ([]topology.Edge, error) {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row.NodeKey)
	}
	var edgeRows []struct {
		Upstream   string `db:"upstream_key"`
		Downstream string `db:"downstream_key"`
	}
	if err := sqlx.SelectContext(ctx, r.conn(), &edgeRows, `
		SELECT e.upstream_key, e.downstream_key
		FROM unnest($3::text[]) AS d(node_key)
		JOIN topology_edges e
		  ON e.tenant_id = $1 AND e.release_id = $2 AND e.downstream_key = d.node_key`,
		tenantID, releaseID, pq.Array(keys)); err != nil {
		return nil, fmt.Errorf("read the edges into %d nodes of release %s: %w", len(keys), releaseID, err)
	}
	edges := make([]topology.Edge, 0, len(edgeRows))
	for _, e := range edgeRows {
		edges = append(edges, topology.Edge{Upstream: topology.NodeKey(e.Upstream), Downstream: topology.NodeKey(e.Downstream)})
	}
	return edges, nil
}

// graphOf builds the graph of rows; topology.NewGraph drops every edge with an
// end outside them.
func graphOf(rows []topologyNodeRow, edges []topology.Edge) topology.Graph {
	nodes := make([]topology.Node, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, row.toDomain())
	}
	return topology.NewGraph(nodes, edges)
}

// NodeContentHash reads one node's content hash.
func (r *TopologyRepository) NodeContentHash(ctx context.Context, tenantID, releaseID string, key topology.NodeKey) (string, bool, error) {
	var hash string
	err := sqlx.GetContext(ctx, r.conn(), &hash, `
		SELECT content_hash FROM topology_nodes
		WHERE tenant_id = $1 AND release_id = $2 AND node_key = $3`, tenantID, releaseID, string(key))
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read the content hash of %s in release %s: %w", key, releaseID, err)
	}
	return hash, true, nil
}

// PruneReleases deletes old releases that are neither live nor named by any
// run; the foreign keys cascade to their nodes and edges. A run reads its own
// release for its whole life, so a release stays while any run names it.
func (r *TopologyRepository) PruneReleases(ctx context.Context, loadedBefore time.Time) (int64, error) {
	res, err := r.conn().ExecContext(ctx, `
		DELETE FROM topology_releases rel
		WHERE rel.loaded_at < $1
		  AND NOT EXISTS (
		      SELECT 1 FROM live_release live
		      WHERE live.tenant_id = rel.tenant_id AND live.release_id = rel.release_id)
		  AND NOT EXISTS (
		      SELECT 1 FROM scheduler_tracker s
		      WHERE s.release_id = rel.release_id)`,
		loadedBefore)
	if err != nil {
		return 0, fmt.Errorf("prune topology releases: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune topology releases: %w", err)
	}
	return n, nil
}
