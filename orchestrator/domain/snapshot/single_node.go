package snapshot

import (
	"context"
	"fmt"

	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// SingleNode is the Feature 4 selector — produces exactly one TaskProjection
// for the identified target node. Two modes:
//
//   - "latest": metadata pair from latest topology (:Table node properties).
//     Used by single-node-run "latest" mode (the default user-triggered path).
//
//   - "snapshot_of_run": metadata pair from source :Run's :EXECUTES edge.
//     Used by single-node-run "stale" mode (re-run a node at the metadata
//     pinned in a specific historical run).
//
// Returns ErrTargetNotFound if:
//   - latest mode: no :Table node with the requested identity exists (or the
//     table is inactive).
//   - snapshot_of_run mode: source :Run doesn't exist OR has no :EXECUTES
//     edge to a :Table with the requested identity.
type SingleNode struct {
	ServiceName    string
	SchemaName     string
	TableName      string
	MetadataSource string // "latest" | "snapshot_of_run"
}

func (s SingleNode) SelectTasks(ctx context.Context, tx neo4j.ManagedTransaction, p Params) ([]TaskProjection, error) {
	if s.ServiceName == "" || s.SchemaName == "" || s.TableName == "" {
		return nil, fmt.Errorf("SingleNode: service_name, schema_name, and table_name are required")
	}
	switch s.MetadataSource {
	case "latest":
		return s.selectLatest(ctx, tx)
	case "snapshot_of_run":
		if p.SourceRunID == nil {
			return nil, fmt.Errorf("SingleNode: snapshot_of_run mode requires SourceRunID")
		}
		return s.selectFromSourceRun(ctx, tx, p.SourceRunID.String())
	default:
		return nil, fmt.Errorf("SingleNode: invalid MetadataSource %q (want 'latest' or 'snapshot_of_run')", s.MetadataSource)
	}
}

func (s SingleNode) selectLatest(ctx context.Context, tx neo4j.ManagedTransaction) ([]TaskProjection, error) {
	const q = `
		MATCH (tbl:Table {service_name: $svc, schema_name: $schema, table_name: $tbl})
		WHERE COALESCE(tbl.active, true)
		RETURN tbl.schedule_name AS schedule_name,
		       COALESCE(tbl.node_type, 'dbt-model') AS node_type,
		       COALESCE(tbl.image_tag, '')          AS image_tag,
		       COALESCE(tbl.manifest_version, '')   AS manifest_version
		LIMIT 1
	`
	result, err := tx.Run(ctx, q, map[string]interface{}{
		"svc": s.ServiceName, "schema": s.SchemaName, "tbl": s.TableName,
	})
	if err != nil {
		return nil, fmt.Errorf("SingleNode latest: %w", err)
	}
	if !result.Next(ctx) {
		if rerr := result.Err(); rerr != nil {
			return nil, fmt.Errorf("SingleNode latest result: %w", rerr)
		}
		return nil, ErrTargetNotFound
	}
	rec := result.Record()
	return []TaskProjection{{
		TaskID:          uuid.New(),
		ServiceName:     s.ServiceName,
		SchemaName:      s.SchemaName,
		TableName:       s.TableName,
		ScheduleName:    stringField(rec, "schedule_name"),
		NodeType:        stringField(rec, "node_type"),
		InitialStatus:   "PENDING",
		ImageTag:        stringField(rec, "image_tag"),
		ManifestVersion: stringField(rec, "manifest_version"),
		MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
	}}, nil
}

func (s SingleNode) selectFromSourceRun(ctx context.Context, tx neo4j.ManagedTransaction, sourceRunID string) ([]TaskProjection, error) {
	const q = `
		MATCH (src:Run {run_id: $source_run_id})-[srcEdge:EXECUTES]->
		      (tbl:Table {service_name: $svc, schema_name: $schema, table_name: $tbl})
		RETURN tbl.schedule_name AS schedule_name,
		       COALESCE(tbl.node_type, 'dbt-model') AS node_type,
		       COALESCE(srcEdge.image_tag, '')        AS image_tag,
		       COALESCE(srcEdge.manifest_version, '') AS manifest_version
		LIMIT 1
	`
	result, err := tx.Run(ctx, q, map[string]interface{}{
		"source_run_id": sourceRunID,
		"svc":           s.ServiceName, "schema": s.SchemaName, "tbl": s.TableName,
	})
	if err != nil {
		return nil, fmt.Errorf("SingleNode stale: %w", err)
	}
	if !result.Next(ctx) {
		if rerr := result.Err(); rerr != nil {
			return nil, fmt.Errorf("SingleNode stale result: %w", rerr)
		}
		return nil, ErrTargetNotFound
	}
	rec := result.Record()
	return []TaskProjection{{
		TaskID:          uuid.New(),
		ServiceName:     s.ServiceName,
		SchemaName:      s.SchemaName,
		TableName:       s.TableName,
		ScheduleName:    stringField(rec, "schedule_name"),
		NodeType:        stringField(rec, "node_type"),
		InitialStatus:   "PENDING",
		ImageTag:        stringField(rec, "image_tag"),
		ManifestVersion: stringField(rec, "manifest_version"),
		MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
	}}, nil
}
