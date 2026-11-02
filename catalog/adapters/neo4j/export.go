package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/carolsimone/continuo/catalog/service/export"
	"github.com/carolsimone/continuo/catalog/service/export/graphschema"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// exportFetchSize is how many records an export pulls from Neo4j at a time;
// the export holds no more than that and the record it writes.
const exportFetchSize = 500

var (
	// errExportUnrestorable means a relationship of the tenant's graph joins
	// a node of another tenant (or of none) or the export lease node: the
	// export would leave it out, so the restore could not return the graph.
	errExportUnrestorable = errors.New("neo4j: the tenant's graph holds a relationship an export cannot carry")
	// errExportChanged means the records streamed disagree with the counts
	// the header was written with: the graph changed during the export.
	errExportChanged = errors.New("neo4j: the tenant's graph changed during the export")
)

// Every statement of the export and the import leaves out the tenant's export
// lease node, (:Meta {key: $lease_key}): it is neither exported nor restored.

// exportNodeCountsCypher counts the tenant's nodes per set of labels.
const exportNodeCountsCypher = `
MATCH (n {tenant_id: $tenant_id})
WHERE NOT (coalesce(n.key, '') = $lease_key AND n:Meta)
RETURN labels(n) AS labels, count(*) AS nodes`

// exportRelCountsCypher counts the relationships between the tenant's nodes
// per type.
const exportRelCountsCypher = `
MATCH (a {tenant_id: $tenant_id})-[r]->(b {tenant_id: $tenant_id})
WHERE NOT (coalesce(a.key, '') = $lease_key AND a:Meta)
  AND NOT (coalesce(b.key, '') = $lease_key AND b:Meta)
RETURN type(r) AS type, count(*) AS relationships`

// exportStrayRelsCypher counts the relationships of the tenant's nodes the
// export would leave out: those to a node of another tenant or of none, and
// those of the lease node.
const exportStrayRelsCypher = `
MATCH (a {tenant_id: $tenant_id})-[r]-(b)
WHERE b.tenant_id IS NULL OR b.tenant_id <> $tenant_id
   OR (coalesce(a.key, '') = $lease_key AND a:Meta)
   OR (coalesce(b.key, '') = $lease_key AND b:Meta)
RETURN count(DISTINCT r) AS stray`

// exportClockCypher reads the database clock, which the export lease runs on.
const exportClockCypher = `RETURN datetime() AS now`

// exportNodesCypher streams the tenant's nodes.
const exportNodesCypher = `
MATCH (n {tenant_id: $tenant_id})
WHERE NOT (coalesce(n.key, '') = $lease_key AND n:Meta)
RETURN labels(n) AS labels, properties(n) AS props`

// exportRelsFormat streams the relationships between the tenant's nodes with
// each end's labels and identity properties; %s is the projection of every
// identity property of the schema.
const exportRelsFormat = `
MATCH (a {tenant_id: $tenant_id})-[r]->(b {tenant_id: $tenant_id})
WHERE NOT (coalesce(a.key, '') = $lease_key AND a:Meta)
  AND NOT (coalesce(b.key, '') = $lease_key AND b:Meta)
RETURN type(r) AS type, labels(a) AS from_labels, a {%s} AS from_key,
       labels(b) AS to_labels, b {%s} AS to_key, properties(r) AS props`

// exportRelsCypher is exportRelsFormat with the schema's identity properties.
var exportRelsCypher = func() string {
	p := identityProjection()
	return fmt.Sprintf(exportRelsFormat, p, p)
}()

// identityProjection is a map projection of every property that identifies a
// node of some label of the schema, each once: .`unique_id`, .`key`, ….
func identityProjection() string {
	var names []string
	for _, l := range graphschema.Labels() {
		for _, p := range graphschema.Identity(l) {
			if !slices.Contains(names, p) {
				names = append(names, p)
			}
		}
	}
	parts := make([]string, 0, len(names))
	for _, p := range names {
		parts = append(parts, ".`"+p+"`")
	}
	return strings.Join(parts, ", ")
}

// graphExporter implements ports.GraphExporter on the catalog graph.
type graphExporter struct {
	client         Neo4jClient
	catalogVersion string
	logger         *slog.Logger
}

var _ ports.GraphExporter = (*graphExporter)(nil)

// NewGraphExporter returns an exporter of client's graph whose headers name
// catalogVersion.
func NewGraphExporter(client Neo4jClient, catalogVersion string, logger *slog.Logger) ports.GraphExporter {
	return &graphExporter{client: client, catalogVersion: catalogVersion, logger: logger}
}

// Export counts the tenant's nodes per label and relationships per type,
// writes the header, then streams the nodes and the relationships to out, all
// in one read transaction that pulls exportFetchSize records at a time. The
// tenant's writes are paused by its export lease, so the counts hold; a
// stream that disagrees with them fails the export. A graph the export
// cannot carry (a label or type outside the schema, a value of another type,
// a relationship to another tenant or of the lease node, a relationship end
// without its identity) fails it too, before the header when the counts
// show it.
func (e *graphExporter) Export(ctx context.Context, tenantID string, out ports.RecordWriter) error {
	if tenantID == "" {
		return errEmptyTenant
	}
	if err := e.export(ctx, tenantID, out); err != nil {
		return fmt.Errorf("neo4j: export the graph of tenant %q: %w", tenantID, err)
	}
	return nil
}

func (e *graphExporter) export(ctx context.Context, tenantID string, out ports.RecordWriter) error {
	session := e.client.NewSession(ctx, neo4j.AccessModeRead, func(c *neo4j.SessionConfig) { c.FetchSize = exportFetchSize })
	defer func() { _ = session.Close(ctx) }()
	// An explicit transaction, not a managed one: the driver retries a
	// managed transaction, and records already written cannot be unwritten.
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin read transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	run, err := NewTenantRunner(tenantID, tx)
	if err != nil {
		return err
	}

	h, err := e.header(ctx, run, tenantID)
	if err != nil {
		return err
	}
	if err := out.WriteHeader(h); err != nil {
		return fmt.Errorf("write the header: %w", err)
	}
	nodes, err := streamNodes(ctx, run, tenantID, out)
	if err != nil {
		return err
	}
	rels, err := streamRels(ctx, run, out)
	if err != nil {
		return err
	}
	if !maps.Equal(nodes, nonZero(h.Nodes)) || !maps.Equal(rels, nonZero(h.Relationships)) {
		return fmt.Errorf("%w: counted nodes %v and relationships %v, streamed %v and %v",
			errExportChanged, h.Nodes, h.Relationships, nodes, rels)
	}
	e.logger.Debug("tenant graph exported", "tenant_id", tenantID, "nodes", nodes, "relationships", rels)
	return nil
}

// header counts the tenant's graph and refuses one the export cannot carry
// before anything is written.
func (e *graphExporter) header(ctx context.Context, run TenantRunner, tenantID string) (export.Header, error) {
	params := map[string]any{"lease_key": export.LeaseNodeKey}
	stray, err := singleInt(ctx, run, exportStrayRelsCypher, params, "stray")
	if err != nil {
		return export.Header{}, fmt.Errorf("count relationships an export cannot carry: %w", err)
	}
	if stray > 0 {
		return export.Header{}, fmt.Errorf("%w: %d relationships to another tenant's node or of the export lease node", errExportUnrestorable, stray)
	}

	nodes := map[string]int64{}
	if err := eachRecord(ctx, run, exportNodeCountsCypher, params, func(rec *neo4j.Record) error {
		labels, err := stringListColumn(rec, "labels")
		if err != nil {
			return err
		}
		if _, err := (export.NodeRecord{Labels: labels}).SchemaLabels(); err != nil {
			return err
		}
		n, err := intColumn(rec, "nodes")
		if err != nil {
			return err
		}
		for _, l := range labels {
			nodes[l] += n
		}
		return nil
	}); err != nil {
		return export.Header{}, fmt.Errorf("count nodes: %w", err)
	}

	rels := map[string]int64{}
	if err := eachRecord(ctx, run, exportRelCountsCypher, params, func(rec *neo4j.Record) error {
		typ, err := stringColumn(rec, "type")
		if err != nil {
			return err
		}
		if _, err := (export.RelRecord{Type: typ}).SchemaType(); err != nil {
			return err
		}
		n, err := intColumn(rec, "relationships")
		if err != nil {
			return err
		}
		rels[typ] += n
		return nil
	}); err != nil {
		return export.Header{}, fmt.Errorf("count relationships: %w", err)
	}

	var now time.Time
	if err := eachRecord(ctx, run, exportClockCypher, nil, func(rec *neo4j.Record) error {
		v, _ := rec.Get("now")
		t, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("the database clock reads %T, want a datetime", v)
		}
		now = t.UTC()
		return nil
	}); err != nil {
		return export.Header{}, fmt.Errorf("read the database clock: %w", err)
	}

	h := export.Header{
		FormatVersion: export.FormatVersion, TenantID: tenantID, ExportedAt: now,
		CatalogVersion: e.catalogVersion, Nodes: nodes, Relationships: rels,
	}
	if err := h.Check(); err != nil {
		return export.Header{}, err
	}
	return h, nil
}

// streamNodes writes every node of the tenant to out and counts them per
// label.
func streamNodes(ctx context.Context, run TenantRunner, tenantID string, out ports.RecordWriter) (map[string]int64, error) {
	counts := map[string]int64{}
	err := eachRecord(ctx, run, exportNodesCypher, map[string]any{"lease_key": export.LeaseNodeKey}, func(rec *neo4j.Record) error {
		labels, err := stringListColumn(rec, "labels")
		if err != nil {
			return err
		}
		props, err := mapColumn(rec, "props")
		if err != nil {
			return err
		}
		n := export.NodeRecord{Labels: labels, Props: props}
		if err := n.Check(tenantID); err != nil {
			return err
		}
		if err := out.WriteNode(n); err != nil {
			return fmt.Errorf("write a node: %w", err)
		}
		for _, l := range labels {
			counts[l]++
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("stream nodes: %w", err)
	}
	return counts, nil
}

// streamRels writes every relationship between the tenant's nodes to out and
// counts them per type.
func streamRels(ctx context.Context, run TenantRunner, out ports.RecordWriter) (map[string]int64, error) {
	counts := map[string]int64{}
	err := eachRecord(ctx, run, exportRelsCypher, map[string]any{"lease_key": export.LeaseNodeKey}, func(rec *neo4j.Record) error {
		typ, err := stringColumn(rec, "type")
		if err != nil {
			return err
		}
		from, err := endRef(rec, "from_labels", "from_key")
		if err != nil {
			return fmt.Errorf("a :%s's start: %w", typ, err)
		}
		to, err := endRef(rec, "to_labels", "to_key")
		if err != nil {
			return fmt.Errorf("a :%s's end: %w", typ, err)
		}
		props, err := mapColumn(rec, "props")
		if err != nil {
			return err
		}
		r := export.RelRecord{Type: typ, From: from, To: to, Props: props}
		if err := r.Check(); err != nil {
			return err
		}
		if err := out.WriteRel(r); err != nil {
			return fmt.Errorf("write a relationship: %w", err)
		}
		counts[typ]++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("stream relationships: %w", err)
	}
	return counts, nil
}

// endRef names a relationship end by the first of its labels in the
// schema's order and that label's identity properties.
func endRef(rec *neo4j.Record, labelsKey, keyKey string) (export.NodeRef, error) {
	labels, err := stringListColumn(rec, labelsKey)
	if err != nil {
		return export.NodeRef{}, err
	}
	keyProps, err := mapColumn(rec, keyKey)
	if err != nil {
		return export.NodeRef{}, err
	}
	end := export.NodeRecord{Labels: labels, Props: keyProps}
	schemaLabels, err := end.SchemaLabels()
	if err != nil {
		return export.NodeRef{}, err
	}
	label := schemaLabels[0]
	key, ok := end.Identity(label)
	if !ok {
		return export.NodeRef{}, fmt.Errorf("%w: a :%s without its identity %v", export.ErrInvalidRecord, label, graphschema.Identity(label))
	}
	return export.NodeRef{Label: string(label), Key: key}, nil
}

// eachRecord runs a statement and hands each record to fn as the driver
// pulls it.
func eachRecord(ctx context.Context, run TenantRunner, cypher string, params map[string]any, fn func(*neo4j.Record) error) error {
	res, err := run.Run(ctx, cypher, params)
	if err != nil {
		return err
	}
	for res.Next(ctx) {
		if err := fn(res.Record()); err != nil {
			return err
		}
	}
	return res.Err()
}

// singleInt runs a statement that returns one row and reads the integer in
// column key of it.
func singleInt(ctx context.Context, run TenantRunner, cypher string, params map[string]any, key string) (int64, error) {
	res, err := run.Run(ctx, cypher, params)
	if err != nil {
		return 0, err
	}
	rec, err := res.Single(ctx)
	if err != nil {
		return 0, err
	}
	return intColumn(rec, key)
}

func intColumn(rec *neo4j.Record, key string) (int64, error) {
	v, _ := rec.Get(key)
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("column %q is %T, want an integer", key, v)
	}
	return n, nil
}

func stringColumn(rec *neo4j.Record, key string) (string, error) {
	v, _ := rec.Get(key)
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("column %q is %T, want a string", key, v)
	}
	return s, nil
}

func mapColumn(rec *neo4j.Record, key string) (map[string]any, error) {
	v, _ := rec.Get(key)
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("column %q is %T, want a map", key, v)
	}
	return m, nil
}

func stringListColumn(rec *neo4j.Record, key string) ([]string, error) {
	v, _ := rec.Get(key)
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("column %q is %T, want a list", key, v)
	}
	out := make([]string, 0, len(list))
	for _, e := range list {
		s, ok := e.(string)
		if !ok {
			return nil, fmt.Errorf("column %q holds %T, want strings", key, e)
		}
		out = append(out, s)
	}
	return out, nil
}

// nonZero returns counts without its zero entries, as a stream counts them.
func nonZero(counts map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(counts))
	for k, n := range counts {
		if n != 0 {
			out[k] = n
		}
	}
	return out
}
