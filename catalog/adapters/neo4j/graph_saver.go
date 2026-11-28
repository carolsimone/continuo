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

	"github.com/carolsimone/continuo/catalog/service/graphbackup"
	"github.com/carolsimone/continuo/catalog/service/graphbackup/graphschema"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// backupFetchSize is how many records a backup pulls from Neo4j at a time;
// the backup holds no more than that and the record it writes.
const backupFetchSize = 500

var (
	// errBackupUnrestorable means a relationship of the tenant's graph joins
	// a node of another tenant (or of none) or the backup lease node: the
	// backup would leave it out, so the restore could not return the graph.
	errBackupUnrestorable = errors.New("neo4j: the tenant's graph holds a relationship a backup cannot carry")
	// errBackupChanged means the records streamed disagree with the counts
	// the header was written with: the graph changed during the backup.
	errBackupChanged = errors.New("neo4j: the tenant's graph changed during the backup")
)

// Every statement of the backup and the restore leaves out the tenant's backup
// lease node, (:Meta {key: $lease_key}): it is neither backed up nor restored.

// backupNodeCountsCypher counts the tenant's nodes per set of labels.
const backupNodeCountsCypher = `
MATCH (n {tenant_id: $tenant_id})
WHERE NOT (coalesce(n.key, '') = $lease_key AND n:Meta)
RETURN labels(n) AS labels, count(*) AS nodes`

// backupRelCountsCypher counts the relationships between the tenant's nodes
// per type.
const backupRelCountsCypher = `
MATCH (a {tenant_id: $tenant_id})-[r]->(b {tenant_id: $tenant_id})
WHERE NOT (coalesce(a.key, '') = $lease_key AND a:Meta)
  AND NOT (coalesce(b.key, '') = $lease_key AND b:Meta)
RETURN type(r) AS type, count(*) AS relationships`

// backupStrayRelsCypher counts the relationships of the tenant's nodes the
// backup would leave out: those to a node of another tenant or of none, and
// those of the lease node.
const backupStrayRelsCypher = `
MATCH (a {tenant_id: $tenant_id})-[r]-(b)
WHERE b.tenant_id IS NULL OR b.tenant_id <> $tenant_id
   OR (coalesce(a.key, '') = $lease_key AND a:Meta)
   OR (coalesce(b.key, '') = $lease_key AND b:Meta)
RETURN count(DISTINCT r) AS stray`

// backupClockCypher reads the database clock, which the backup lease runs on.
const backupClockCypher = `RETURN datetime() AS now`

// backupNodesCypher streams the tenant's nodes.
const backupNodesCypher = `
MATCH (n {tenant_id: $tenant_id})
WHERE NOT (coalesce(n.key, '') = $lease_key AND n:Meta)
RETURN labels(n) AS labels, properties(n) AS props`

// backupRelsFormat streams the relationships between the tenant's nodes with
// each end's labels and identity properties; %s is the projection of every
// identity property of the schema.
const backupRelsFormat = `
MATCH (a {tenant_id: $tenant_id})-[r]->(b {tenant_id: $tenant_id})
WHERE NOT (coalesce(a.key, '') = $lease_key AND a:Meta)
  AND NOT (coalesce(b.key, '') = $lease_key AND b:Meta)
RETURN type(r) AS type, labels(a) AS from_labels, a {%s} AS from_key,
       labels(b) AS to_labels, b {%s} AS to_key, properties(r) AS props`

// backupRelsCypher is backupRelsFormat with the schema's identity properties.
var backupRelsCypher = func() string {
	p := identityProjection()
	return fmt.Sprintf(backupRelsFormat, p, p)
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

// graphSaver implements ports.GraphSaver on the catalog graph.
type graphSaver struct {
	client         Neo4jClient
	catalogVersion string
	logger         *slog.Logger
}

var _ ports.GraphSaver = (*graphSaver)(nil)

// NewGraphSaver returns a saver of client's graph whose headers name
// catalogVersion.
func NewGraphSaver(client Neo4jClient, catalogVersion string, logger *slog.Logger) ports.GraphSaver {
	return &graphSaver{client: client, catalogVersion: catalogVersion, logger: logger}
}

// Backup counts the tenant's nodes per label and relationships per type,
// writes the header, then streams the nodes and the relationships to out, all
// in one read transaction that pulls backupFetchSize records at a time. The
// tenant's writes are paused by its backup lease, so the counts hold; a
// stream that disagrees with them fails the backup. A graph the backup
// cannot carry (a label or type outside the schema, a value of another type,
// a relationship to another tenant or of the lease node, a relationship end
// without its identity) fails it too, before the header when the counts
// show it.
func (e *graphSaver) Save(ctx context.Context, tenantID string, out ports.RecordWriter) error {
	if tenantID == "" {
		return errEmptyTenant
	}
	if err := e.save(ctx, tenantID, out); err != nil {
		return fmt.Errorf("neo4j: save the graph of tenant %q: %w", tenantID, err)
	}
	return nil
}

func (e *graphSaver) save(ctx context.Context, tenantID string, out ports.RecordWriter) error {
	session := e.client.NewSession(ctx, neo4j.AccessModeRead, func(c *neo4j.SessionConfig) { c.FetchSize = backupFetchSize })
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
			errBackupChanged, h.Nodes, h.Relationships, nodes, rels)
	}
	e.logger.Debug("tenant graph backed up", "tenant_id", tenantID, "nodes", nodes, "relationships", rels)
	return nil
}

// header counts the tenant's graph and refuses one the backup cannot carry
// before anything is written.
func (e *graphSaver) header(ctx context.Context, run TenantRunner, tenantID string) (graphbackup.Header, error) {
	params := map[string]any{"lease_key": graphbackup.LeaseNodeKey}
	stray, err := singleInt(ctx, run, backupStrayRelsCypher, params, "stray")
	if err != nil {
		return graphbackup.Header{}, fmt.Errorf("count relationships a backup cannot carry: %w", err)
	}
	if stray > 0 {
		return graphbackup.Header{}, fmt.Errorf("%w: %d relationships to another tenant's node or of the backup lease node", errBackupUnrestorable, stray)
	}

	nodes := map[string]int64{}
	if err := eachRecord(ctx, run, backupNodeCountsCypher, params, func(rec *neo4j.Record) error {
		labels, err := stringListColumn(rec, "labels")
		if err != nil {
			return err
		}
		if _, err := (graphbackup.NodeRecord{Labels: labels}).SchemaLabels(); err != nil {
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
		return graphbackup.Header{}, fmt.Errorf("count nodes: %w", err)
	}

	rels := map[string]int64{}
	if err := eachRecord(ctx, run, backupRelCountsCypher, params, func(rec *neo4j.Record) error {
		typ, err := stringColumn(rec, "type")
		if err != nil {
			return err
		}
		if _, err := (graphbackup.RelRecord{Type: typ}).SchemaType(); err != nil {
			return err
		}
		n, err := intColumn(rec, "relationships")
		if err != nil {
			return err
		}
		rels[typ] += n
		return nil
	}); err != nil {
		return graphbackup.Header{}, fmt.Errorf("count relationships: %w", err)
	}

	var now time.Time
	if err := eachRecord(ctx, run, backupClockCypher, nil, func(rec *neo4j.Record) error {
		v, _ := rec.Get("now")
		t, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("the database clock reads %T, want a datetime", v)
		}
		now = t.UTC()
		return nil
	}); err != nil {
		return graphbackup.Header{}, fmt.Errorf("read the database clock: %w", err)
	}

	h := graphbackup.Header{
		FormatVersion: graphbackup.FormatVersion, TenantID: tenantID, TakenAt: now,
		CatalogVersion: e.catalogVersion, Nodes: nodes, Relationships: rels,
	}
	if err := h.Check(); err != nil {
		return graphbackup.Header{}, err
	}
	return h, nil
}

// streamNodes writes every node of the tenant to out and counts them per
// label.
func streamNodes(ctx context.Context, run TenantRunner, tenantID string, out ports.RecordWriter) (map[string]int64, error) {
	counts := map[string]int64{}
	err := eachRecord(ctx, run, backupNodesCypher, map[string]any{"lease_key": graphbackup.LeaseNodeKey}, func(rec *neo4j.Record) error {
		labels, err := stringListColumn(rec, "labels")
		if err != nil {
			return err
		}
		props, err := mapColumn(rec, "props")
		if err != nil {
			return err
		}
		n := graphbackup.NodeRecord{Labels: labels, Props: props}
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
	err := eachRecord(ctx, run, backupRelsCypher, map[string]any{"lease_key": graphbackup.LeaseNodeKey}, func(rec *neo4j.Record) error {
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
		r := graphbackup.RelRecord{Type: typ, From: from, To: to, Props: props}
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
func endRef(rec *neo4j.Record, labelsKey, keyKey string) (graphbackup.NodeRef, error) {
	labels, err := stringListColumn(rec, labelsKey)
	if err != nil {
		return graphbackup.NodeRef{}, err
	}
	keyProps, err := mapColumn(rec, keyKey)
	if err != nil {
		return graphbackup.NodeRef{}, err
	}
	end := graphbackup.NodeRecord{Labels: labels, Props: keyProps}
	schemaLabels, err := end.SchemaLabels()
	if err != nil {
		return graphbackup.NodeRef{}, err
	}
	label := schemaLabels[0]
	key, ok := end.Identity(label)
	if !ok {
		return graphbackup.NodeRef{}, fmt.Errorf("%w: a :%s without its identity %v", graphbackup.ErrInvalidRecord, label, graphschema.Identity(label))
	}
	return graphbackup.NodeRef{Label: string(label), Key: key}, nil
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
