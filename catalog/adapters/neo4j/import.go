package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"strings"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/domain/graphschema"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// importBatchRows is how many nodes, or relationships, one import
// transaction creates.
const importBatchRows = 500

// wipeBatchRows is how many nodes each inner transaction of a wipe deletes.
const wipeBatchRows = 1000

var (
	// errImportTenantNotEmpty means the tenant holds nodes besides its lease
	// node, so an import would mix them with the export's.
	errImportTenantNotEmpty = errors.New("neo4j: the tenant holds nodes besides its export lease node; wipe it before an import")
	// errImportOutOfOrder means a node came after the first relationship.
	errImportOutOfOrder = errors.New("neo4j: a node after the relationships")
	// errImportUnresolvedEnd means a relationship's ends did not name exactly
	// one node each among the nodes the import loaded.
	errImportUnresolvedEnd = errors.New("neo4j: a relationship end names no single node of the import")
	// errImportCountMismatch means the records read, or the graph loaded,
	// disagree with the export's counts.
	errImportCountMismatch = errors.New("neo4j: the import's counts disagree with the export")
)

// wipeCypher deletes the tenant's nodes but its lease node, with their
// relationships, committing every $batch_rows nodes.
const wipeCypher = `
MATCH (n {tenant_id: $tenant_id})
WHERE NOT (coalesce(n.key, '') = $lease_key AND n:Meta)
CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF $batch_rows ROWS`

// tenantHoldsNodesCypher reports whether the tenant holds a node besides its
// lease node.
const tenantHoldsNodesCypher = `
MATCH (n {tenant_id: $tenant_id})
WHERE NOT (coalesce(n.key, '') = $lease_key AND n:Meta)
WITH n LIMIT 1
RETURN count(n) > 0 AS found`

// createNodesFormat creates one batch of nodes that share their labels; %s
// is those labels, the schema's constants joined by ':'. Each row is a
// node's properties without tenant_id, which comes from the tenant runner.
const createNodesFormat = `
UNWIND $rows AS row
CREATE (n:%s {tenant_id: $tenant_id})
SET n += row
RETURN count(n) AS created`

// createRelsFormat creates one batch of relationships of one type between
// ends of one label each. The verbs are, in order: the start's label, its
// identity properties matched to row.from, the end's label, its identity
// properties matched to row.to, and the type, all from the schema.
const createRelsFormat = `
UNWIND $rows AS row
MATCH (a:%s {tenant_id: $tenant_id, %s})
MATCH (b:%s {tenant_id: $tenant_id, %s})
CREATE (a)-[r:%s]->(b)
SET r += row.props
RETURN count(r) AS created`

// createNodesCypher returns the statement that creates nodes with labels,
// which are the schema's own constants.
func createNodesCypher(labels []graphschema.Label) string {
	names := make([]string, 0, len(labels))
	for _, l := range labels {
		names = append(names, string(l))
	}
	return fmt.Sprintf(createNodesFormat, strings.Join(names, ":"))
}

// createRelsCypher returns the statement that creates relationships of type
// typ from a node of label from to a node of label to.
func createRelsCypher(typ graphschema.RelType, from, to graphschema.Label) string {
	return fmt.Sprintf(createRelsFormat, from, identityMatch(from, "row.from"), to, identityMatch(to, "row.to"), typ)
}

// identityMatch matches label's identity properties to the elements of the
// list keys, in graphschema.Identity order: `unique_id`: row.from[0], ….
func identityMatch(label graphschema.Label, keys string) string {
	props := graphschema.Identity(label)
	parts := make([]string, 0, len(props))
	for i, p := range props {
		parts = append(parts, fmt.Sprintf("`%s`: %s[%d]", p, keys, i))
	}
	return strings.Join(parts, ", ")
}

// graphImporter implements ports.GraphImporter on the catalog graph.
type graphImporter struct {
	client Neo4jClient
	logger *slog.Logger
}

var _ ports.GraphImporter = (*graphImporter)(nil)

// NewGraphImporter returns an importer into client's graph.
func NewGraphImporter(client Neo4jClient, logger *slog.Logger) ports.GraphImporter {
	return &graphImporter{client: client, logger: logger}
}

// autoCommit runs each statement as an auto-commit transaction of its
// session, the only kind in which Neo4j runs CALL { … } IN TRANSACTIONS.
type autoCommit struct{ session neo4j.SessionWithContext }

func (a autoCommit) Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error) {
	return a.session.Run(ctx, cypher, params)
}

// Wipe deletes every node of the tenant but its export lease node, with its
// relationships, in batches that each commit.
func (i *graphImporter) Wipe(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return errEmptyTenant
	}
	session := i.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()
	run, err := NewTenantRunner(tenantID, autoCommit{session: session})
	if err != nil {
		return err
	}
	res, err := run.Run(ctx, wipeCypher, map[string]any{"lease_key": export.LeaseNodeKey, "batch_rows": wipeBatchRows})
	if err != nil {
		return fmt.Errorf("neo4j: wipe the graph of tenant %q: %w", tenantID, err)
	}
	summary, err := res.Consume(ctx)
	if err != nil {
		return fmt.Errorf("neo4j: wipe the graph of tenant %q: %w", tenantID, err)
	}
	i.logger.Info("tenant graph wiped", "tenant_id", tenantID,
		"nodes_deleted", summary.Counters().NodesDeleted(), "relationships_deleted", summary.Counters().RelationshipsDeleted())
	return nil
}

// Import loads in into the tenant's wiped graph: its nodes in batches of
// importBatchRows, then its relationships, each end matched by its label and
// identity within the tenant. Every record is checked against the schema and
// the tenant before any statement is built from it, and the statements are
// built from the schema's constants, never from the record's text. An export
// lease node in the export is not loaded. Import then checks that the records
// matched the header's counts and that the graph holds exactly what it
// loaded, and ends with the startup migration (InitSchema), which brings an
// export written by an older catalog to the current shape. A failure leaves
// what earlier batches loaded; a restore wipes again before it retries.
func (i *graphImporter) Import(ctx context.Context, tenantID string, in ports.RecordReader) error {
	if tenantID == "" {
		return errEmptyTenant
	}
	loaded, err := i.load(ctx, tenantID, in)
	if err != nil {
		return fmt.Errorf("neo4j: import the graph of tenant %q: %w", tenantID, err)
	}
	if err := InitSchema(ctx, i.client, i.logger); err != nil {
		return fmt.Errorf("neo4j: import the graph of tenant %q: %w", tenantID, err)
	}
	i.logger.Info("tenant graph imported", "tenant_id", tenantID, "nodes", loaded.nodes, "relationships", loaded.rels)
	return nil
}

// importCounts counts records per label and per relationship type.
type importCounts struct {
	nodes map[string]int64
	rels  map[string]int64
}

func newImportCounts() importCounts {
	return importCounts{nodes: map[string]int64{}, rels: map[string]int64{}}
}

func (i *graphImporter) load(ctx context.Context, tenantID string, in ports.RecordReader) (importCounts, error) {
	h := in.Header()
	if err := h.Check(); err != nil {
		return importCounts{}, err
	}
	if h.TenantID != tenantID {
		return importCounts{}, fmt.Errorf("the export is of tenant %q: %w", h.TenantID, export.ErrTenantMismatch)
	}

	session := i.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()
	l := &importLoader{
		ctx: ctx, session: session, tenantID: tenantID,
		nodeBatches: map[string]*nodeBatch{}, relBatches: map[relGroup][]any{},
		read: newImportCounts(), loaded: newImportCounts(),
	}
	var found bool
	if err := l.inTx(false, func(run TenantRunner) (err error) {
		found, err = singleBool(ctx, run, tenantHoldsNodesCypher, map[string]any{"lease_key": export.LeaseNodeKey}, "found")
		return err
	}); err != nil {
		return importCounts{}, fmt.Errorf("check the tenant holds no nodes: %w", err)
	}
	if found {
		return importCounts{}, errImportTenantNotEmpty
	}

	for {
		rec, err := in.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return importCounts{}, fmt.Errorf("read the export: %w", err)
		}
		switch x := rec.(type) {
		case export.NodeRecord:
			err = l.node(x)
		case export.RelRecord:
			err = l.rel(x)
		default:
			err = fmt.Errorf("%w: a record of type %T", export.ErrInvalidRecord, rec)
		}
		if err != nil {
			return importCounts{}, err
		}
	}
	if !maps.Equal(l.read.nodes, nonZero(h.Nodes)) || !maps.Equal(l.read.rels, nonZero(h.Relationships)) {
		return importCounts{}, fmt.Errorf("%w: the header counts nodes %v and relationships %v, the export holds %v and %v",
			errImportCountMismatch, h.Nodes, h.Relationships, l.read.nodes, l.read.rels)
	}
	if err := l.flushNodes(); err != nil {
		return importCounts{}, err
	}
	if err := l.flushRels(); err != nil {
		return importCounts{}, err
	}
	if err := l.checkGraph(); err != nil {
		return importCounts{}, err
	}
	return l.loaded, nil
}

// nodeBatch is the pending nodes of one set of labels.
type nodeBatch struct {
	labels []graphschema.Label
	rows   []any
}

// relGroup is what one relationship statement is built from.
type relGroup struct {
	typ      graphschema.RelType
	from, to graphschema.Label
}

// importLoader batches an import's records and creates them.
type importLoader struct {
	ctx         context.Context
	session     neo4j.SessionWithContext
	tenantID    string
	nodeBatches map[string]*nodeBatch
	relBatches  map[relGroup][]any
	inRels      bool
	read        importCounts
	loaded      importCounts
}

// node checks a node record and queues it for its labels' batch. It does not
// queue the export lease node.
func (l *importLoader) node(n export.NodeRecord) error {
	if l.inRels {
		return errImportOutOfOrder
	}
	if err := n.Check(l.tenantID); err != nil {
		return err
	}
	labels, err := n.SchemaLabels()
	if err != nil {
		return err
	}
	for _, label := range labels {
		l.read.nodes[string(label)]++
	}
	if n.IsLease() {
		return nil
	}
	row := make(map[string]any, len(n.Props))
	for k, v := range n.Props {
		if k != export.TenantProperty {
			row[k] = v
		}
	}
	key := createNodesCypher(labels)
	b, ok := l.nodeBatches[key]
	if !ok {
		b = &nodeBatch{labels: labels}
		l.nodeBatches[key] = b
	}
	b.rows = append(b.rows, row)
	for _, label := range labels {
		l.loaded.nodes[string(label)]++
	}
	if len(b.rows) >= importBatchRows {
		return l.createNodes(key, b)
	}
	return nil
}

// rel checks a relationship record and queues it for its group's batch. The
// first relationship flushes every pending node, so each end exists before a
// relationship names it.
func (l *importLoader) rel(r export.RelRecord) error {
	if err := r.Check(); err != nil {
		return err
	}
	typ, err := r.SchemaType()
	if err != nil {
		return err
	}
	from, err := r.From.SchemaLabel()
	if err != nil {
		return err
	}
	to, err := r.To.SchemaLabel()
	if err != nil {
		return err
	}
	if !l.inRels {
		if err := l.flushNodes(); err != nil {
			return err
		}
		l.inRels = true
	}
	l.read.rels[string(typ)]++
	l.loaded.rels[string(typ)]++
	g := relGroup{typ: typ, from: from, to: to}
	props := r.Props
	if props == nil {
		props = map[string]any{}
	}
	l.relBatches[g] = append(l.relBatches[g], map[string]any{"from": r.From.Key, "to": r.To.Key, "props": props})
	if len(l.relBatches[g]) >= importBatchRows {
		return l.createRels(g)
	}
	return nil
}

func (l *importLoader) flushNodes() error {
	for key, b := range l.nodeBatches {
		if err := l.createNodes(key, b); err != nil {
			return err
		}
	}
	return nil
}

func (l *importLoader) flushRels() error {
	for g := range l.relBatches {
		if err := l.createRels(g); err != nil {
			return err
		}
	}
	return nil
}

// createNodes creates a batch's nodes in one transaction and empties it.
func (l *importLoader) createNodes(key string, b *nodeBatch) error {
	if len(b.rows) == 0 {
		return nil
	}
	created, err := l.create(key, b.rows)
	if err != nil {
		return fmt.Errorf("create %d nodes %v: %w", len(b.rows), b.labels, err)
	}
	if created != int64(len(b.rows)) {
		return fmt.Errorf("%w: created %d of %d nodes %v", errImportCountMismatch, created, len(b.rows), b.labels)
	}
	b.rows = b.rows[:0]
	return nil
}

// createRels creates a group's relationships in one transaction and empties
// it. Each must find exactly its two ends.
func (l *importLoader) createRels(g relGroup) error {
	rows := l.relBatches[g]
	if len(rows) == 0 {
		return nil
	}
	created, err := l.create(createRelsCypher(g.typ, g.from, g.to), rows)
	if err != nil {
		return fmt.Errorf("create %d :%s relationships: %w", len(rows), g.typ, err)
	}
	if created != int64(len(rows)) {
		return fmt.Errorf("%w: %d :%s relationships from a :%s to a :%s created %d",
			errImportUnresolvedEnd, len(rows), g.typ, g.from, g.to, created)
	}
	delete(l.relBatches, g)
	return nil
}

// create runs a create statement over rows in one transaction and returns
// how many it created.
func (l *importLoader) create(cypher string, rows []any) (int64, error) {
	var created int64
	err := l.inTx(true, func(run TenantRunner) (err error) {
		created, err = singleInt(l.ctx, run, cypher, map[string]any{"rows": rows}, "created")
		return err
	})
	return created, err
}

// checkGraph compares the tenant's graph, its lease node aside, with what the
// import loaded.
func (l *importLoader) checkGraph() error {
	params := map[string]any{"lease_key": export.LeaseNodeKey}
	inGraph := newImportCounts()
	err := l.inTx(false, func(run TenantRunner) error {
		if err := eachRecord(l.ctx, run, exportNodeCountsCypher, params, func(rec *neo4j.Record) error {
			labels, err := stringListColumn(rec, "labels")
			if err != nil {
				return err
			}
			n, err := intColumn(rec, "nodes")
			if err != nil {
				return err
			}
			for _, label := range labels {
				inGraph.nodes[label] += n
			}
			return nil
		}); err != nil {
			return err
		}
		return eachRecord(l.ctx, run, exportRelCountsCypher, params, func(rec *neo4j.Record) error {
			typ, err := stringColumn(rec, "type")
			if err != nil {
				return err
			}
			n, err := intColumn(rec, "relationships")
			if err != nil {
				return err
			}
			inGraph.rels[typ] += n
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("count the loaded graph: %w", err)
	}
	if !maps.Equal(inGraph.nodes, l.loaded.nodes) || !maps.Equal(inGraph.rels, l.loaded.rels) {
		return fmt.Errorf("%w: loaded nodes %v and relationships %v, the graph holds %v and %v",
			errImportCountMismatch, l.loaded.nodes, l.loaded.rels, inGraph.nodes, inGraph.rels)
	}
	return nil
}

// inTx runs body in one explicit transaction of the loader's session, bound
// to its tenant, and commits it when commit is true and body succeeds. It
// does not retry.
func (l *importLoader) inTx(commit bool, body func(run TenantRunner) error) error {
	tx, err := l.session.BeginTransaction(l.ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(l.ctx) //nolint:errcheck
	run, err := NewTenantRunner(l.tenantID, tx)
	if err != nil {
		return err
	}
	if err := body(run); err != nil {
		return err
	}
	if commit {
		if err := tx.Commit(l.ctx); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
	return nil
}
