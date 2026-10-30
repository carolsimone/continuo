package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strings"

	"github.com/carolsimone/continuo/catalog/domain/graphschema"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// InitSchema brings the graph to the catalog's shape and creates its
// constraints and indexes, before any consumer or the gRPC server starts. It
// runs the steps of startupMigration in order and returns the first failure,
// so the service refuses to start rather than serve a graph halfway there.
//
// Every step is idempotent: it acts only on what it has not brought to the
// catalog's shape yet, so a second InitSchema changes nothing, and a run that
// stopped anywhere, even inside a batched step, resumes at the next start.
// A batched step runs each statement as one auto-commit transaction whose
// inner transactions commit every migrationBatchRows rows (CALL { … } IN
// TRANSACTIONS).
func InitSchema(ctx context.Context, client Neo4jClient, logger *slog.Logger) error {
	return runMigration(ctx, client, logger, startupMigration)
}

// migrationBatchRows is how many rows each inner transaction of a batched
// migration statement commits.
const migrationBatchRows = 1000

// migrationStep is one step of the startup migration.
type migrationStep struct {
	name string
	run  func(ctx context.Context, session neo4j.SessionWithContext, changes *migrationChanges) error
}

// startupMigration is the steps InitSchema runs, in order.
//
//  1. Delete the run history (:Run, with its :EXECUTES) and :TopologyRoot.
//  2. Relabel every :Table as :Node.
//  3. Put every node of a graphschema label that has no tenant in the default
//     tenant.
//  4. Give every :Node and :CodeUnit with no code_version_promotion_seq the
//     live promotion_seq of its tenant as its version watermark, and remove
//     code_version_promoted_at, so a promotion newer than the live one moves
//     :CURRENT and a redelivered older one cannot.
//  5. Drop every uniqueness constraint and range index whose properties lack
//     tenant_id or whose label is :Table or :Run, whatever its name: CREATE
//     … IF NOT EXISTS keeps an object of the same name whatever its
//     definition, so an old definition under a current name must go first.
//  6. Create the catalog's constraints and indexes and wait until they are
//     online.
//  7. Delete the outgoing :DEPENDS_ON of every retired :Node: those edges
//     describe the live topology, and a retired node keeps its history in
//     its versions, failures and fix PRs.
var startupMigration = []migrationStep{
	{"delete run history and the topology root", deleteRetiredLabels},
	{"relabel :Table as :Node", relabelTables},
	{"set the default tenant", setDefaultTenant},
	{"set the version watermark", setVersionWatermark},
	{"drop constraints and indexes without a tenant", dropUntenantedSchema},
	{"create constraints and indexes", createSchema},
	{"delete retired nodes' dependencies", deleteRetiredDependencies},
}

// runMigration runs steps in order on one session, logging what each changed.
func runMigration(ctx context.Context, client Neo4jClient, logger *slog.Logger, steps []migrationStep) error {
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	for i, step := range steps {
		var changes migrationChanges
		if err := step.run(ctx, session, &changes); err != nil {
			return fmt.Errorf("InitSchema: step %d, %s: %w", i+1, step.name, err)
		}
		logger.Info("Neo4j startup migration step done",
			append([]any{"step", i + 1, "name", step.name}, changes.logAttrs()...)...)
	}
	logger.Info("Neo4j startup migration finished", "steps", len(steps))
	return nil
}

// deleteRunsCypher and deleteTopologyRootsCypher delete the run history and
// the topology root, with every relationship they have.
const (
	deleteRunsCypher          = `MATCH (n:Run) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF $batch_rows ROWS`
	deleteTopologyRootsCypher = `MATCH (n:TopologyRoot) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF $batch_rows ROWS`
)

func deleteRetiredLabels(ctx context.Context, session neo4j.SessionWithContext, changes *migrationChanges) error {
	for _, stmt := range []string{deleteRunsCypher, deleteTopologyRootsCypher} {
		if err := runBatched(ctx, session, stmt, nil, changes); err != nil {
			return err
		}
	}
	return nil
}

// relabelTablesCypher moves every :Table to :Node, keeping its properties and
// relationships.
const relabelTablesCypher = `MATCH (n:Table) CALL { WITH n SET n:Node REMOVE n:Table } IN TRANSACTIONS OF $batch_rows ROWS`

func relabelTables(ctx context.Context, session neo4j.SessionWithContext, changes *migrationChanges) error {
	return runBatched(ctx, session, relabelTablesCypher, nil, changes)
}

// setDefaultTenantCypher puts the nodes of one label that have no tenant in
// the default tenant; %s is the label.
const setDefaultTenantCypher = `MATCH (n:%s) WHERE n.tenant_id IS NULL CALL { WITH n SET n.tenant_id = $tenant_id } IN TRANSACTIONS OF $batch_rows ROWS`

func setDefaultTenant(ctx context.Context, session neo4j.SessionWithContext, changes *migrationChanges) error {
	for _, label := range graphschema.Labels() {
		stmt := fmt.Sprintf(setDefaultTenantCypher, label)
		if err := runBatched(ctx, session, stmt, map[string]any{"tenant_id": events.DefaultTenantID}, changes); err != nil {
			return fmt.Errorf(":%s: %w", label, err)
		}
	}
	return nil
}

// setVersionWatermarkCypher gives the nodes of one label still on the
// code_version_promoted_at watermark, or with none, the promotion_seq of their
// tenant's live release; %s is the label. A node whose tenant has no live
// release keeps no seq.
const setVersionWatermarkCypher = `
MATCH (n:%s)
WHERE n.code_version_promotion_seq IS NULL OR n.code_version_promoted_at IS NOT NULL
CALL {
  WITH n
  OPTIONAL MATCH (m:Meta {tenant_id: n.tenant_id, key: 'current_release'})
  SET n.code_version_promotion_seq = coalesce(n.code_version_promotion_seq, m.promotion_seq)
  REMOVE n.code_version_promoted_at
} IN TRANSACTIONS OF $batch_rows ROWS`

func setVersionWatermark(ctx context.Context, session neo4j.SessionWithContext, changes *migrationChanges) error {
	for _, label := range []graphschema.Label{graphschema.Node, graphschema.CodeUnit} {
		if err := runBatched(ctx, session, fmt.Sprintf(setVersionWatermarkCypher, label), nil, changes); err != nil {
			return fmt.Errorf(":%s: %w", label, err)
		}
	}
	return nil
}

// untenantedConstraintsCypher lists the uniqueness constraints step 5 drops.
// The type is UNIQUENESS on older 5.x servers and NODE_PROPERTY_UNIQUENESS
// (or RELATIONSHIP_PROPERTY_UNIQUENESS) on newer ones.
const untenantedConstraintsCypher = `
SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties
WHERE type CONTAINS 'UNIQUENESS'
  AND (NOT 'tenant_id' IN properties OR any(l IN labelsOrTypes WHERE l IN ['Table', 'Run']))
RETURN name`

// untenantedIndexesCypher lists the range indexes step 5 drops. An index that
// backs a constraint goes with its constraint; LOOKUP and every other index
// type stay.
const untenantedIndexesCypher = `
SHOW INDEXES YIELD name, type, labelsOrTypes, properties, owningConstraint
WHERE type = 'RANGE' AND owningConstraint IS NULL
  AND (NOT 'tenant_id' IN properties OR any(l IN labelsOrTypes WHERE l IN ['Table', 'Run']))
RETURN name`

func dropUntenantedSchema(ctx context.Context, session neo4j.SessionWithContext, changes *migrationChanges) error {
	for _, drop := range []struct{ list, kind, what string }{
		{untenantedConstraintsCypher, "CONSTRAINT", "constraints"},
		{untenantedIndexesCypher, "INDEX", "indexes"},
	} {
		names, err := listNames(ctx, session, drop.list)
		if err != nil {
			return fmt.Errorf("list the %s to drop: %w", drop.what, err)
		}
		for _, name := range names {
			stmt := "DROP " + drop.kind + " " + quoteName(name) + " IF EXISTS"
			if err := runCounted(ctx, session, stmt, nil, changes); err != nil {
				return fmt.Errorf("%s: %w", stmt, err)
			}
		}
	}
	return nil
}

// deleteRetiredDependenciesCypher deletes the outgoing :DEPENDS_ON of every
// retired :Node.
const deleteRetiredDependenciesCypher = `
MATCH (n:Node)-[d:DEPENDS_ON]->()
WHERE n.active = false
CALL { WITH d DELETE d } IN TRANSACTIONS OF $batch_rows ROWS`

func deleteRetiredDependencies(ctx context.Context, session neo4j.SessionWithContext, changes *migrationChanges) error {
	return runBatched(ctx, session, deleteRetiredDependenciesCypher, nil, changes)
}

// runBatched runs a CALL { … } IN TRANSACTIONS statement with $batch_rows
// bound. The session runs it as an auto-commit transaction, the only kind in
// which Neo4j accepts the inner transactions.
func runBatched(ctx context.Context, session neo4j.SessionWithContext, cypher string, params map[string]any, changes *migrationChanges) error {
	bound := map[string]any{"batch_rows": migrationBatchRows}
	maps.Copy(bound, params)
	return runCounted(ctx, session, cypher, bound, changes)
}

// runCounted runs one statement in an auto-commit transaction, consumes its
// result, so a failure the server reports only then is not lost, and adds
// what it changed to changes.
func runCounted(ctx context.Context, session neo4j.SessionWithContext, cypher string, params map[string]any, changes *migrationChanges) error {
	res, err := session.Run(ctx, cypher, params)
	if err != nil {
		return err
	}
	summary, err := res.Consume(ctx)
	if err != nil {
		return err
	}
	changes.add(summary.Counters())
	return nil
}

// listNames runs a SHOW statement that returns a name column and reads it.
func listNames(ctx context.Context, session neo4j.SessionWithContext, cypher string) ([]string, error) {
	res, err := session.Run(ctx, cypher, nil)
	if err != nil {
		return nil, err
	}
	recs, err := res.Collect(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(recs))
	for _, rec := range recs {
		v, _ := rec.Get("name")
		name, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("a schema object without a name: %v", v)
		}
		names = append(names, name)
	}
	return names, nil
}

// quoteName quotes a constraint or index name for a DROP statement.
func quoteName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// migrationChanges totals what one step of the migration changed, for its log
// line.
type migrationChanges struct {
	nodesDeleted, relationshipsDeleted, labelsAdded, labelsRemoved, propertiesSet int
	constraintsRemoved, indexesRemoved, constraintsAdded, indexesAdded            int
}

func (c *migrationChanges) add(s neo4j.Counters) {
	c.nodesDeleted += s.NodesDeleted()
	c.relationshipsDeleted += s.RelationshipsDeleted()
	c.labelsAdded += s.LabelsAdded()
	c.labelsRemoved += s.LabelsRemoved()
	c.propertiesSet += s.PropertiesSet()
	c.constraintsRemoved += s.ConstraintsRemoved()
	c.indexesRemoved += s.IndexesRemoved()
	c.constraintsAdded += s.ConstraintsAdded()
	c.indexesAdded += s.IndexesAdded()
}

// logAttrs returns the counts that are not zero as slog key-value pairs.
func (c migrationChanges) logAttrs() []any {
	var attrs []any
	for _, count := range []struct {
		key string
		n   int
	}{
		{"nodes_deleted", c.nodesDeleted},
		{"relationships_deleted", c.relationshipsDeleted},
		{"labels_added", c.labelsAdded},
		{"labels_removed", c.labelsRemoved},
		{"properties_set", c.propertiesSet},
		{"constraints_removed", c.constraintsRemoved},
		{"indexes_removed", c.indexesRemoved},
		{"constraints_added", c.constraintsAdded},
		{"indexes_added", c.indexesAdded},
	} {
		if count.n != 0 {
			attrs = append(attrs, count.key, count.n)
		}
	}
	return attrs
}
