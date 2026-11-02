package neo4jinfra

import (
	"context"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/catalog/service/export/graphschema"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// tenantProperty is the property every node of the catalog graph carries its
// tenant in. It leads every constraint and index of the schema.
const tenantProperty = "tenant_id"

// schemaObject is one uniqueness constraint or range index of the catalog
// graph, over tenant_id followed by properties.
type schemaObject struct {
	name       string
	unique     bool
	label      graphschema.Label
	properties []string
}

// identityConstraint makes the label's identity in graphschema unique within
// a tenant.
func identityConstraint(name string, label graphschema.Label) schemaObject {
	return schemaObject{name: name, unique: true, label: label, properties: graphschema.Identity(label)}
}

// uniqueConstraint makes properties unique within a tenant among the nodes of
// label.
func uniqueConstraint(name string, label graphschema.Label, properties ...string) schemaObject {
	return schemaObject{name: name, unique: true, label: label, properties: properties}
}

// rangeIndex indexes the nodes of label by tenant and properties.
func rangeIndex(name string, label graphschema.Label, properties ...string) schemaObject {
	return schemaObject{name: name, label: label, properties: properties}
}

// statement is the object's CREATE statement, which does nothing when an
// object of the same name exists.
func (o schemaObject) statement() string {
	props := make([]string, 0, len(o.properties)+1)
	for _, p := range append([]string{tenantProperty}, o.properties...) {
		props = append(props, "n."+p)
	}
	on := strings.Join(props, ", ")
	if o.unique {
		return fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:%s) REQUIRE (%s) IS UNIQUE", o.name, o.label, on)
	}
	return fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR (n:%s) ON (%s)", o.name, o.label, on)
}

// catalogSchema is every constraint and index of the catalog graph. Each
// label's identity is unique within its tenant, so two concurrent MERGEs of
// one node cannot create two, and a redelivered message MERGEs onto what it
// wrote before. A uniqueness constraint ignores a node that lacks one of its
// properties, so the code that writes a node, not the schema, keeps tenant_id
// present.
//
//   - node_fqn backs GetNode's lookup by service, schema and table name.
//   - node_version_uid and code_unit_version_unit_id back the version-history
//     reads that start from a node's unique_id or a code unit's unit_id; the
//     identity constraints cover only the full (id, hash) pair.
//   - rejection_node_id backs the [:RESOLVED_BY] links the versions group and
//     a late rejection draw from a node's rejections.
//   - rejection_category_reason backs the category and reason precedent
//     lookup, which filters :Rejection's own properties.
//   - error_signature_category_reason indexes the signature hub's first-seen
//     category and reason for direct hub lookups; the hubs are few, so it
//     costs little to keep.
//   - processed_message_outbox_unique is the marker's second key: a producer's
//     outbox row re-sent under a new stream message id is the same message.
var catalogSchema = []schemaObject{
	identityConstraint("node_unique", graphschema.Node),
	rangeIndex("node_fqn", graphschema.Node, "service_name", "schema_name", "table_name"),
	identityConstraint("meta_key_unique", graphschema.Meta),
	identityConstraint("node_version_unique", graphschema.NodeVersion),
	rangeIndex("node_version_uid", graphschema.NodeVersion, "unique_id"),
	identityConstraint("code_unit_unique", graphschema.CodeUnit),
	identityConstraint("code_unit_version_unique", graphschema.CodeUnitVersion),
	rangeIndex("code_unit_version_unit_id", graphschema.CodeUnitVersion, "unit_id"),
	identityConstraint("rejection_unique", graphschema.Rejection),
	rangeIndex("rejection_node_id", graphschema.Rejection, "node_id"),
	rangeIndex("rejection_category_reason", graphschema.Rejection, "category", "reason"),
	identityConstraint("error_signature_unique", graphschema.ErrorSignature),
	rangeIndex("error_signature_category_reason", graphschema.ErrorSignature, "category", "reason"),
	identityConstraint("proposal_unique", graphschema.Proposal),
	identityConstraint("pull_request_unique", graphschema.PullRequest),
	identityConstraint("processed_message_unique", graphschema.ProcessedMessage),
	uniqueConstraint("processed_message_outbox_unique", graphschema.ProcessedMessage, "group", "outbox_entry_id"),
}

// awaitIndexTimeoutSeconds bounds how long createSchema waits for the indexes
// to come online. On an empty graph they are online at once; on a populated
// one this is the ceiling before startup fails rather than serve queries
// against an index still being built.
const awaitIndexTimeoutSeconds = 300

// createSchema creates every object of catalogSchema that does not exist yet
// and waits until every index is online. Creating a uniqueness constraint
// fails when existing nodes already break it.
func createSchema(ctx context.Context, session neo4j.SessionWithContext, changes *migrationChanges) error {
	for _, o := range catalogSchema {
		if err := runCounted(ctx, session, o.statement(), nil, changes); err != nil {
			return fmt.Errorf("create %s: %w", o.name, err)
		}
	}
	// CREATE only registers an index; on a populated graph Neo4j fills it in
	// the background. Waiting here means the first message never runs against
	// a full label scan.
	if err := runCounted(ctx, session, "CALL db.awaitIndexes($timeout)",
		map[string]any{"timeout": awaitIndexTimeoutSeconds}, changes); err != nil {
		return fmt.Errorf("await indexes online: %w", err)
	}
	return nil
}
