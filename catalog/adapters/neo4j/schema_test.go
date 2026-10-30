//go:build integration

package neo4jinfra

import (
	"context"
	"slices"
	"sort"
	"testing"

	"github.com/carolsimone/continuo/catalog/domain/graphschema"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemaEntry is the label and properties of one constraint or index.
type schemaEntry struct {
	Label      string
	Properties []string
}

// wantConstraints is the graph's uniqueness constraints, by name: each
// label's identity within its tenant, and the marker's second key.
var wantConstraints = map[string]schemaEntry{
	"node_unique":                     {"Node", []string{"tenant_id", "unique_id"}},
	"meta_key_unique":                 {"Meta", []string{"tenant_id", "key"}},
	"node_version_unique":             {"NodeVersion", []string{"tenant_id", "unique_id", "content_hash"}},
	"code_unit_unique":                {"CodeUnit", []string{"tenant_id", "unit_id"}},
	"code_unit_version_unique":        {"CodeUnitVersion", []string{"tenant_id", "unit_id", "checksum"}},
	"rejection_unique":                {"Rejection", []string{"tenant_id", "release_id", "node_id"}},
	"error_signature_unique":          {"ErrorSignature", []string{"tenant_id", "signature"}},
	"proposal_unique":                 {"Proposal", []string{"tenant_id", "proposal_id"}},
	"pull_request_unique":             {"PullRequest", []string{"tenant_id", "proposal_id", "service"}},
	"processed_message_unique":        {"ProcessedMessage", []string{"tenant_id", "group", "message_id"}},
	"processed_message_outbox_unique": {"ProcessedMessage", []string{"tenant_id", "group", "outbox_entry_id"}},
}

// wantIndexes is the graph's range indexes that back no constraint, by name.
var wantIndexes = map[string]schemaEntry{
	"node_fqn":                        {"Node", []string{"tenant_id", "service_name", "schema_name", "table_name"}},
	"node_version_uid":                {"NodeVersion", []string{"tenant_id", "unique_id"}},
	"code_unit_version_unit_id":       {"CodeUnitVersion", []string{"tenant_id", "unit_id"}},
	"rejection_node_id":               {"Rejection", []string{"tenant_id", "node_id"}},
	"rejection_category_reason":       {"Rejection", []string{"tenant_id", "category", "reason"}},
	"error_signature_category_reason": {"ErrorSignature", []string{"tenant_id", "category", "reason"}},
}

// graphSchemaState is what SHOW CONSTRAINTS and SHOW INDEXES report.
type graphSchemaState struct {
	constraints     map[string]schemaEntry // every constraint, whatever its type
	constraintTypes map[string]string
	ranges          map[string]schemaEntry // every RANGE index, a constraint's included
	rangeOwners     map[string]string      // RANGE index name -> owning constraint ("" for none)
	lookups         []string               // LOOKUP index names, sorted
	others          []string               // every other index, as "name (TYPE)"
	ids             map[string]int64       // constraint and index ids, keyed "constraint:name" or "index:name"
}

func stringList(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		s, _ := item.(string)
		out = append(out, s)
	}
	return out
}

func readGraphSchema(t *testing.T, client Neo4jClient) graphSchemaState {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	state := graphSchemaState{
		constraints:     map[string]schemaEntry{},
		constraintTypes: map[string]string{},
		ranges:          map[string]schemaEntry{},
		rangeOwners:     map[string]string{},
		ids:             map[string]int64{},
	}

	res, err := s.Run(ctx, `SHOW CONSTRAINTS YIELD id, name, type, labelsOrTypes, properties RETURN *`, nil)
	require.NoError(t, err)
	recs, err := res.Collect(ctx)
	require.NoError(t, err)
	for _, rec := range recs {
		m := rec.AsMap()
		name := m["name"].(string)
		labels := stringList(m["labelsOrTypes"])
		require.Len(t, labels, 1, "constraint %s", name)
		state.constraints[name] = schemaEntry{labels[0], stringList(m["properties"])}
		state.constraintTypes[name] = m["type"].(string)
		state.ids["constraint:"+name] = m["id"].(int64)
	}

	res, err = s.Run(ctx, `SHOW INDEXES YIELD id, name, type, labelsOrTypes, properties, owningConstraint RETURN *`, nil)
	require.NoError(t, err)
	recs, err = res.Collect(ctx)
	require.NoError(t, err)
	for _, rec := range recs {
		m := rec.AsMap()
		name := m["name"].(string)
		state.ids["index:"+name] = m["id"].(int64)
		switch typ := m["type"].(string); typ {
		case "RANGE":
			labels := stringList(m["labelsOrTypes"])
			require.Len(t, labels, 1, "index %s", name)
			state.ranges[name] = schemaEntry{labels[0], stringList(m["properties"])}
			owner, _ := m["owningConstraint"].(string)
			state.rangeOwners[name] = owner
		case "LOOKUP":
			state.lookups = append(state.lookups, name)
		default:
			state.others = append(state.others, name+" ("+typ+")")
		}
	}
	sort.Strings(state.lookups)
	return state
}

// assertTargetSchema checks the graph's constraints and indexes are exactly
// the catalog's: every uniqueness constraint and range index of
// wantConstraints and wantIndexes, under its name and over its properties
// with tenant_id first, nothing else, and the LOOKUP indexes the graph had
// before.
func assertTargetSchema(t *testing.T, client Neo4jClient, lookupsBefore []string) graphSchemaState {
	t.Helper()
	state := readGraphSchema(t, client)

	assert.Equal(t, wantConstraints, state.constraints, "SHOW CONSTRAINTS")
	for name, typ := range state.constraintTypes {
		assert.Contains(t, typ, "UNIQUENESS", "constraint %s is a uniqueness constraint", name)
	}

	wantRanges := map[string]schemaEntry{}
	wantOwners := map[string]string{}
	for name, e := range wantConstraints {
		wantRanges[name] = e
		wantOwners[name] = name
	}
	for name, e := range wantIndexes {
		wantRanges[name] = e
		wantOwners[name] = ""
	}
	assert.Equal(t, wantRanges, state.ranges, "SHOW INDEXES, type RANGE")
	assert.Equal(t, wantOwners, state.rangeOwners, "each constraint owns its own index; the plain indexes own none")
	for name, e := range state.ranges {
		if assert.NotEmpty(t, e.Properties, "index %s", name) {
			assert.Equal(t, "tenant_id", e.Properties[0], "index %s leads with tenant_id", name)
		}
	}

	assert.Empty(t, state.others, "no index other than RANGE and LOOKUP")
	assert.Equal(t, lookupsBefore, state.lookups, "the LOOKUP indexes are left as they were")
	return state
}

// TestInitSchema_Idempotent applies the schema to a graph with no old-shape
// data and applies it again: the second run finds the exact target set and
// drops and recreates nothing.
func TestInitSchema_Idempotent(t *testing.T) {
	ctx := context.Background()
	client := integrationClient(t) // the first apply
	lookups := readGraphSchema(t, client).lookups
	require.NotEmpty(t, lookups, "Neo4j keeps its token LOOKUP indexes")
	first := assertTargetSchema(t, client, lookups)

	require.NoError(t, InitSchema(ctx, client, quietLogger()), "second apply")
	second := assertTargetSchema(t, client, lookups)
	assert.Equal(t, first.ids, second.ids, "a second apply drops and recreates no constraint or index")
}

// TestCatalogSchema_EveryLabelHasItsIdentityConstraint pins the schema to
// graphschema: every label of the graph has one uniqueness constraint over
// tenant_id and the label's identity, and every object of the schema leads
// with tenant_id and is created only when absent.
func TestCatalogSchema_EveryLabelHasItsIdentityConstraint(t *testing.T) {
	for _, label := range graphschema.Labels() {
		found := 0
		for _, o := range catalogSchema {
			if o.unique && o.label == label && slices.Equal(o.properties, graphschema.Identity(label)) {
				found++
			}
		}
		assert.Equal(t, 1, found, "label %s has one identity constraint", label)
	}
	for _, o := range catalogSchema {
		assert.Contains(t, o.statement(), "(n.tenant_id, ", "%s leads with tenant_id", o.name)
		assert.Contains(t, o.statement(), " IF NOT EXISTS ", "%s is created only when absent", o.name)
	}
}
