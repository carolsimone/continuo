package serialization_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/catalog/adapters/serialization"
	"github.com/carolsimone/continuo/catalog/domain/contractversion"
)

func customProperty(name string, value any) map[string]any {
	return map[string]any{"vendor": "continuo", "property": name, "value": value}
}

// storedKPIs is design 3.9's py_daily_kpis in its stored form, as catalog
// decodes it from the graph: numbers are float64.
func storedKPIs() map[string]any {
	return map[string]any{
		"apiVersion":  "v3.2.0",
		"kind":        "DataContract",
		"id":          "analytics.py_daily_kpis",
		"name":        "py_daily_kpis",
		"version":     "0",
		"status":      "active",
		"description": map[string]any{"purpose": "Daily transaction count and total amount in EUR, rolled up from analytics.daily_transactions."},
		"team":        map[string]any{"name": "service-py"},
		"customProperties": []any{
			customProperty("service", "service-py"),
			customProperty("kind", "python-node"),
			customProperty("schedule", "daily"),
			customProperty("criticality", "SECONDARY"),
			customProperty("script", "scripts/py_daily_kpis.py"),
			customProperty("readQueries", map[string]any{"daily": "select created_at, amount_eur from analytics.daily_transactions"}),
		},
		"schema": []any{map[string]any{
			"name":         "py_daily_kpis",
			"physicalName": "py_daily_kpis",
			"physicalType": "table",
			"properties": []any{
				map[string]any{"name": "day", "physicalType": "DATE", "logicalType": "date", "required": true},
				map[string]any{"name": "tx_count", "physicalType": "INTEGER", "logicalType": "integer", "required": true},
				map[string]any{"name": "total_amount", "physicalType": "NUMERIC(12,2)", "logicalType": "number", "required": true},
			},
		}},
	}
}

// publishedKPIs is the design's published py_daily_kpis: version 3, on an
// install with the warehouse server settings, promoted by rel-77aa0bc-230.
func publishedKPIs() map[string]any {
	return publishedKPIsDeclaredIn("")
}

// publishedKPIsDeclaredIn is publishedKPIs for a node whose declared schema is
// schema ("" for a node that has none).
func publishedKPIsDeclaredIn(schema string) map[string]any {
	server := &contractversion.Server{Type: "postgres", Host: "warehouse.internal.example", Port: 5432, Database: "continuo_dbt"}
	return contractversion.Published(storedKPIs(), 3, server, schema, contractversion.Provenance{
		ContractHash:      "sha256:3f1c...",
		ContentHash:       "sha256:91d4...",
		ReleaseID:         "rel-77aa0bc-230",
		ContinuoContracts: "0.2.0",
		SourcePath:        "contracts/py_daily_kpis.odcs.yaml",
		PromotionSeq:      52,
		PromotedAt:        time.Date(2026, 11, 27, 16, 40, 12, 0, time.UTC),
	}, "")
}

// wantKPIs is design 3.9's published file as the renderer writes it. It
// differs from the design's text where the design is not what a renderer can
// write: the caption comment naming the object is not part of the file, the
// purpose is quoted (a plain scalar in a flow mapping cannot hold ", "), a
// property lists its keys after name in sorted order, and the provenance value
// is a block mapping because its flow form does not fit on one line.
const wantKPIs = `apiVersion: v3.2.0
kind: DataContract
id: analytics.py_daily_kpis
name: py_daily_kpis
version: "3"
status: active
servers:
  - {server: warehouse, type: postgres, host: warehouse.internal.example, port: 5432, database: continuo_dbt, schema: analytics}
description: {purpose: "Daily transaction count and total amount in EUR, rolled up from analytics.daily_transactions."}
team: {name: service-py}
customProperties:
  - {vendor: continuo, property: service, value: service-py}
  - {vendor: continuo, property: kind, value: python-node}
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: script, value: scripts/py_daily_kpis.py}
  - {vendor: continuo, property: readQueries, value: {daily: "select created_at, amount_eur from analytics.daily_transactions"}}
  - vendor: continuo
    property: provenance
    value:
      contractHash: "sha256:3f1c..."
      contentHash: "sha256:91d4..."
      releaseId: rel-77aa0bc-230
      promotionSeq: 52
      promotedAt: "2026-11-27T16:40:12Z"
      continuoContracts: 0.2.0
      sourcePath: contracts/py_daily_kpis.odcs.yaml
schema:
  - name: py_daily_kpis
    physicalName: py_daily_kpis
    physicalType: table
    properties:
      - {name: day, logicalType: date, physicalType: DATE, required: true}
      - {name: tx_count, logicalType: integer, physicalType: INTEGER, required: true}
      - {name: total_amount, logicalType: number, physicalType: "NUMERIC(12,2)", required: true}
`

func TestODCSYAML_RendersTheDesignsPublishedFileInItsKeyOrder(t *testing.T) {
	got, err := serialization.ODCSYAML{}.Render(publishedKPIs())

	require.NoError(t, err)
	assert.Equal(t, wantKPIs, string(got))
}

// A node declared in a lower-case schema renders the design's file byte for
// byte, and one declared in a quoted schema renders its servers entry with that
// spelling and nothing else changed: the id stays in lower case.
func TestODCSYAML_RendersTheDeclaredSchemaInTheServersEntryOnly(t *testing.T) {
	lower, err := serialization.ODCSYAML{}.Render(publishedKPIsDeclaredIn("analytics"))
	require.NoError(t, err)
	assert.Equal(t, wantKPIs, string(lower))

	quoted, err := serialization.ODCSYAML{}.Render(publishedKPIsDeclaredIn("Analytics"))

	require.NoError(t, err)
	want := strings.Replace(wantKPIs, "database: continuo_dbt, schema: analytics}", "database: continuo_dbt, schema: Analytics}", 1)
	require.NotEqual(t, wantKPIs, want)
	assert.Equal(t, want, string(quoted))
	assert.Contains(t, string(quoted), "id: analytics.py_daily_kpis\n")
}

// The rendered file reads back as the document it was rendered from, so
// quoting never changes a value: "3" stays a string, a date-like string stays
// a string, and a number stays a number.
func TestODCSYAML_ReadsBackAsTheDocument(t *testing.T) {
	doc := publishedKPIs()
	got, err := serialization.ODCSYAML{}.Render(doc)
	require.NoError(t, err)

	var back map[string]any
	require.NoError(t, yaml.Unmarshal(got, &back))

	assert.Equal(t, "3", back["version"])
	provenance := back["customProperties"].([]any)[6].(map[string]any)["value"].(map[string]any)
	assert.Equal(t, "2026-11-27T16:40:12Z", provenance["promotedAt"])
	assert.Equal(t, 52, provenance["promotionSeq"])
	assert.Equal(t, doc["description"], back["description"])
	assert.Equal(t, "NUMERIC(12,2)", back["schema"].([]any)[0].(map[string]any)["properties"].([]any)[2].(map[string]any)["physicalType"])
}

// The same document renders the same bytes every time, although Go visits a
// map's keys in a different order on each walk.
func TestODCSYAML_SameDocumentSameBytes(t *testing.T) {
	first, err := serialization.ODCSYAML{}.Render(publishedKPIs())
	require.NoError(t, err)
	for range 50 {
		again, err := serialization.ODCSYAML{}.Render(publishedKPIs())
		require.NoError(t, err)
		require.Equal(t, string(first), string(again))
	}
}

// A root key outside the fixed order follows the ordered ones, sorted, and a
// mapping outside the named places lists its keys sorted.
func TestODCSYAML_OrdersUnknownKeysSortedAfterTheKnownOnes(t *testing.T) {
	doc := map[string]any{
		"zeta":       "z",
		"alpha":      "a",
		"schema":     []any{},
		"apiVersion": "v3.2.0",
		"support":    []any{map[string]any{"url": "u", "channel": "c", "tool": "slack", "scope": "interactive"}},
	}

	got, err := serialization.ODCSYAML{}.Render(doc)

	require.NoError(t, err)
	assert.Equal(t, `apiVersion: v3.2.0
support:
  - {channel: c, scope: interactive, tool: slack, url: u}
schema: []
alpha: a
zeta: z
`, string(got))
}

// A mapping whose flow form would not fit on one line, or that holds a list,
// renders as a block; a string with a line break is double-quoted.
func TestODCSYAML_LongMappingsAndListsRenderAsBlocks(t *testing.T) {
	long := "a purpose long enough that the description cannot sit on one line of the file with its key and the braces around it"
	doc := map[string]any{
		"description": map[string]any{"purpose": long, "usage": "line one\nline two"},
		"tags":        []any{"finance", "daily"},
		"team":        map[string]any{"name": "service-py", "members": []any{map[string]any{"username": "ana"}}},
	}

	got, err := serialization.ODCSYAML{}.Render(doc)

	require.NoError(t, err)
	assert.Equal(t, `description:
  purpose: `+long+`
  usage: "line one\nline two"
team:
  members:
    - {username: ana}
  name: service-py
tags:
  - finance
  - daily
`, string(got))
}

// A value of a type a decoded document never holds is refused, not rendered
// by guesswork.
func TestODCSYAML_RefusesAValueOfAnotherType(t *testing.T) {
	_, err := serialization.ODCSYAML{}.Render(map[string]any{"id": struct{}{}})

	require.Error(t, err)
}

// A number decoded as its text is written as that number, exactly: an
// integer beyond float64's exact range keeps every digit and reads back as
// the same integer, a fraction reads back as a float.
func TestODCSYAML_WritesANumberAsItsText(t *testing.T) {
	doc := map[string]any{"limit": json.Number("9007199254740993"), "ratio": json.Number("0.1"), "big": json.Number("1e+21")}

	got, err := serialization.ODCSYAML{}.Render(doc)
	require.NoError(t, err)

	assert.Equal(t, "big: 1e+21\nlimit: 9007199254740993\nratio: 0.1\n", string(got))
	var back map[string]any
	require.NoError(t, yaml.Unmarshal(got, &back))
	assert.Equal(t, 9007199254740993, back["limit"])
	assert.InDelta(t, 0.1, back["ratio"], 0)
	assert.InDelta(t, 1e21, back["big"], 0)
}

// The ontology version a contract was resolved against is the provenance fact
// after the library version and before the source path, and the ontology links
// the published form carries render as absolute URLs.
func TestODCSYAML_RendersTheOntologyVersionAndAbsoluteLinks(t *testing.T) {
	stored := storedKPIs()
	stored["authoritativeDefinitions"] = []any{map[string]any{"type": "ontology", "url": "continuo-ontology:Kpi"}}
	published := contractversion.Published(stored, 3, nil, "", contractversion.Provenance{
		ContractHash:      "sha256:3f1c...",
		ContentHash:       "sha256:91d4...",
		ReleaseID:         "rel-77aa0bc-230",
		ContinuoContracts: "0.2.0",
		OntologyVersion:   "rel-o-3@sha256:abc123",
		SourcePath:        "contracts/py_daily_kpis.odcs.yaml",
		PromotionSeq:      52,
		PromotedAt:        time.Date(2026, 11, 27, 16, 40, 12, 0, time.UTC),
	}, "https://ontology.example/things")

	got, err := serialization.ODCSYAML{}.Render(published)

	require.NoError(t, err)
	want := strings.Replace(wantKPIs, "servers:\n  - {server: warehouse, type: postgres, host: warehouse.internal.example, port: 5432, database: continuo_dbt, schema: analytics}\n", "", 1)
	want = strings.Replace(want, "customProperties:\n", "authoritativeDefinitions:\n  - {type: ontology, url: \"https://ontology.example/things/Kpi\"}\ncustomProperties:\n", 1)
	want = strings.Replace(want, "      continuoContracts: 0.2.0\n", "      continuoContracts: 0.2.0\n      ontologyVersion: \"rel-o-3@sha256:abc123\"\n", 1)
	require.NotEqual(t, wantKPIs, want)
	assert.Equal(t, want, string(got))
}
