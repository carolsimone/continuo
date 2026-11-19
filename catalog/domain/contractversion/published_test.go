package contractversion_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/pkg/warehouse"
)

func customProperty(name string, value any) map[string]any {
	return map[string]any{"vendor": "continuo", "property": name, "value": value}
}

// storedKPIs is design 3.9's py_daily_kpis in its stored form: version "0", no
// servers, no provenance.
func storedKPIs() map[string]any {
	return map[string]any{
		"apiVersion":  "v3.2.0",
		"kind":        "DataContract",
		"id":          kpis,
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

func provenance52() contractversion.Provenance {
	return contractversion.Provenance{
		ContractHash:      "sha256:3f1c",
		ContentHash:       "sha256:91d4",
		ReleaseID:         "rel-77aa0bc-230",
		ContinuoContracts: "0.2.0",
		SourcePath:        "contracts/py_daily_kpis.odcs.yaml",
		PromotionSeq:      52,
		PromotedAt:        time.Date(2026, 11, 27, 16, 40, 12, 0, time.UTC),
	}
}

func warehouseServer() *contractversion.Server {
	return &contractversion.Server{Type: "postgres", Host: "warehouse.internal.example", Port: 5432, Database: "continuo_dbt"}
}

func TestPublished_TheDesignsPublishedFile(t *testing.T) {
	want := storedKPIs()
	want["version"] = "3"
	want["servers"] = []any{map[string]any{
		"server":   "warehouse",
		"type":     "postgres",
		"host":     "warehouse.internal.example",
		"port":     5432,
		"database": "continuo_dbt",
		"schema":   "analytics",
	}}
	want["customProperties"] = append(want["customProperties"].([]any), map[string]any{
		"vendor":   "continuo",
		"property": "provenance",
		"value": map[string]any{
			"contractHash":      "sha256:3f1c",
			"contentHash":       "sha256:91d4",
			"releaseId":         "rel-77aa0bc-230",
			"promotionSeq":      int64(52),
			"promotedAt":        "2026-11-27T16:40:12Z",
			"continuoContracts": "0.2.0",
			"sourcePath":        "contracts/py_daily_kpis.odcs.yaml",
		},
	})

	got := contractversion.Published(storedKPIs(), 3, warehouseServer(), "", provenance52())

	assert.Equal(t, want, got)
}

func TestPublished_LeavesTheStoredFormUntouchedAndSharesNothingWithIt(t *testing.T) {
	stored := storedKPIs()

	got := contractversion.Published(stored, 3, warehouseServer(), "", provenance52())

	assert.Equal(t, storedKPIs(), stored, "publishing changes nothing in the stored form")
	got["team"].(map[string]any)["name"] = "changed"
	got["schema"].([]any)[0].(map[string]any)["properties"].([]any)[0].(map[string]any)["name"] = "changed"
	got["customProperties"].([]any)[0].(map[string]any)["value"] = "changed"
	assert.Equal(t, storedKPIs(), stored, "editing the published copy changes nothing in the stored form")
}

func TestPublished_NoServerWithoutAHost(t *testing.T) {
	noHost := &contractversion.Server{Type: "postgres", Port: 5432, Database: "continuo_dbt"}
	for name, server := range map[string]*contractversion.Server{"nil": nil, "a server with no host": noHost} {
		got := contractversion.Published(storedKPIs(), 3, server, "", provenance52())
		assert.NotContains(t, got, "servers", name)
		assert.Equal(t, "3", got["version"], name)
	}
}

func TestPublished_TrinoUsesCatalog(t *testing.T) {
	server := &contractversion.Server{Type: "trino", Host: "trino.internal.example", Port: 8080, Database: "iceberg"}

	got := contractversion.Published(storedKPIs(), 1, server, "", provenance52())

	assert.Equal(t, []any{map[string]any{
		"server":  "warehouse",
		"type":    "trino",
		"host":    "trino.internal.example",
		"port":    8080,
		"catalog": "iceberg",
		"schema":  "analytics",
	}}, got["servers"])
}

func TestPublished_AnEmptyDatabaseIsLeftOut(t *testing.T) {
	server := &contractversion.Server{Type: "postgres", Host: "h", Port: 5432}

	got := contractversion.Published(storedKPIs(), 1, server, "", provenance52())

	entry := got["servers"].([]any)[0].(map[string]any)
	assert.NotContains(t, entry, "database")
	assert.NotContains(t, entry, "catalog")
}

func TestPublished_TheSchemaIsTheIDUpToItsFirstDot(t *testing.T) {
	// A versioned dbt model's id is <schema>.<name>.v<version>.
	stored := storedKPIs()
	stored["id"] = "analytics.dim_customer.v2"

	got := contractversion.Published(stored, 1, warehouseServer(), "", provenance52())

	assert.Equal(t, "analytics", got["servers"].([]any)[0].(map[string]any)["schema"])
}

// dbt quotes the schema it builds in, so a node declared in "Analytics" lives in
// a schema a consumer binds to only by that spelling, while the logical id is
// lower case. The servers entry names the declared spelling; the id stays.
func TestPublished_TheServersEntryNamesTheDeclaredSchemaNotTheIDsSpelling(t *testing.T) {
	stored := storedKPIs()

	got := contractversion.Published(stored, 1, warehouseServer(), "Analytics", provenance52())

	assert.Equal(t, "Analytics", got["servers"].([]any)[0].(map[string]any)["schema"])
	assert.Equal(t, kpis, got["id"], "the logical id keeps its lower-case spelling")
	assert.Equal(t, kpis, stored["id"])
}

func TestPublished_NoDeclaredSchemaNamesTheIDsSchema(t *testing.T) {
	got := contractversion.Published(storedKPIs(), 1, warehouseServer(), "", provenance52())

	assert.Equal(t, "analytics", got["servers"].([]any)[0].(map[string]any)["schema"])
}

// A node declared in a lower-case schema, which is every node of an install
// that quotes nothing, publishes the same file with or without its declared
// schema: the declared spelling and the id's agree.
func TestPublished_ALowerCaseDeclaredSchemaPublishesWhatTheIDsSchemaDoes(t *testing.T) {
	declared := contractversion.Published(storedKPIs(), 3, warehouseServer(), "analytics", provenance52())
	fromID := contractversion.Published(storedKPIs(), 3, warehouseServer(), "", provenance52())

	assert.Equal(t, fromID, declared)
}

func TestPublished_TheDeclaredSchemaIsShownOnlyInAServersEntry(t *testing.T) {
	noHost := &contractversion.Server{Type: "postgres", Port: 5432, Database: "continuo_dbt"}
	for name, server := range map[string]*contractversion.Server{"nil": nil, "a server with no host": noHost} {
		got := contractversion.Published(storedKPIs(), 3, server, "Analytics", provenance52())

		assert.NotContains(t, got, "servers", name)
		assert.Equal(t, contractversion.Published(storedKPIs(), 3, server, "", provenance52()), got, name)
	}
}

func TestPublished_AnIDWithNoSchemaGivesNoSchemaKey(t *testing.T) {
	stored := storedKPIs()
	stored["id"] = "py_daily_kpis"

	got := contractversion.Published(stored, 1, warehouseServer(), "", provenance52())

	assert.NotContains(t, got["servers"].([]any)[0].(map[string]any), "schema")
}

func TestServerSchema(t *testing.T) {
	for name, tc := range map[string]struct {
		id       string
		declared string
		want     string
	}{
		"the declared schema wins over the id's":      {"analytics.orders", "Analytics", "Analytics"},
		"a lower-case declared schema is the same":    {"analytics.orders", "analytics", "analytics"},
		"no declared schema takes the id's":           {"analytics.orders", "", "analytics"},
		"a versioned id keeps its schema":             {"analytics.dim_customer.v2", "", "analytics"},
		"an id with no schema part and none declared": {"orders", "", ""},
		"a declared schema names an id with no dot":   {"orders", "Analytics", "Analytics"},
		"no id and none declared":                     {"", "", ""},
	} {
		stored := map[string]any{"id": tc.id}
		assert.Equal(t, tc.want, contractversion.ServerSchema(stored, tc.declared), name)
	}
	assert.Empty(t, contractversion.ServerSchema(map[string]any{"id": 3}, ""), "an id that is no text has no schema")
	assert.Empty(t, contractversion.ServerSchema(nil, ""), "a nil document has none")
}

func TestPublished_TheProvenanceIsTheOnlyOneEvenWhenTheStoredFormCarriesOne(t *testing.T) {
	stored := storedKPIs()
	stored["customProperties"] = append(stored["customProperties"].([]any), customProperty("provenance", map[string]any{"releaseId": "stale"}))

	got := contractversion.Published(stored, 3, nil, "", provenance52())

	var provenances []any
	for _, p := range got["customProperties"].([]any) {
		if p.(map[string]any)["property"] == "provenance" {
			provenances = append(provenances, p)
		}
	}
	require.Len(t, provenances, 1)
	assert.Equal(t, "rel-77aa0bc-230", provenances[0].(map[string]any)["value"].(map[string]any)["releaseId"])
	props := got["customProperties"].([]any)
	assert.Equal(t, "provenance", props[len(props)-1].(map[string]any)["property"], "provenance comes last")
}

func TestPublished_AContractWithNoCustomPropertiesGetsTheProvenanceAlone(t *testing.T) {
	stored := map[string]any{"id": kpis, "version": "0"}

	got := contractversion.Published(stored, 2, nil, "", provenance52())

	props, ok := got["customProperties"].([]any)
	require.True(t, ok)
	require.Len(t, props, 1)
	assert.Equal(t, "provenance", props[0].(map[string]any)["property"])
	assert.NotContains(t, stored, "customProperties")
}

func TestPublished_PromotedAtIsRFC3339UTC(t *testing.T) {
	prov := provenance52()
	prov.PromotedAt = time.Date(2026, 11, 27, 18, 40, 12, 987654321, time.FixedZone("CEST", 2*60*60))

	got := contractversion.Published(storedKPIs(), 3, nil, "", prov)

	props := got["customProperties"].([]any)
	value := props[len(props)-1].(map[string]any)["value"].(map[string]any)
	assert.Equal(t, "2026-11-27T16:40:12Z", value["promotedAt"])
}

func TestIsEphemeral(t *testing.T) {
	withType := func(physicalType any) map[string]any {
		return map[string]any{"schema": []any{map[string]any{"physicalType": physicalType}}}
	}
	for name, tc := range map[string]struct {
		stored map[string]any
		want   bool
	}{
		"an ephemeral model":              {withType("ephemeral"), true},
		"a table":                         {withType("table"), false},
		"a view":                          {withType("view"), false},
		"a physical type that is no text": {withType(3), false},
		"no schema":                       {map[string]any{"id": kpis}, false},
		"an empty schema":                 {map[string]any{"schema": []any{}}, false},
		"a schema that is no list":        {map[string]any{"schema": "ephemeral"}, false},
		"a schema entry that is no object": {
			map[string]any{"schema": []any{"ephemeral"}}, false},
		"only the first schema object counts": {
			map[string]any{"schema": []any{map[string]any{"physicalType": "table"}, map[string]any{"physicalType": "ephemeral"}}}, false},
		"a nil document": {nil, false},
	} {
		assert.Equal(t, tc.want, contractversion.IsEphemeral(tc.stored), name)
	}
}

func TestServerType(t *testing.T) {
	for engine, want := range map[string]string{"": "postgres", "postgres": "postgres", "trino": "trino"} {
		got, err := contractversion.ServerType(engine)
		require.NoError(t, err, engine)
		assert.Equal(t, want, got, engine)
	}
}

func TestServerType_RefusesAnUnknownEngine(t *testing.T) {
	for _, engine := range []string{"mysql", "Postgres", " trino", "bigquery"} {
		got, err := contractversion.ServerType(engine)
		require.Error(t, err, engine)
		assert.True(t, errors.Is(err, contractversion.ErrUnsupportedEngine), engine)
		assert.Contains(t, err.Error(), `"`+engine+`"`, "the error names the engine it refused")
		assert.Empty(t, got)
	}
}

func TestServerType_AcceptsEveryEngineTheWarehouseFamiliesKnow(t *testing.T) {
	// WAREHOUSE_ENGINE stops a service at boot when it names no family; the
	// published servers list must honour the same set.
	for _, engine := range warehouse.Engines() {
		_, err := contractversion.ServerType(engine)
		assert.NoError(t, err, engine)
	}
}

func sumHex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func identity(b []byte) string { return string(b) }

func TestServerFingerprint_HashesTheCanonicalSettings(t *testing.T) {
	got := warehouseServer().Fingerprint(identity)

	assert.Equal(t, "type=\"postgres\"\nhost=\"warehouse.internal.example\"\nport=5432\ndatabase=\"continuo_dbt\"\n", got)
}

func TestServerFingerprint_EverySettingMovesIt(t *testing.T) {
	base := *warehouseServer()
	seen := map[string]string{base.Fingerprint(sumHex): "base"}
	for name, edit := range map[string]func(*contractversion.Server){
		"type":     func(s *contractversion.Server) { s.Type = "trino" },
		"host":     func(s *contractversion.Server) { s.Host = "other.example" },
		"port":     func(s *contractversion.Server) { s.Port = 5433 },
		"database": func(s *contractversion.Server) { s.Database = "other" },
	} {
		changed := base
		edit(&changed)
		fp := changed.Fingerprint(sumHex)
		assert.NotContains(t, seen, fp, name)
		seen[fp] = name
	}
	assert.Equal(t, base.Fingerprint(sumHex), base.Fingerprint(sumHex), "the same settings give the same fingerprint")
}

func TestServerFingerprint_TwoDifferentSettingsNeverShareBytes(t *testing.T) {
	a := contractversion.Server{Type: "postgres", Host: "h\nport=1", Port: 2}
	b := contractversion.Server{Type: "postgres", Host: "h", Port: 1}
	assert.NotEqual(t, a.Fingerprint(identity), b.Fingerprint(identity))
}

func TestServerFingerprint_NoHostMeansNoServerWhateverElseIsSet(t *testing.T) {
	// A published file carries no servers entry without a host, so the other
	// settings cannot change any file and must not change the fingerprint.
	unset := contractversion.Server{}
	trinoNoHost := contractversion.Server{Type: "trino", Port: 8080, Database: "iceberg"}
	assert.Equal(t, unset.Fingerprint(sumHex), trinoNoHost.Fingerprint(sumHex))
	assert.NotEqual(t, unset.Fingerprint(sumHex), warehouseServer().Fingerprint(sumHex))
}
