package repofs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTree writes files (repository-relative path -> content) under a new
// temporary directory and returns it.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, content := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o750))
		require.NoError(t, os.WriteFile(full, []byte(content), 0o600))
	}
	return root
}

func TestModelReferenceFinder_FindsWhatNamesTheModel(t *testing.T) {
	root := writeTree(t, map[string]string{
		"services/marketing/models/table_b.sql":    "select 1 as id",
		"services/marketing/models/table_b_v2.sql": "select 2 as id",
		"services/marketing/models/uses_b.sql":     "select * from {{ ref('table_b') }}",
		"services/marketing/models/unrelated.sql":  "select * from {{ ref('spend') }}",
		"services/marketing/models/forecast.py":    "def model(dbt, session): pass",
		"services/marketing/models/schema.yml":     "version: 2\nmodels:\n  - name: table_b\n    meta: {continuo: {grain: [id]}}\n  - name: spend\n    meta: {continuo: {grain: [month]}}\n",
		"services/marketing/models/sources.yml":    "version: 2\nsources:\n  - name: table_b\n    tables: [{name: raw}]\n",
		"services/marketing/dbt_project.yml":       "name: marketing\nmodels:\n  marketing:\n    +materialized: table\n",
		"services/marketing/target/compiled/x.sql": "select * from {{ ref('table_b') }}",
		"services/marketing/dbt_packages/p/m.sql":  "select * from {{ ref('table_b') }}",
		"services/marketing/models/broken.yml":     "models: [unclosed",
		"services/service-1/models/table_b.sql":    "select * from {{ ref('table_b') }}",
		"services/service-1/models/schema.yml":     "models:\n  - name: table_b\n",
	})

	refs, err := ModelReferenceFinder{}.Find(root, "services/marketing", "table_b")
	require.NoError(t, err)

	assert.Equal(t, []string{"services/marketing/models/schema.yml", "services/marketing/models/uses_b.sql"}, refs.Order,
		"only the project's own properties entry and ref name the model: not a source of that name, dbt's output, an installed package or another project")
	assert.Equal(t, "select * from {{ ref('table_b') }}", refs.Files["services/marketing/models/uses_b.sql"])
	for _, name := range []string{"table_b", "table_b_v2", "uses_b", "unrelated", "forecast", "spend"} {
		assert.True(t, refs.ModelNames[name], name)
	}
	assert.True(t, refs.DeclaresKind)
	assert.True(t, refs.OthersDeclareKind)
}

func TestModelReferenceFinder_AModelWithNoMetadataInAProjectThatDeclaresIt(t *testing.T) {
	root := writeTree(t, map[string]string{
		"services/marketing/models/table_b.sql": "select 1 as id",
		"services/marketing/models/schema.yml":  "models:\n  - name: spend\n    config:\n      meta:\n        continuo:\n          entity: MarketingSpend\n",
	})

	refs, err := ModelReferenceFinder{}.Find(root, "services/marketing", "table_b")
	require.NoError(t, err)
	assert.Empty(t, refs.Order)
	assert.False(t, refs.DeclaresKind)
	assert.True(t, refs.OthersDeclareKind, "an entity under config.meta counts")
}

func TestModelReferenceFinder_AMissingProjectIsAnError(t *testing.T) {
	_, err := ModelReferenceFinder{}.Find(t.TempDir(), "services/absent", "table_b")
	assert.ErrorContains(t, err, "services/absent")
}
