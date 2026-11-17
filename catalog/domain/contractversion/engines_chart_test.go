package contractversion_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
)

// chartSupportedEngines reads the engines the chart accepts for
// validation.engine from the `$supported := list ...` line of
// continuo.validation.image in templates/_helpers.tpl.
func chartSupportedEngines(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "continuo", "templates", "_helpers.tpl"))
	require.NoError(t, err)
	line := regexp.MustCompile(`\$supported := list((?: "[^"]+")+)`).FindSubmatch(raw)
	require.NotNil(t, line, "_helpers.tpl has a `$supported := list ...` line")
	var engines []string
	for _, quoted := range regexp.MustCompile(`"([^"]+)"`).FindAllSubmatch(line[1], -1) {
		engines = append(engines, string(quoted[1]))
	}
	return engines
}

// engineServerTypes is what ServerType maps for each candidate engine name:
// the engines it accepts, with their server types.
func engineServerTypes(candidates []string) map[string]string {
	accepted := map[string]string{}
	for _, engine := range candidates {
		if serverType, err := contractversion.ServerType(engine); err == nil {
			accepted[engine] = serverType
		}
	}
	return accepted
}

// The engines ServerType maps are exactly the ones the chart accepts, and each
// maps to the server type of its own name. An engine the chart allows but
// catalog cannot map would render fine and then stop catalog at boot, and an
// engine catalog maps that the chart refuses would never be installed.
func TestServerTypes_MatchTheChartsEngines(t *testing.T) {
	chart := chartSupportedEngines(t)
	require.NotEmpty(t, chart)

	// The chart's own list is the candidate set, plus the names a mapping
	// could wrongly accept.
	candidates := append(slices.Clone(chart), "", "mysql", "duckdb", "bigquery", "Postgres", "snowflake")
	accepted := engineServerTypes(candidates)

	for _, engine := range chart {
		assert.Equal(t, engine, accepted[engine], "chart engine %q maps to the server type of its own name", engine)
	}
	for engine := range accepted {
		if engine == "" {
			assert.Equal(t, "postgres", accepted[engine], "an unset engine is postgres, the chart's default")
			continue
		}
		assert.Contains(t, chart, engine, "ServerType accepts %q but the chart does not", engine)
	}
}
