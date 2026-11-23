package release_test

import (
	"github.com/carolsimone/continuo/pkg/domain/model"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParseTimings_ANilCountIsAbsentAndAnEmptyOneIsNot(t *testing.T) {
	t.Run("a nil count", func(t *testing.T) {
		got := release.NewParseTimings(10, 4, nil)
		assert.Nil(t, got.ContractsByFamily, "a parse result without a count reports none")
	})

	t.Run("an empty count", func(t *testing.T) {
		got := release.NewParseTimings(10, 4, map[model.ContractFamily]int{})
		require.NotNil(t, got.ContractsByFamily, "a release with no contracts reports an empty count")
		assert.Empty(t, got.ContractsByFamily)
	})

	t.Run("a count with families", func(t *testing.T) {
		got := release.NewParseTimings(10, 4, map[model.ContractFamily]int{"dbt": 3, "python": 2})
		assert.Equal(t, map[model.ContractFamily]int{"dbt": 3, "python": 2}, got.ContractsByFamily)
	})
}

func TestNewParseTimings_TwoZeroDurationsAreAbsent(t *testing.T) {
	got := release.NewParseTimings(0, 0, map[model.ContractFamily]int{"dbt": 1})
	assert.Nil(t, got.ParseMs, "a producer that predates the durations decodes both as zero")
	assert.Nil(t, got.ContractsMs)
	assert.Equal(t, map[model.ContractFamily]int{"dbt": 1}, got.ContractsByFamily, "the count is judged on its own")
}

func TestNewParseTimings_OneNonZeroDurationReportsBoth(t *testing.T) {
	cases := map[string]struct {
		parseMs, contractsMs int64
	}{
		"both set":              {parseMs: 4180, contractsMs: 1260},
		"only the parse":        {parseMs: 4180, contractsMs: 0},
		"only the contracts":    {parseMs: 0, contractsMs: 1260},
		"a parse of one second": {parseMs: 1000, contractsMs: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := release.NewParseTimings(tc.parseMs, tc.contractsMs, nil)
			require.NotNil(t, got.ParseMs)
			require.NotNil(t, got.ContractsMs)
			assert.Equal(t, tc.parseMs, *got.ParseMs)
			assert.Equal(t, tc.contractsMs, *got.ContractsMs)
		})
	}
}

func TestNewParseTimings_DoesNotShareTheCountWithItsCaller(t *testing.T) {
	counts := map[model.ContractFamily]int{"dbt": 3}
	got := release.NewParseTimings(1, 1, counts)
	counts["dbt"] = 99
	assert.Equal(t, map[model.ContractFamily]int{"dbt": 3}, got.ContractsByFamily)
}
