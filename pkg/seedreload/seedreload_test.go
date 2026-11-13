package seedreload_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/seedreload"
)

func TestChangeOf(t *testing.T) {
	assert.Equal(t, seedreload.ChangeRows, seedreload.ChangeOf("", ""))
	assert.Equal(t, seedreload.ChangeRows, seedreload.ChangeOf("sha256:a", ""))
	assert.Equal(t, seedreload.ChangeRows, seedreload.ChangeOf("", "sha256:a"))
	assert.Equal(t, seedreload.ChangeRows, seedreload.ChangeOf("sha256:a", "sha256:a"))
	assert.Equal(t, seedreload.ChangeColumns, seedreload.ChangeOf("sha256:e5a0", "sha256:0d3c"))
}

func TestFindingsOutside(t *testing.T) {
	f := seedreload.Findings{Dependents: []seedreload.Dependent{
		{Schema: "analytics", Name: "fx_rates_current", Allowed: true}, {Schema: "BI", Name: "FX_Rates_Report", Kind: "view", Owner: "bi"}}}
	require.Len(t, f.Outside(), 1)
	assert.Equal(t, "bi.fx_rates_report", f.Outside()[0].Object())
	assert.Equal(t, []string{}, seedreload.NormalizeObjects(nil))
}

func TestNormalizeObjects(t *testing.T) {
	assert.Equal(t, []string{"a.b", "c.d"}, seedreload.NormalizeObjects([]string{" C.D ", "a.b", "A.B", "", "  "}))
}
