package release

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// A node built from its own candidate code gets the op of its runtime; any
// other node of the validation set is an unchanged upstream, copied from its
// production schema whatever its type.
func TestValidationOpFor(t *testing.T) {
	rebuilt := map[string]bool{"m": true, "s": true, "py": true, "csv": true, "api": true, "tst": true}
	cases := []struct {
		id         string
		nodeType   pkg_model.NodeType
		wantOp     pkg_model.ValidationOp
		wantSchema string
	}{
		{"m", pkg_model.NodeTypeDbtModel, pkg_model.ValidationOpBuildFromSQL, ""},
		{"s", pkg_model.NodeTypeDbtSnapshot, pkg_model.ValidationOpBuildFromSQL, ""},
		{"py", pkg_model.NodeTypePythonNode, pkg_model.ValidationOpBuildFromColumns, ""},
		{"csv", pkg_model.NodeTypePythonCsv, pkg_model.ValidationOpBuildFromColumns, ""},
		{"api", pkg_model.NodeTypePythonApi, pkg_model.ValidationOpBuildFromColumns, ""},
		{"tst", pkg_model.NodeTypeDbtTest, pkg_model.ValidationOpCheckBinds, ""},
		{"up-model", pkg_model.NodeTypeDbtModel, pkg_model.ValidationOpCloneFromProd, "prod_schema"},
		{"up-python", pkg_model.NodeTypePythonNode, pkg_model.ValidationOpCloneFromProd, "prod_schema"},
		{"up-seed", pkg_model.NodeTypeDbtSeed, pkg_model.ValidationOpCloneFromProd, "prod_schema"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			n := Node{UniqueID: tc.id, NodeType: string(tc.nodeType), SchemaName: "prod_schema"}

			op, schema := ValidationOpFor(n, rebuilt)

			assert.Equal(t, tc.wantOp, op)
			assert.Equal(t, tc.wantSchema, schema)
		})
	}
}

// The changed closure is always built from candidate code. A verification's
// context rebuild is built only when it lands in the validation set.
func TestRebuiltFromCandidateSet(t *testing.T) {
	got := RebuiltFromCandidateSet(
		[]string{"a", "b"},
		[]string{"ctx-in", "ctx-out"},
		[]string{"a", "b", "ctx-in", "up"},
	)

	assert.Equal(t, map[string]bool{"a": true, "b": true, "ctx-in": true}, got)
}
