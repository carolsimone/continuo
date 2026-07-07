package commandcfg

import (
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
)

func TestDefaults_NodeCommand_MatchesPkgNodeType(t *testing.T) {
	r := Defaults()
	for _, nt := range []pkg_model.NodeType{
		pkg_model.NodeTypeDbtModel, pkg_model.NodeTypeDbtSeed, pkg_model.NodeTypeDbtSnapshot,
	} {
		assert.Equal(t, nt.Command("orders"), r.NodeCommand("any-service", nt, "orders"),
			"built-in default for %s must delegate to pkg NodeType.Command", nt)
	}
}

func TestDefaults_SeedBuildCommand_FallsBackToSeed(t *testing.T) {
	r := Defaults()
	assert.Equal(t, []string{"dbt", "seed", "--select", "fx"},
		r.SeedBuildCommand("any-service", "fx", "_candidate_rel1"),
		"without a seed_build template, seed-build uses the seed command (schema routed via DBT_TARGET_SCHEMA env)")
}

func TestDefaults_CompileCommand(t *testing.T) {
	argv, manifestPath := Defaults().CompileCommand("any-service")
	assert.Equal(t, []string{"dbt", "compile", "--profiles-dir", "/project"}, argv)
	assert.Equal(t, "/project/target/manifest.json", manifestPath)
}
