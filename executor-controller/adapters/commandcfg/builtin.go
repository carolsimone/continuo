package commandcfg

import pkg_model "github.com/carolsimone/continuo/pkg/domain/model"

// builtinDefault returns the complete plain-dbt command set used when no
// dbt-commands.yaml file is configured. run/seed/snapshot delegate to the pkg
// model NodeType.Command (the single source for those verbs) with the
// {{ node }} placeholder as the table name; test/build/seed_build/compile are
// the executor's own plain-dbt commands. It is always complete —
// TestBuiltinDefault_IsComplete pins that it passes the same checks applied to
// file-provided blocks.
func builtinDefault() *opSet {
	node := "{{ node }}"
	return &opSet{
		Run:       pkg_model.NodeTypeDbtModel.Command(node),
		Seed:      pkg_model.NodeTypeDbtSeed.Command(node),
		Snapshot:  pkg_model.NodeTypeDbtSnapshot.Command(node),
		Test:      []string{"dbt", "test", "--select", node},
		Build:     []string{"dbt", "build", "--select", node},
		SeedBuild: []string{"dbt", "seed", "--select", node},
		Compile: &compileSpec{
			Command:      []string{"dbt", "compile", "--profiles-dir", "/project"},
			ManifestPath: "/project/target/manifest.json",
		},
	}
}
