// Package commandcfg resolves the container argv for every dbt operation the
// executor dispatches, from an optional deploy-time dbt-commands.yaml. A
// missing file yields the built-in plain-dbt commands; an invalid file is a
// startup error.
package commandcfg

import "regexp"

// compileSpec is a compile override: the team's compile argv plus the
// absolute path where their tool writes manifest.json.
type compileSpec struct {
	Command      []string `yaml:"command"`
	ManifestPath string   `yaml:"manifest_path"`
}

// opSet is one command set covering the operations continuo can dispatch.
// Nil slices mean "not overridden at this level".
type opSet struct {
	Run       []string     `yaml:"run"`
	Seed      []string     `yaml:"seed"`
	Snapshot  []string     `yaml:"snapshot"`
	SeedBuild []string     `yaml:"seed_build"`
	Test      []string     `yaml:"test"`
	Build     []string     `yaml:"build"`
	Compile   *compileSpec `yaml:"compile"`
}

// fileConfig is the root dbt-commands.yaml document.
type fileConfig struct {
	Default  *opSet            `yaml:"default"`
	Services map[string]*opSet `yaml:"services"`
}

// placeholderRe matches {{ name }} tokens inside argv elements, tolerating
// inner whitespace ({{node}} and {{ node }} are equivalent).
var placeholderRe = regexp.MustCompile(`\{\{\s*([a-zA-Z_]+)\s*\}\}`)
