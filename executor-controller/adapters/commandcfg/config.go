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
	// PartialParsePath is the absolute path where the team's dbt writes
	// partial_parse.msgpack. Empty defaults to
	// dirname(ManifestPath)/partial_parse.msgpack. When set, it must live in
	// the same directory as ManifestPath: the executor mounts the parse-cache
	// volume at dirname(PartialParsePath) in every run/seed/build pod for this
	// team's image, and a directory that differs from the --target-path
	// dbt writes both artifacts into would shadow the team's actual project
	// files instead of just the disposable target dir.
	PartialParsePath string `yaml:"partial_parse_path"`
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
	Parse     []string     `yaml:"parse"`
}

// fileConfig is the root dbt-commands.yaml document.
type fileConfig struct {
	Default  *opSet            `yaml:"default"`
	Services map[string]*opSet `yaml:"services"`
}

// placeholderRe matches {{ name }} tokens inside argv elements, tolerating
// inner whitespace ({{node}} and {{ node }} are equivalent).
var placeholderRe = regexp.MustCompile(`\{\{\s*([a-zA-Z_]+)\s*\}\}`)

// missingKeys returns the required command keys this opSet does not define.
// An empty result means the opSet is complete. Every configured block (the
// default and each service override) must be complete so no dispatched job can
// fall through to a command the team's image cannot run.
func (o *opSet) missingKeys() []string {
	var missing []string
	if o.Run == nil {
		missing = append(missing, "run")
	}
	if o.Seed == nil {
		missing = append(missing, "seed")
	}
	if o.Snapshot == nil {
		missing = append(missing, "snapshot")
	}
	if o.Test == nil {
		missing = append(missing, "test")
	}
	if o.Build == nil {
		missing = append(missing, "build")
	}
	if o.SeedBuild == nil {
		missing = append(missing, "seed_build")
	}
	if o.Compile == nil {
		missing = append(missing, "compile")
	}
	if o.Parse == nil {
		missing = append(missing, "parse")
	}
	return missing
}
