// Package commandcfg resolves the container argv for every dbt operation the
// executor dispatches, from an optional deploy-time dbt-commands.yaml. A
// missing file yields the built-in plain-dbt commands; an invalid file is a
// startup error.
package commandcfg

import "regexp"

// compileSpec is a compile override: the team's compile argv plus the absolute
// paths where their tool writes its outputs. partial_parse_path is optional and
// only needed by a wrapper that relocates the file; otherwise it derives beside
// manifest.json, where dbt writes it by default.
type compileSpec struct {
	Command          []string `yaml:"command"`
	ManifestPath     string   `yaml:"manifest_path"`
	PartialParsePath string   `yaml:"partial_parse_path,omitempty"`
}

// workerSpec declares how a team's dbt wrapper behaves in a reusable worker.
type workerSpec struct {
	// WrapperCache is "required" or "opaque". Empty means the service made
	// no claim, which resolves to opaque.
	WrapperCache string `yaml:"wrapper_cache,omitempty"`
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
	// Worker is valid on a services.<name> block only; load rejects it on the
	// default block, which describes plain dbt rather than a team's wrapper.
	Worker *workerSpec `yaml:"worker,omitempty"`
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
	return missing
}
