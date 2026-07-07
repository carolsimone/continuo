package commandcfg

import (
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// Resolver resolves argv templates with precedence
// services.<name> > default > built-in plain dbt.
type Resolver struct {
	cfg *fileConfig
}

var _ ports.CommandResolver = (*Resolver)(nil)

// Defaults returns a Resolver with no overrides: every operation resolves to
// the built-in plain-dbt command.
func Defaults() *Resolver {
	return &Resolver{cfg: &fileConfig{}}
}

// template returns the first non-nil template for the operation selected by
// pick, walking service override then deployment default. Nil means "use the
// built-in".
func (r *Resolver) template(serviceName string, pick func(*opSet) []string) []string {
	if ops := r.cfg.Services[serviceName]; ops != nil {
		if t := pick(ops); t != nil {
			return t
		}
	}
	if r.cfg.Default != nil {
		if t := pick(r.cfg.Default); t != nil {
			return t
		}
	}
	return nil
}

// NodeCommand resolves the production argv for nt against node.
func (r *Resolver) NodeCommand(serviceName string, nt pkg_model.NodeType, node string) []string {
	pick := func(o *opSet) []string {
		switch nt {
		case pkg_model.NodeTypeDbtSeed:
			return o.Seed
		case pkg_model.NodeTypeDbtSnapshot:
			return o.Snapshot
		default:
			return o.Run
		}
	}
	if t := r.template(serviceName, pick); t != nil {
		return substitute(t, map[string]string{"node": node})
	}
	return nt.Command(node)
}

// SeedBuildCommand resolves the argv for building a seed into targetSchema.
// Without a seed_build template it falls back to the seed command; schema
// routing then relies on the DBT_TARGET_SCHEMA env var contract.
func (r *Resolver) SeedBuildCommand(serviceName, node, targetSchema string) []string {
	if t := r.template(serviceName, func(o *opSet) []string { return o.SeedBuild }); t != nil {
		return substitute(t, map[string]string{"node": node, "target_schema": targetSchema})
	}
	return r.NodeCommand(serviceName, pkg_model.NodeTypeDbtSeed, node)
}

// CompileCommand resolves the compile argv and manifest.json path.
func (r *Resolver) CompileCommand(serviceName string) ([]string, string) {
	if ops := r.cfg.Services[serviceName]; ops != nil && ops.Compile != nil {
		return append([]string(nil), ops.Compile.Command...), ops.Compile.ManifestPath
	}
	if r.cfg.Default != nil && r.cfg.Default.Compile != nil {
		return append([]string(nil), r.cfg.Default.Compile.Command...), r.cfg.Default.Compile.ManifestPath
	}
	return []string{"dbt", "compile", "--profiles-dir", "/project"}, "/project/target/manifest.json"
}

// substitute replaces {{ name }} tokens in each argv element with vals[name].
// Unknown names were rejected at load time, so they cannot appear here; the
// template slice is never mutated.
func substitute(tpl []string, vals map[string]string) []string {
	out := make([]string, len(tpl))
	for i, elem := range tpl {
		out[i] = placeholderRe.ReplaceAllStringFunc(elem, func(tok string) string {
			name := placeholderRe.FindStringSubmatch(tok)[1]
			if v, ok := vals[name]; ok {
				return v
			}
			return tok
		})
	}
	return out
}
