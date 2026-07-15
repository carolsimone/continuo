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

// Defaults returns a Resolver backed by the complete built-in plain-dbt
// command set. Used when no dbt-commands.yaml is configured.
func Defaults() *Resolver {
	return &Resolver{cfg: &fileConfig{Default: builtinDefault()}}
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

// NodeCommand resolves the argv for op against node. OperationRun dispatches on
// nt (model/seed/snapshot); OperationTest and OperationBuild resolve a fixed key
// regardless of nt. The default block is always complete (built-in when no file,
// validated at load when a file exists), so template never returns nil.
func (r *Resolver) NodeCommand(serviceName string, op pkg_model.Operation, nt pkg_model.NodeType, node string) []string {
	var pick func(*opSet) []string
	switch op {
	case pkg_model.OperationTest:
		pick = func(o *opSet) []string { return o.Test }
	case pkg_model.OperationBuild:
		pick = func(o *opSet) []string { return o.Build }
	default: // OperationRun
		pick = func(o *opSet) []string {
			switch nt {
			case pkg_model.NodeTypeDbtSeed:
				return o.Seed
			case pkg_model.NodeTypeDbtSnapshot:
				return o.Snapshot
			default:
				return o.Run
			}
		}
	}
	return substitute(r.template(serviceName, pick), map[string]string{"node": node})
}

// SeedBuildCommand resolves the argv for building a seed into targetSchema.
func (r *Resolver) SeedBuildCommand(serviceName, node, targetSchema string) []string {
	t := r.template(serviceName, func(o *opSet) []string { return o.SeedBuild })
	return substitute(t, map[string]string{"node": node, "target_schema": targetSchema})
}

// CompileCommand resolves the compile argv and manifest.json path. The default
// block is always complete, so a compile spec is always present.
func (r *Resolver) CompileCommand(serviceName string) ([]string, string) {
	if ops := r.cfg.Services[serviceName]; ops != nil && ops.Compile != nil {
		return append([]string(nil), ops.Compile.Command...), ops.Compile.ManifestPath
	}
	d := r.cfg.Default.Compile
	return append([]string(nil), d.Command...), d.ManifestPath
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
