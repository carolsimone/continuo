package commandcfg

import (
	"fmt"
	"path"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
)

// Resolver resolves argv templates with precedence: a services.<name>
// override, then the (always-complete) default block.
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
// pick, walking the service override then the default. Because the default
// is always complete, template always returns a non-nil template.
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
// regardless of nt; OperationFullRefresh is resolved by fullRefreshCommand. The
// default block is always complete (built-in when no file, validated at load
// when a file exists), so the run/test/build templates are never nil.
func (r *Resolver) NodeCommand(serviceName string, op pkg_model.Operation, nt pkg_model.NodeType, node string) ([]string, error) {
	var pick func(*opSet) []string
	switch op {
	case pkg_model.OperationTest:
		pick = func(o *opSet) []string { return o.Test }
	case pkg_model.OperationBuild:
		pick = func(o *opSet) []string { return o.Build }
	case pkg_model.OperationFullRefresh:
		return r.fullRefreshCommand(serviceName, nt, node)
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
	return substitute(r.template(serviceName, pick), map[string]string{"node": node}), nil
}

// fullRefreshCommand resolves a full refresh. A seed uses the seed template,
// which rebuilds the table on every production load. A model uses the
// full_refresh template of the block that governs the service — its override
// when it has one, else the default — and never falls through from an override
// to the default, whose binary the team's image may not carry. Every failure
// wraps events.ErrPermanent: no retry can supply a missing command.
func (r *Resolver) fullRefreshCommand(serviceName string, nt pkg_model.NodeType, node string) ([]string, error) {
	vals := map[string]string{"node": node}
	switch nt {
	case pkg_model.NodeTypeDbtSeed:
		return substitute(r.template(serviceName, func(o *opSet) []string { return o.Seed }), vals), nil
	case pkg_model.NodeTypeDbtModel:
	default:
		return nil, fmt.Errorf("%w: full refresh is not supported for node type %q", events.ErrPermanent, nt)
	}
	block, name := r.cfg.Default, "default"
	if ops := r.cfg.Services[serviceName]; ops != nil {
		block, name = ops, "services."+serviceName
	}
	if block.FullRefresh == nil {
		return nil, fmt.Errorf("%w: dbt-commands %s defines no full_refresh command", events.ErrPermanent, name)
	}
	return substitute(block.FullRefresh, vals), nil
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

// ParseCommand resolves the argv that runs the team's dbt parse. The default
// block is always complete, so a parse template is always present.
func (r *Resolver) ParseCommand(serviceName string) []string {
	t := r.template(serviceName, func(o *opSet) []string { return o.Parse })
	return append([]string(nil), t...)
}

// PartialParsePath resolves the absolute path where the service's dbt writes
// partial_parse.msgpack. An explicit compile.partial_parse_path wins; otherwise
// it is the manifest_path's directory + "/partial_parse.msgpack".
func (r *Resolver) PartialParsePath(serviceName string) string {
	spec := r.cfg.Default.Compile
	if ops := r.cfg.Services[serviceName]; ops != nil && ops.Compile != nil {
		spec = ops.Compile
	}
	if spec.PartialParsePath != "" {
		return spec.PartialParsePath
	}
	return path.Dir(spec.ManifestPath) + "/partial_parse.msgpack"
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
