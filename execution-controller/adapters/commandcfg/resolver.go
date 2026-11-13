package commandcfg

import (
	"fmt"
	"path"

	"github.com/carolsimone/continuo/execution-controller/domain/deploy"
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
// nt (model/seed/snapshot) — a seed resolves to the block's plain Seed
// template, the one every automatic seed load runs, never SeedFullRefresh (what
// it does to the table depends on the warehouse and the seed's +full_refresh,
// see deploy/continuo/files/dbt-commands.yaml); OperationTest and
// OperationBuild resolve a fixed key regardless of nt; OperationFullRefresh is
// resolved by fullRefreshCommand, which resolves a model to FullRefresh and a
// seed to SeedFullRefresh. The default block is always complete (built-in when
// no file, validated at load when a file exists), so the run/test/build
// templates are never nil. An ingested node is never run, so every operation on
// one fails with an error that wraps events.ErrPermanent and
// deploy.ErrInvalidNodeType, the reason the attempt's failure report gives.
func (r *Resolver) NodeCommand(serviceName string, op pkg_model.Operation, nt pkg_model.NodeType, node string) ([]string, error) {
	if nt.IsIngested() {
		return nil, fmt.Errorf("%w: %w: node type %q has no command", events.ErrPermanent, deploy.ErrInvalidNodeType, nt)
	}
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

// fullRefreshCommand resolves a full refresh of a node type the contract marks
// as supporting one: a model to its FullRefresh template, a seed to its
// SeedFullRefresh template — never to the plain Run/Seed template, which is the
// command an automatic load runs. Either template is resolved
// from the block that governs the service — its override when it has one, else
// the default — and this never falls through from an override to the default,
// whose binary the team's image may not carry. Every failure wraps
// events.ErrPermanent: no retry can supply a missing command.
func (r *Resolver) fullRefreshCommand(serviceName string, nt pkg_model.NodeType, node string) ([]string, error) {
	if !nt.SupportsFullRefresh() {
		return nil, fmt.Errorf("%w: full refresh is not supported for node type %q", events.ErrPermanent, nt)
	}
	pick, key := fullRefreshTemplate(nt)
	if pick == nil {
		return nil, fmt.Errorf("%w: no full-refresh command key is mapped for node type %q", events.ErrPermanent, nt)
	}
	block, name := r.governingBlock(serviceName)
	tpl := pick(block)
	if tpl == nil {
		return nil, fmt.Errorf("%w: dbt-commands %s defines no %s command", events.ErrPermanent, name, key)
	}
	return substitute(tpl, map[string]string{"node": node}), nil
}

// FullRefreshBlock names the block that governs the service and reports
// whether it defines the full refresh command of nt, without falling through
// from an override to the default.
func (r *Resolver) FullRefreshBlock(serviceName string, nt pkg_model.NodeType) (string, bool) {
	block, name := r.governingBlock(serviceName)
	pick, _ := fullRefreshTemplate(nt)
	if pick == nil {
		return name, false
	}
	return name, pick(block) != nil
}

// governingBlock is the block that governs the service, its override when it
// has one, else the default, with the name an error gives it.
func (r *Resolver) governingBlock(serviceName string) (*opSet, string) {
	if ops := r.cfg.Services[serviceName]; ops != nil {
		return ops, "services." + serviceName
	}
	return r.cfg.Default, "default"
}

// fullRefreshTemplate picks the full refresh template of nt from a block, with
// the key that names it: FullRefresh for a model, SeedFullRefresh for a seed;
// nil for any other node type.
func fullRefreshTemplate(nt pkg_model.NodeType) (func(*opSet) []string, string) {
	switch nt {
	case pkg_model.NodeTypeDbtModel:
		return func(o *opSet) []string { return o.FullRefresh }, "full_refresh"
	case pkg_model.NodeTypeDbtSeed:
		return func(o *opSet) []string { return o.SeedFullRefresh }, "seed_full_refresh"
	default:
		return nil, ""
	}
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
