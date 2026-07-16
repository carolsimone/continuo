package commandcfg

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
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

// compileSpec resolves the compile block with the usual precedence. The default
// block is always complete, so a spec is always present.
func (r *Resolver) compileSpec(serviceName string) *compileSpec {
	if ops := r.cfg.Services[serviceName]; ops != nil && ops.Compile != nil {
		return ops.Compile
	}
	return r.cfg.Default.Compile
}

// CompileCommand resolves the compile leg. A service that does not declare
// partial_parse_path gets the dbt default location: beside manifest.json.
func (r *Resolver) CompileCommand(serviceName string) ports.CompileCommand {
	spec := r.compileSpec(serviceName)
	partial := spec.PartialParsePath
	if partial == "" {
		partial = filepath.Join(filepath.Dir(spec.ManifestPath), "partial_parse.msgpack")
	}
	return ports.CompileCommand{
		Argv:             append([]string(nil), spec.Command...),
		ManifestPath:     spec.ManifestPath,
		PartialParsePath: partial,
	}
}

// WrapperCachePolicy resolves the service's declared wrapper-cache behaviour.
// Anything undeclared — no override, no worker block, or an empty one — is
// opaque: continuo only assumes a reusable parse cache when a team says so.
func (r *Resolver) WrapperCachePolicy(serviceName string) ports.WrapperCachePolicy {
	if ops := r.cfg.Services[serviceName]; ops != nil && ops.Worker != nil && ops.Worker.WrapperCache != "" {
		return ports.WrapperCachePolicy(ops.Worker.WrapperCache)
	}
	if d := r.cfg.Default; d != nil && d.Worker != nil && d.Worker.WrapperCache != "" {
		return ports.WrapperCachePolicy(d.Worker.WrapperCache)
	}
	return ports.WrapperCacheOpaque
}

// runtimeContextDoc is the canonical shape of a service's resolved command
// surface. It is a struct, not a map, so encoding/json emits its fields in a
// fixed declared order and the JSON is byte-stable across processes.
type runtimeContextDoc struct {
	Run              []string `json:"run"`
	Seed             []string `json:"seed"`
	Snapshot         []string `json:"snapshot"`
	SeedBuild        []string `json:"seed_build"`
	Test             []string `json:"test"`
	Build            []string `json:"build"`
	Compile          []string `json:"compile"`
	ManifestPath     string   `json:"manifest_path"`
	PartialParsePath string   `json:"partial_parse_path"`
	WrapperCache     string   `json:"wrapper_cache"`
}

// RuntimeContext returns the canonical JSON of everything that decides what dbt
// actually runs for the service: the seven raw templates (unsubstituted, since
// the node name is per-invocation and must not enter the context), both compile
// paths, and the wrapper policy. Any config change a team makes to these
// changes the JSON, and therefore the parse-context digest derived from it.
func (r *Resolver) RuntimeContext(serviceName string) string {
	compile := r.CompileCommand(serviceName)
	body, err := json.Marshal(runtimeContextDoc{
		Run:              r.template(serviceName, func(o *opSet) []string { return o.Run }),
		Seed:             r.template(serviceName, func(o *opSet) []string { return o.Seed }),
		Snapshot:         r.template(serviceName, func(o *opSet) []string { return o.Snapshot }),
		SeedBuild:        r.template(serviceName, func(o *opSet) []string { return o.SeedBuild }),
		Test:             r.template(serviceName, func(o *opSet) []string { return o.Test }),
		Build:            r.template(serviceName, func(o *opSet) []string { return o.Build }),
		Compile:          compile.Argv,
		ManifestPath:     compile.ManifestPath,
		PartialParsePath: compile.PartialParsePath,
		WrapperCache:     string(r.WrapperCachePolicy(serviceName)),
	})
	if err != nil {
		// runtimeContextDoc holds only strings and string slices, which
		// encoding/json cannot fail on.
		panic(fmt.Sprintf("marshal runtime context for %s: %v", serviceName, err))
	}
	return string(body)
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
