package ports

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// CompileCommand is a resolved compile leg: the argv to run plus the absolute
// paths where the service's tool leaves its outputs.
type CompileCommand struct {
	// Argv is the compile command to execute.
	Argv []string
	// ManifestPath is where the tool writes manifest.json.
	ManifestPath string
	// PartialParsePath is where the tool writes its partial-parse msgpack.
	// It derives beside ManifestPath unless the service declares otherwise.
	PartialParsePath string
}

// WrapperCachePolicy states what a team's dbt wrapper does to dbt's parse
// cache, which decides whether a compile's partial parse can be reused.
type WrapperCachePolicy string

const (
	// WrapperCacheRequired means the wrapper reliably writes a reusable
	// partial parse at the declared path.
	WrapperCacheRequired WrapperCachePolicy = "required"
	// WrapperCacheOpaque means the wrapper's cache behaviour is unknown, so
	// its partial parse must not be assumed reusable. This is the default
	// for any service that does not declare otherwise.
	WrapperCacheOpaque WrapperCachePolicy = "opaque"
)

// CommandResolver resolves the container argv for every dbt operation the
// executor dispatches to a Kubernetes Job. Implementations resolve a team's
// declared command dialect with precedence service override > deployment
// default > built-in plain dbt, so events keep carrying intent (node type +
// table name) and command strings exist only at Job-build time.
type CommandResolver interface {
	// NodeCommand returns the argv for op against node, for the given
	// service. For OperationRun, nt (model, seed, or snapshot) selects the
	// verb; OperationTest and OperationBuild resolve to a fixed dbt verb
	// regardless of nt.
	NodeCommand(serviceName string, op pkg_model.Operation, nt pkg_model.NodeType, node string) []string
	// SeedBuildCommand returns the argv for building a seed into the
	// release's candidate schema. When the service has no seed_build
	// template it falls back to the seed command; schema routing then relies
	// on the DBT_TARGET_SCHEMA env var contract.
	SeedBuildCommand(serviceName, node, targetSchema string) []string
	// CompileCommand returns the resolved compile leg for the service.
	CompileCommand(serviceName string) CompileCommand
	// WrapperCachePolicy returns what the service's wrapper does to dbt's
	// parse cache. Services that declare nothing are opaque.
	WrapperCachePolicy(serviceName string) WrapperCachePolicy
	// RuntimeContext returns canonical JSON describing the service's whole
	// resolved command surface: the raw (unsubstituted) templates, the
	// compile paths, and the wrapper policy. It is stable for identical
	// configuration, so it can be hashed into a parse-context digest that
	// decides whether a previously produced artifact is still reusable.
	RuntimeContext(serviceName string) string
}
