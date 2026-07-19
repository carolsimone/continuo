package ports

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
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
	// CompileCommand returns the compile argv and the absolute path where
	// the service's tool writes manifest.json after compiling.
	CompileCommand(serviceName string) (argv []string, manifestPath string)
	// ParseCommand returns the argv that runs the team's dbt parse for the
	// compile Job's parse-export/rehearsal containers.
	ParseCommand(serviceName string) []string
	// PartialParsePath returns the absolute in-container path where the
	// service's dbt writes partial_parse.msgpack (explicit
	// compile.partial_parse_path, else the manifest_path's directory).
	PartialParsePath(serviceName string) string
}
