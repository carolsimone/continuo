package routing

import (
	"fmt"
	"path/filepath"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
)

// ResolveExecution pins the exact command a worker runs for a claimed task, and
// how it runs it. It is pure: the caller resolves once, at claim, and persists
// the result, so a later configuration reload cannot change the command of a
// task that has already been attempted.
//
// The path follows from the resolved program. Plain dbt runs in-process against
// the hydrated manifest (native). Anything else is a team-supplied wrapper, and
// what it does to dbt's parse cache decides whether its partial parse can be
// reused: a service that declares its wrapper writes a reusable one gets
// wrapper_required, and a service that declares nothing gets wrapper_opaque.
//
// A service whose dialect resolves to no command cannot be run at all, and no
// retry changes that, so it fails permanently.
func ResolveExecution(commands ports.CommandResolver, dep *model.Deployment) ([]string, model.ExecutionPath, error) {
	cmd := dep.Command()
	argv := commands.NodeCommand(cmd.ServiceName, pkgmodel.Operation(cmd.Operation),
		pkgmodel.NodeType(cmd.NodeType), cmd.TableName)
	if len(argv) == 0 || argv[0] == "" {
		return nil, "", fmt.Errorf("%w: empty resolved argv", pkgevents.ErrPermanent)
	}
	if filepath.Base(argv[0]) == "dbt" {
		return argv, model.ExecutionPathNative, nil
	}
	if commands.WrapperCachePolicy(cmd.ServiceName) == ports.WrapperCacheRequired {
		return argv, model.ExecutionPathWrapperRequired, nil
	}
	return argv, model.ExecutionPathWrapperOpaque, nil
}
