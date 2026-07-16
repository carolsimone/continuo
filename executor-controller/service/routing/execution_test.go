package routing_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubCommandResolver returns a fixed argv and cache policy, and records the
// arguments it was asked to resolve.
type stubCommandResolver struct {
	ports.CommandResolver
	argv   []string
	policy ports.WrapperCachePolicy
	gotOp  pkgmodel.Operation
	gotNT  pkgmodel.NodeType
	gotNod string
	gotSvc string
}

func (r *stubCommandResolver) NodeCommand(serviceName string, op pkgmodel.Operation, nt pkgmodel.NodeType, node string) []string {
	r.gotSvc, r.gotOp, r.gotNT, r.gotNod = serviceName, op, nt, node
	return r.argv
}

func (r *stubCommandResolver) WrapperCachePolicy(string) ports.WrapperCachePolicy { return r.policy }

func workerDeployment(cmd command.DeployTask) *model.Deployment {
	return model.NewWorkerDeployment(cmd, uuid.New(), "pool-key", time.Now())
}

func TestResolveExecution_PlainDBTIsNative(t *testing.T) {
	resolver := &stubCommandResolver{argv: []string{"dbt", "run", "--select", "orders"}}
	dep := workerDeployment(command.DeployTask{
		ServiceName: "finance", Operation: "run", NodeType: "dbt-model", TableName: "orders",
	})

	argv, path, err := routing.ResolveExecution(resolver, dep)
	require.NoError(t, err)
	assert.Equal(t, []string{"dbt", "run", "--select", "orders"}, argv)
	assert.Equal(t, model.ExecutionPathNative, path)

	assert.Equal(t, "finance", resolver.gotSvc)
	assert.Equal(t, pkgmodel.Operation("run"), resolver.gotOp)
	assert.Equal(t, pkgmodel.NodeType("dbt-model"), resolver.gotNT)
	assert.Equal(t, "orders", resolver.gotNod)
}

// TestResolveExecution_AbsoluteDBTPathIsNative pins that the native check reads
// the program name, not the whole path a team's image happens to use.
func TestResolveExecution_AbsoluteDBTPathIsNative(t *testing.T) {
	resolver := &stubCommandResolver{argv: []string{"/usr/local/bin/dbt", "run"}}

	_, path, err := routing.ResolveExecution(resolver, workerDeployment(command.DeployTask{ServiceName: "finance"}))
	require.NoError(t, err)
	assert.Equal(t, model.ExecutionPathNative, path)
}

// TestResolveExecution_WrapperWithAReusableCacheIsRequired pins that a team
// entrypoint that reliably writes a partial parse gets the pinned-command path.
func TestResolveExecution_WrapperWithAReusableCacheIsRequired(t *testing.T) {
	resolver := &stubCommandResolver{
		argv:   []string{"/opt/team/run-dbt.sh", "run", "--select", "orders"},
		policy: ports.WrapperCacheRequired,
	}

	argv, path, err := routing.ResolveExecution(resolver, workerDeployment(command.DeployTask{ServiceName: "finance"}))
	require.NoError(t, err)
	assert.Equal(t, resolver.argv, argv)
	assert.Equal(t, model.ExecutionPathWrapperRequired, path)
}

// TestResolveExecution_WrapperWithAnUnknownCacheIsOpaque pins the default: a
// wrapper that declares nothing about its parse cache is not assumed reusable.
func TestResolveExecution_WrapperWithAnUnknownCacheIsOpaque(t *testing.T) {
	resolver := &stubCommandResolver{
		argv:   []string{"/opt/team/run-dbt.sh", "run"},
		policy: ports.WrapperCacheOpaque,
	}

	_, path, err := routing.ResolveExecution(resolver, workerDeployment(command.DeployTask{ServiceName: "finance"}))
	require.NoError(t, err)
	assert.Equal(t, model.ExecutionPathWrapperOpaque, path)
}

// TestResolveExecution_EmptyArgvIsPermanent pins that a service whose dialect
// resolves to nothing fails the task instead of running an empty command.
func TestResolveExecution_EmptyArgvIsPermanent(t *testing.T) {
	for name, argv := range map[string][]string{
		"nil":            nil,
		"empty_slice":    {},
		"empty_program":  {""},
		"blank_then_arg": {"", "run"},
	} {
		t.Run(name, func(t *testing.T) {
			resolver := &stubCommandResolver{argv: argv}

			_, path, err := routing.ResolveExecution(resolver, workerDeployment(command.DeployTask{ServiceName: "finance"}))
			require.Error(t, err)
			assert.ErrorIs(t, err, pkgevents.ErrPermanent,
				"an unresolvable command cannot become resolvable on retry")
			assert.Empty(t, path)
		})
	}
}
