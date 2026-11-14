package runner_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
)

// TestEveryRegisteredStepIsUsedByABuiltinFile: a step the registry builds but
// no built-in flow or type names is never run and never checked by the
// conformance suite, so each registered name must appear in some built-in file.
func TestEveryRegisteredStepIsUsedByABuiltinFile(t *testing.T) {
	set, err := workflowfile.Load(workflows.Builtin)
	require.NoError(t, err)

	used := map[workflow.StepName]bool{}
	mark := func(refs ...workflow.StepRef) {
		for _, r := range refs {
			used[r.Name] = true
		}
	}
	for _, f := range set.Flows {
		mark(f.Context...)
		mark(f.AcrossClusters...)
		mark(f.Gate...)
		// An output step is named by the action of the flow's output.
		used[workflow.StepName(f.OnPass.Action)] = true
		used[workflow.StepName(f.OnGiveUp.Action)] = true
	}
	for _, ty := range set.Types {
		mark(ty.Context...)
		mark(ty.Agent.Prompt)
		mark(ty.Agent.Tools...)
		mark(ty.Preflight...)
	}

	var unused []string
	for _, m := range runner.NewRegistry(runner.Collaborators{}).All() {
		if !used[m.Name] {
			unused = append(unused, string(m.Name))
		}
	}
	sort.Strings(unused)
	require.Empty(t, unused, "registered steps no built-in flow or type file uses")
}

// TestBuiltinFilesValidate: the files built into the binary pass every check
// of Validate against the registry, with no warning.
func TestBuiltinFilesValidate(t *testing.T) {
	set, err := workflowfile.Load(workflows.Builtin)
	require.NoError(t, err)

	warnings, err := runner.Validate(set, runner.NewRegistry(runner.Collaborators{}))
	require.NoError(t, err)
	require.Empty(t, warnings)
}
