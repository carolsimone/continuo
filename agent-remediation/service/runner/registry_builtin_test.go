package runner_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/remediationfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/flow"
	"github.com/carolsimone/continuo/agent-remediation/files"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
)

// TestEveryRegisteredStepIsUsedByABuiltinFile: a step the registry builds but
// no built-in flow or type names is never run and never checked by the
// conformance suite, so each registered name must appear in some built-in file.
func TestEveryRegisteredStepIsUsedByABuiltinFile(t *testing.T) {
	set, err := remediationfile.Load(files.Builtin)
	require.NoError(t, err)

	used := map[flow.StepName]bool{}
	mark := func(refs ...flow.StepRef) {
		for _, r := range refs {
			used[r.Name] = true
		}
	}
	for _, f := range set.Flows {
		mark(f.Context...)
		mark(f.AcrossClusters...)
		mark(f.Gate...)
		mark(f.Approver)
		// An output step is named by the action of the flow's output.
		used[flow.StepName(f.OnPass.Action)] = true
		used[flow.StepName(f.OnGiveUp.Action)] = true
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
	set, err := remediationfile.Load(files.Builtin)
	require.NoError(t, err)

	warnings, err := runner.Validate(set, runner.NewRegistry(runner.Collaborators{}))
	require.NoError(t, err)
	require.Empty(t, warnings)
}

// TestBuiltinFlowLetsTheModelSeeCodeAndLogs: the built-in flow shows the model
// source code and the text failures and tools report, and no rows of data.
func TestBuiltinFlowLetsTheModelSeeCodeAndLogs(t *testing.T) {
	set, err := remediationfile.Load(files.Builtin)
	require.NoError(t, err)

	fl, ok := set.Flows["rejected-release"]
	require.True(t, ok)
	require.Equal(t, []flow.DataClass{flow.ClassCode, flow.ClassLogs}, fl.ModelMaySee)
}

// TestBuiltinTypesCheckSiblingsBeforeImagePackages: a python or csv cluster
// whose sibling also failed and whose release records no image packages
// qualifies for two declines. The sibling is checked first, so every built-in
// type that runs both steps runs failing_siblings before image_packages, and
// the reason recorded is the sibling's.
func TestBuiltinTypesCheckSiblingsBeforeImagePackages(t *testing.T) {
	set, err := remediationfile.Load(files.Builtin)
	require.NoError(t, err)

	checked := 0
	for name, ty := range set.Types {
		siblings, images := -1, -1
		for i, r := range ty.Context {
			switch r.Name {
			case "failing_siblings":
				siblings = i
			case "image_packages":
				images = i
			}
		}
		if images < 0 {
			continue
		}
		checked++
		require.GreaterOrEqual(t, siblings, 0, "type %s reads image packages but checks no sibling", name)
		require.Less(t, siblings, images, "type %s checks image packages before its siblings", name)
	}
	require.Equal(t, 3, checked, "python-validation, csv-validation and python-parse read image packages")
}
