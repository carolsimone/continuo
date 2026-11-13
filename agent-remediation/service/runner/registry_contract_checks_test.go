package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestRegistry_HoldsTheContractChecks(t *testing.T) {
	r := NewRegistry(Collaborators{})
	for _, name := range []workflow.StepName{
		"contract_paths_inside_folder",
		"yaml_reads",
		"node_identity_unchanged",
		"reads_kept",
		"csv_read_kept",
		"no_new_secret_ref",
		"declared_once",
		"contract_packages",
	} {
		_, m, ok := r.Lookup(name)
		require.True(t, ok, name)
		assert.Equal(t, step.KindCheck, m.Kind, name)
		assert.Equal(t, []workflow.Stage{workflow.StagePreflight}, m.Stages, name)
	}
}

func TestRegistry_EvidenceTypeKnowsThePackagedContract(t *testing.T) {
	f, ok := NewRegistry(Collaborators{}).EvidenceType(remediation.KeyPackagedContract)
	require.True(t, ok)
	assert.IsType(t, remediation.Bytes{}, f())
}
