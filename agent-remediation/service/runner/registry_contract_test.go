package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestRegistry_HoldsTheContractSteps(t *testing.T) {
	r := NewRegistry(Collaborators{})
	for name, kind := range map[workflow.StepName]step.Kind{
		"contract_file":    step.KindContext,
		"failing_siblings": step.KindContext,
		"contract_entry":   step.KindContext,
		"prior_attempts":   step.KindContext,
		"python-contract":  step.KindPrompt,
		"python-parse":     step.KindPrompt,
		"csv-contract":     step.KindPrompt,
	} {
		_, m, ok := r.Lookup(name)
		assert.True(t, ok, name)
		assert.Equal(t, kind, m.Kind, name)
	}
}

func TestRegistry_EvidenceTypeKnowsTheContractKeys(t *testing.T) {
	r := NewRegistry(Collaborators{})
	for key, want := range map[remediation.EvidenceKey]any{
		remediation.KeyContractFile:  remediation.ContractFile{},
		remediation.KeyContractEntry: remediation.Text(""),
		remediation.KeyPriorAttempts: []prompt.PriorAttempt{},
	} {
		f, ok := r.EvidenceType(key)
		assert.True(t, ok, key)
		assert.IsType(t, want, f(), key)
	}
}
