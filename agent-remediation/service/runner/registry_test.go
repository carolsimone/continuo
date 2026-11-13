package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestRegistry_EveryStepDescribesItself(t *testing.T) {
	r := NewRegistry(Collaborators{})
	for _, m := range r.All() {
		assert.NotEmpty(t, m.Description, m.Name)
		assert.Contains(t, []step.Kind{step.KindContext, step.KindPrompt, step.KindCheck, step.KindApprover, step.KindOutput}, m.Kind, m.Name)
		if m.Kind == step.KindCheck {
			assert.NotEmpty(t, m.Stages, m.Name)
		}
		for _, k := range m.Provides {
			_, ok := r.EvidenceType(k)
			assert.True(t, ok, "%s provides %s with no registered type", m.Name, k)
		}
	}
}

type fakeStep struct{ meta step.Meta }

func (f fakeStep) Meta() step.Meta { return f.meta }

func TestRegistry_RegisterRefusesADuplicateName(t *testing.T) {
	r := NewRegistry(Collaborators{})
	r.register(fakeStep{step.Meta{Name: "a", Kind: step.KindContext}})
	assert.PanicsWithValue(t, `runner: step "a" registered twice`, func() {
		r.register(fakeStep{step.Meta{Name: "a", Kind: step.KindOutput}})
	})
}

func TestRegistry_RegisterRefusesAnEmptyName(t *testing.T) {
	r := NewRegistry(Collaborators{})
	assert.Panics(t, func() { r.register(fakeStep{}) })
}

func TestRegistry_LookupAndAllReturnRegisteredSteps(t *testing.T) {
	r := NewRegistry(Collaborators{})
	r.register(fakeStep{step.Meta{Name: "b", Kind: step.KindContext, Description: "b"}})
	r.register(fakeStep{step.Meta{Name: "a", Kind: step.KindContext, Description: "a"}})

	s, m, ok := r.Lookup("a")
	assert.True(t, ok)
	assert.Equal(t, workflow.StepName("a"), m.Name)
	assert.Equal(t, fakeStep{m}, s)

	_, _, ok = r.Lookup("missing")
	assert.False(t, ok)

	var names []workflow.StepName
	for _, m := range r.All() {
		names = append(names, m.Name)
	}
	assert.Equal(t, []workflow.StepName{"a", "b"}, names)
}

func TestRegistry_EvidenceTypeKnowsTheRunnerProvidedKeys(t *testing.T) {
	r := NewRegistry(Collaborators{})

	f, ok := r.EvidenceType(remediation.KeyFailure)
	assert.True(t, ok)
	assert.Equal(t, remediation.Failure{}, f())

	d, ok := r.EvidenceType(remediation.KeyDraft)
	assert.True(t, ok)
	assert.Equal(t, remediation.Draft{}, d())

	_, ok = r.EvidenceType("no_such_key")
	assert.False(t, ok)
}
