package runner

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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

// fakeContext is a context-kind step, so it registers.
type fakeContext struct{ fakeStep }

func (fakeContext) Gather(context.Context, step.ContextInput) (step.ContextOutput, error) {
	return step.ContextOutput{}, nil
}

// fakePass is an output-kind step, so it registers.
type fakePass struct{ fakeStep }

func (fakePass) Pass(context.Context, uow.UnitOfWork, step.OutputInput, remediation.Verified) error {
	return nil
}

func TestRegistry_RegisterRefusesADuplicateName(t *testing.T) {
	r := NewRegistry(Collaborators{})
	r.register(fakeContext{fakeStep{step.Meta{Name: "a", Kind: step.KindContext}}})
	assert.PanicsWithValue(t, `runner: step "a" registered twice`, func() {
		r.register(fakePass{fakeStep{step.Meta{Name: "a", Kind: step.KindOutput}}})
	})
}

func TestRegistry_RegisterRefusesAnEmptyName(t *testing.T) {
	r := NewRegistry(Collaborators{})
	assert.Panics(t, func() { r.register(fakeStep{}) })
}

func TestRegistry_LookupAndAllReturnRegisteredSteps(t *testing.T) {
	r := NewRegistry(Collaborators{})
	r.register(fakeContext{fakeStep{step.Meta{Name: "b", Kind: step.KindContext, Description: "b"}}})
	r.register(fakeContext{fakeStep{step.Meta{Name: "a", Kind: step.KindContext, Description: "a"}}})

	s, m, ok := r.Lookup("a")
	assert.True(t, ok)
	assert.Equal(t, workflow.StepName("a"), m.Name)
	assert.Equal(t, fakeContext{fakeStep{m}}, s)

	_, _, ok = r.Lookup("missing")
	assert.False(t, ok)

	var names []workflow.StepName
	for _, m := range r.All() {
		if m.Name == "a" || m.Name == "b" {
			names = append(names, m.Name)
		}
	}
	assert.Equal(t, []workflow.StepName{"a", "b"}, names)

	all := r.All()
	assert.True(t, slices.IsSortedFunc(all, func(x, y step.Meta) int { return strings.Compare(string(x.Name), string(y.Name)) }))
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

func TestRegistry_RegisterRefusesAKindTheStepDoesNotImplement(t *testing.T) {
	cases := map[step.Kind]described{
		step.KindContext: fakePass{fakeStep{step.Meta{Name: "x", Kind: step.KindContext}}},
		step.KindPrompt:  fakeContext{fakeStep{step.Meta{Name: "x", Kind: step.KindPrompt}}},
		step.KindCheck:   fakeContext{fakeStep{step.Meta{Name: "x", Kind: step.KindCheck}}},
		step.KindOutput:  fakeContext{fakeStep{step.Meta{Name: "x", Kind: step.KindOutput}}},
		"unknown":        fakeStep{step.Meta{Name: "x", Kind: "unknown"}},
	}
	for kind, s := range cases {
		t.Run(string(kind), func(t *testing.T) {
			r := NewRegistry(Collaborators{})
			assert.Panics(t, func() { r.register(s) })
		})
	}
}

func TestRegistry_RegisterAcceptsAnApproverOfAnyShape(t *testing.T) {
	r := NewRegistry(Collaborators{})
	assert.NotPanics(t, func() { r.register(fakeStep{step.Meta{Name: "x", Kind: step.KindApprover}}) })
}

func TestRegistry_HoldsTheSharedSteps(t *testing.T) {
	r := NewRegistry(Collaborators{})
	for name, kind := range map[workflow.StepName]step.Kind{
		"dbt_log":            step.KindContext,
		"failure_precedents": step.KindContext,
		"upstream_changes":   step.KindContext,
		"content_changed":    step.KindCheck,
		"confidence_not_low": step.KindCheck,
	} {
		_, m, ok := r.Lookup(name)
		assert.True(t, ok, name)
		assert.Equal(t, kind, m.Kind, name)
	}
}

func TestRegistry_EvidenceTypeKnowsTheSharedStepKeys(t *testing.T) {
	r := NewRegistry(Collaborators{})
	for key, want := range map[remediation.EvidenceKey]any{
		remediation.KeyDBTLog:            remediation.Text(""),
		remediation.KeyFailurePrecedents: []prompt.Precedent{},
		remediation.KeyUpstreamChanges:   []prompt.UpstreamChange{},
	} {
		f, ok := r.EvidenceType(key)
		assert.True(t, ok, key)
		assert.IsType(t, want, f(), key)
	}
}
