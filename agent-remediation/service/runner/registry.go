package runner

import (
	"fmt"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// described is what every step implements, whatever its Kind.
type described interface {
	Meta() step.Meta
}

type entry struct {
	step any
	meta step.Meta
}

// Registry holds every step the runner can run and the Go type of every
// evidence key, so a flow file can be checked against them.
type Registry struct {
	steps    map[workflow.StepName]entry
	evidence map[remediation.EvidenceKey]func() any
}

// NewRegistry builds every step from c. Constructing a step never calls a
// collaborator, so a zero Collaborators builds a registry that can describe
// its steps.
func NewRegistry(c Collaborators) *Registry {
	_ = c // step constructors take the ports they use from c
	r := &Registry{
		steps: map[workflow.StepName]entry{},
		// The keys the runner itself puts in a cluster's evidence. Each step
		// that provides another key adds it here.
		evidence: map[remediation.EvidenceKey]func() any{
			remediation.KeyFailure: func() any { return remediation.Failure{} },
			remediation.KeyDraft:   func() any { return remediation.Draft{} },
		},
	}
	return r
}

// register adds s under the name its Meta gives. It panics on a name already
// registered or an empty one, which is a programming error.
func (r *Registry) register(s described) {
	m := s.Meta()
	if m.Name == "" {
		panic("runner: step registered with no name")
	}
	if _, dup := r.steps[m.Name]; dup {
		panic(fmt.Sprintf("runner: step %q registered twice", m.Name))
	}
	r.steps[m.Name] = entry{step: s, meta: m}
}

// Lookup returns the step registered under name, with its Meta.
func (r *Registry) Lookup(name workflow.StepName) (step any, meta step.Meta, ok bool) {
	e, ok := r.steps[name]
	if !ok {
		return nil, meta, false
	}
	return e.step, e.meta, true
}

// All returns the Meta of every step, sorted by name.
func (r *Registry) All() []step.Meta {
	metas := make([]step.Meta, 0, len(r.steps))
	for _, e := range r.steps {
		metas = append(metas, e.meta)
	}
	slices.SortFunc(metas, func(a, b step.Meta) int {
		return strings.Compare(string(a.Name), string(b.Name))
	})
	return metas
}

// EvidenceType returns a constructor of the zero value of the Go type k holds.
// It reports false for a key no step or runner provides.
func (r *Registry) EvidenceType(k remediation.EvidenceKey) (func() any, bool) {
	f, ok := r.evidence[k]
	return f, ok
}
