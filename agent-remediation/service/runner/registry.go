package runner

import (
	"fmt"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/contract"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/cross"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/gate"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/source"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/upstream"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/validation"
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
	r := &Registry{
		steps: map[workflow.StepName]entry{},
		// The keys the runner itself puts in a cluster's evidence. Each step
		// that provides another key adds it here.
		evidence: map[remediation.EvidenceKey]func() any{
			remediation.KeyFailure: func() any { return remediation.Failure{} },
			remediation.KeyDraft:   func() any { return remediation.Draft{} },
			// The keys the registered steps provide.
			remediation.KeyDBTLog:               func() any { return remediation.Text("") },
			remediation.KeyFailurePrecedents:    func() any { return []prompt.Precedent{} },
			remediation.KeyUpstreamChanges:      func() any { return []prompt.UpstreamChange{} },
			remediation.KeySourceFiles:          func() any { return remediation.SourceFiles{} },
			remediation.KeyCandidateSQL:         func() any { return remediation.Text("") },
			remediation.KeyCodeBundleSource:     func() any { return remediation.CandidateSource{} },
			remediation.KeyOwnChangeDiff:        func() any { return remediation.Text("") },
			remediation.KeyChangedAncestorDiffs: func() any { return []prompt.ReleaseUpstreamChange{} },
			remediation.KeyContractFile:         func() any { return remediation.ContractFile{} },
			remediation.KeyContractEntry:        func() any { return remediation.Text("") },
			remediation.KeyPriorAttempts:        func() any { return []prompt.PriorAttempt{} },
			remediation.KeyPackagedContract:     func() any { return remediation.Bytes{} },
			remediation.KeyUpstreamTarget:       func() any { return remediation.UpstreamTarget{} },
		},
	}
	r.register(shared.NewDBTLog(c.Evidence, c.Sanitizer, c.Logger))
	r.register(shared.NewPrecedents(c.Precedents, c.Sanitizer, c.Logger))
	r.register(shared.NewUpstreamChanges(c.Upstream, c.Sanitizer, c.Logger))
	r.register(shared.NewContentChanged())
	r.register(shared.NewConfidenceNotLow())
	r.register(source.NewFailingNodeSource(c.Source, c.Locator, c.ServiceRepoPaths, c.Logger))
	r.register(source.NewDBTCompile(c.Sanitizer))
	r.register(source.NewDBTParse(c.Sanitizer))
	r.register(source.NewSeed(c.Sanitizer))
	r.register(source.NewDuplicateTable(c.Sanitizer))
	r.register(source.NewTargetFileWasShown())
	r.register(validation.NewCandidateSQL(c.Evidence, c.Logger))
	r.register(validation.NewCodeBundleSource(c.CandidateSource, c.Source, c.Locator, c.ServiceRepoPaths, c.Logger))
	r.register(validation.NewOwnChangeDiff(c.Versions, c.Sanitizer, c.Logger))
	r.register(validation.NewChangedAncestorDiffs(c.CandidateSource, c.Versions, c.Sanitizer, c.Logger))
	r.register(validation.NewDBTValidation(c.Artifacts, c.Sanitizer, c.ServiceRepoPaths, c.Logger))
	r.register(contract.NewContractFile(c.Archive, c.ContractLocator, c.Logger))
	r.register(contract.NewFailingSiblings(c.Archive, c.ContractLocator, c.Releases, c.Logger))
	r.register(contract.NewContractEntry(c.CandidateSource, c.Sanitizer, c.Logger))
	r.register(contract.NewPriorAttempts(c.PriorAttempts, c.Evidence, c.Sanitizer, c.Logger))
	r.register(contract.NewPythonContract(c.Sanitizer))
	r.register(contract.NewPythonParse(c.Sanitizer))
	r.register(contract.NewCSVContract(c.Sanitizer))
	r.register(contract.NewContractPathsInsideFolder())
	r.register(contract.NewYAMLReads(c.ContractInspector))
	r.register(contract.NewNodeIdentityUnchanged(c.ContractInspector))
	r.register(contract.NewReadsKept(c.ContractInspector))
	r.register(contract.NewCSVReadKept(c.ContractInspector))
	r.register(contract.NewNoNewSecretRef(c.ContractInspector))
	r.register(contract.NewDeclaredOnce(c.ContractLocator))
	r.register(contract.NewContractPackages(c.Packager, c.SQLDialect))
	r.register(upstream.NewTarget(c.CandidateSource, c.Locator, c.Versions, c.Sanitizer, c.ServiceRepoPaths, c.Logger))
	r.register(upstream.NewPrompt(c.Sanitizer, c.ServiceRepoPaths))
	r.register(cross.NewEditsMapToAService(c.ServiceRepoPaths, c.Logger))
	r.register(cross.NewNoPathEditedTwice(c.ServiceRepoPaths, c.Logger))
	r.register(cross.NewOneManifestKind(c.ServiceRepoPaths, c.Logger))
	r.register(gate.NewVerificationRun(c.Evidence, c.Artifacts, c.Releases, c.Pipeline, c.ServiceRepoPaths, c.Clock, c.VerificationTimeout, c.Logger))
	return r
}

// register adds s under the name its Meta gives. It panics on a name already
// registered, an empty one, or a Kind the step's methods do not match, which
// are programming errors.
func (r *Registry) register(s described) {
	m := s.Meta()
	if m.Name == "" {
		panic("runner: step registered with no name")
	}
	if !implementsKind(s, m.Kind) {
		panic(fmt.Sprintf("runner: step %q declares kind %q but does not implement its interface", m.Name, m.Kind))
	}
	if _, dup := r.steps[m.Name]; dup {
		panic(fmt.Sprintf("runner: step %q registered twice", m.Name))
	}
	r.steps[m.Name] = entry{step: s, meta: m}
}

// implementsKind reports whether s has the methods the runner calls on a step
// of kind k. An approver step has no interface the runner checks.
func implementsKind(s described, k step.Kind) bool {
	switch k {
	case step.KindContext:
		_, ok := s.(step.ContextSource)
		return ok
	case step.KindPrompt:
		_, ok := s.(step.Prompt)
		return ok
	case step.KindCheck:
		_, isCheck := s.(step.Check)
		_, isCross := s.(step.CrossCheck)
		_, isLong := s.(step.LongCheck)
		return isCheck || isCross || isLong
	case step.KindOutput:
		_, isPass := s.(step.PassOutput)
		_, isGiveUp := s.(step.GiveUpOutput)
		return isPass || isGiveUp
	case step.KindApprover:
		return true
	default:
		return false
	}
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
