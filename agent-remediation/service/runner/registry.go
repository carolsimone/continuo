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
	"github.com/carolsimone/continuo/agent-remediation/service/steps/outputs"
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

// evidenceSpec is what the registry knows of one evidence key: a constructor of
// the zero value of its Go type, and the data class its content falls in.
type evidenceSpec struct {
	zero  func() any
	class workflow.DataClass
}

// builtinEvidence is every evidence key the runner or a registered step puts in
// a cluster's evidence, with its type and data class. A step that provides a
// new key adds it here.
func builtinEvidence() map[remediation.EvidenceKey]evidenceSpec {
	return map[remediation.EvidenceKey]evidenceSpec{
		// The keys the runner itself puts in a cluster's evidence.
		remediation.KeyFailure: {func() any { return remediation.Failure{} }, workflow.ClassLogs},
		remediation.KeyDraft:   {func() any { return remediation.Draft{} }, workflow.ClassCode},
		// The keys the registered steps provide.
		remediation.KeyDBTLog:               {func() any { return remediation.Text("") }, workflow.ClassLogs},
		remediation.KeyFailurePrecedents:    {func() any { return []prompt.Precedent{} }, workflow.ClassLogs},
		remediation.KeyUpstreamChanges:      {func() any { return []prompt.UpstreamChange{} }, workflow.ClassCode},
		remediation.KeySourceFiles:          {func() any { return remediation.SourceFiles{} }, workflow.ClassCode},
		remediation.KeyCandidateSQL:         {func() any { return remediation.Text("") }, workflow.ClassCode},
		remediation.KeyCodeBundleSource:     {func() any { return remediation.CandidateSource{} }, workflow.ClassCode},
		remediation.KeyOwnChangeDiff:        {func() any { return remediation.Text("") }, workflow.ClassCode},
		remediation.KeyChangedAncestorDiffs: {func() any { return []prompt.ReleaseUpstreamChange{} }, workflow.ClassCode},
		remediation.KeyContractFile:         {func() any { return remediation.ContractFile{} }, workflow.ClassCode},
		remediation.KeyContractEntry:        {func() any { return remediation.Text("") }, workflow.ClassCode},
		remediation.KeyCurrentContract:      {func() any { return remediation.Text("") }, workflow.ClassCode},
		remediation.KeyImagePackages:        {func() any { return remediation.ImagePackages{} }, workflow.ClassCode},
		remediation.KeyPackagedContract:     {func() any { return remediation.Bytes{} }, workflow.ClassCode},
		remediation.KeyUpstreamTarget:       {func() any { return remediation.UpstreamTarget{} }, workflow.ClassCode},
		// The earlier tries at the failure, including the diffs of the model's
		// own drafts. It records what happened in those tries, so it is classed
		// logs, whatever the diffs hold.
		remediation.KeyPreviousTries: {func() any { return []remediation.TryRecord{} }, workflow.ClassLogs},
	}
}

// Registry holds every step the runner can run and the Go type and data class
// of every evidence key, so a flow file can be checked against them.
type Registry struct {
	steps    map[workflow.StepName]entry
	evidence map[remediation.EvidenceKey]evidenceSpec
}

// NewRegistry builds every step from c. Constructing a step never calls a
// collaborator, so a zero Collaborators builds a registry that can describe
// its steps.
func NewRegistry(c Collaborators) *Registry {
	r := &Registry{
		steps:    map[workflow.StepName]entry{},
		evidence: builtinEvidence(),
	}
	r.register(shared.NewDBTLog(c.Evidence, c.Logger))
	r.register(shared.NewPrecedents(c.Precedents, c.Logger))
	r.register(shared.NewUpstreamChanges(c.Upstream, c.Logger))
	r.register(shared.NewContentChanged())
	r.register(shared.NewConfidenceNotLow())
	r.register(shared.NewPreviousTries(c.Tries, c.PriorAttempts, c.Evidence, c.Logger))
	r.register(source.NewFailingNodeSource(c.Source, c.Locator, c.ServiceRepoPaths, c.Logger))
	r.register(source.NewDBTCompile())
	r.register(source.NewDBTParse())
	r.register(source.NewSeed())
	r.register(source.NewDuplicateTable())
	r.register(source.NewTargetFileWasShown())
	r.register(validation.NewCandidateSQL(c.Evidence, c.Logger))
	r.register(validation.NewCodeBundleSource(c.CandidateSource, c.Source, c.Locator, c.ServiceRepoPaths, c.Logger))
	r.register(validation.NewOwnChangeDiff(c.Versions, c.Logger))
	r.register(validation.NewChangedAncestorDiffs(c.CandidateSource, c.Versions, c.Logger))
	r.register(validation.NewDBTValidation(c.Artifacts, c.ServiceRepoPaths))
	r.register(contract.NewContractFile(c.Archive, c.ContractLocator, c.Logger))
	r.register(contract.NewFailingSiblings(c.Archive, c.ContractLocator, c.Releases, c.Logger))
	r.register(contract.NewContractEntry(c.CandidateSource, c.Logger))
	r.register(contract.NewCurrentContract(c.CurrentContracts, c.Logger))
	r.register(contract.NewImagePackages(c.ImagePackages, c.Logger))
	r.register(contract.NewPythonContract())
	r.register(contract.NewPythonParse())
	r.register(contract.NewCSVContract())
	r.register(contract.NewContractPathsInsideFolder())
	r.register(contract.NewYAMLReads(c.ContractInspector))
	r.register(contract.NewNodeIdentityUnchanged(c.ContractInspector))
	r.register(contract.NewReadsKept(c.ContractInspector))
	r.register(contract.NewCSVReadKept(c.ContractInspector))
	r.register(contract.NewNoNewSecretRef(c.ContractInspector))
	r.register(contract.NewDeclaredOnce(c.ContractLocator))
	r.register(contract.NewContractPackages(c.Packager, c.ImagePackages, c.SQLDialect))
	r.register(upstream.NewTarget(c.CandidateSource, c.Locator, c.Versions, c.ServiceRepoPaths, c.Logger))
	r.register(upstream.NewPrompt(c.ServiceRepoPaths))
	r.register(cross.NewEditsMapToAService(c.ServiceRepoPaths, c.Logger))
	r.register(cross.NewNoPathEditedTwice(c.ServiceRepoPaths, c.Logger))
	r.register(cross.NewOneManifestKind(c.ServiceRepoPaths, c.Logger))
	r.register(gate.NewVerificationRun(c.Evidence, c.Artifacts, c.Releases, c.Pipeline, c.ServiceRepoPaths, c.Clock, c.VerificationTimeout, c.Logger))
	r.register(outputs.NewPullRequest(c.Clock))
	r.register(outputs.NewHandOver())
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
	spec, ok := r.evidence[k]
	return spec.zero, ok
}

// EvidenceClass returns the data class of the content k holds. It reports
// false for a key no step or runner provides.
func (r *Registry) EvidenceClass(k remediation.EvidenceKey) (workflow.DataClass, bool) {
	spec, ok := r.evidence[k]
	return spec.class, ok
}
