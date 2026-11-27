package runner

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/pkg/streams"
)

// Where a flow or type file names each stage's steps, for messages.
const (
	fieldContext        = "context"
	fieldPrompt         = "agent.prompt"
	fieldPreflight      = "preflight"
	fieldAcrossClusters = "across_clusters"
	fieldGate           = "gate"
	fieldApprover       = "approver"
)

// Validate checks every flow and type of set against the steps reg holds and
// returns every refusal, joined, each naming its file and line. A flow or type
// that cannot run is an error: an unknown step, a step at a stage it may not
// stand in, a missing stage, a flow that lists no loaded type, a gate that is
// not exactly one step that runs the release check, a step that reads evidence
// no earlier step of its chain provides, a flow with no `model_may_see` or one
// that names an unknown class, a context step of a flow or of a type the flow
// lists that provides evidence of a data class the flow's `model_may_see`
// leaves out, a prompt step of a listed type that reads such evidence (or any
// evidence with no class), `rounds` below 1 (a type runs at
// least one pre-flight round), `tries` below 1, an output that is not
// a pull request or a hand-over or that publishes an event outside the stream
// contract, a flow with no approver or one that names a step that is not an
// approver, an unsupported version or trigger, and two listed types that can
// select the same failure. A type no flow lists is only a warning.
func Validate(set workflow.Set, reg *Registry) (warnings []string, err error) {
	v := &validator{set: set, reg: reg, seen: map[string]bool{}}
	for _, name := range sortedKeys(set.Flows) {
		v.flow(set.Flows[name])
	}
	for _, name := range sortedKeys(set.Types) {
		v.typ(set.Types[name])
	}

	listed := map[string]bool{}
	for _, f := range set.Flows {
		for _, name := range f.Types {
			listed[name] = true
		}
	}
	for _, name := range sortedKeys(set.Types) {
		if !listed[name] {
			t := set.Types[name]
			warnings = append(warnings, fmt.Sprintf("%s:%d: type %q is listed by no flow", t.Pos.File, t.Pos.Line, name))
		}
	}
	return warnings, errors.Join(v.errs...)
}

type validator struct {
	set  workflow.Set
	reg  *Registry
	errs []error
	seen map[string]bool
}

// refuse records one refusal, once per distinct text.
func (v *validator) refuse(p workflow.Pos, format string, args ...any) {
	msg := fmt.Sprintf("%s:%d: %s", p.File, p.Line, fmt.Sprintf(format, args...))
	if v.seen[msg] {
		return
	}
	v.seen[msg] = true
	v.errs = append(v.errs, errors.New(msg))
}

func (v *validator) flow(f workflow.Flow) {
	if f.Version != workflow.SupportedVersion {
		v.refuse(f.Pos, "flow %q has version %d, and only version %d is supported", f.Name, f.Version, workflow.SupportedVersion)
	}
	if want := baseName(streams.HealTriggeredV1); f.Trigger != want {
		v.refuse(f.Pos, "flow %q has trigger %q, and the only trigger is %q", f.Name, f.Trigger, want)
	}
	if f.Tries < 1 {
		v.refuse(f.Pos, "flow %q has tries %d, and tries must be at least 1", f.Name, f.Tries)
	}

	if len(f.Types) == 0 {
		v.refuse(f.TypesPos, "flow %q has no types", f.Name)
	}
	var listed []workflow.Type
	for _, name := range f.Types {
		t, ok := v.set.Types[name]
		if !ok {
			v.refuse(f.TypesPos, "flow %q lists type %q, which no file defines", f.Name, name)
			continue
		}
		listed = append(listed, t)
	}
	v.overlaps(f, listed)

	v.modelMaySee(f)
	v.stage(f.Context, workflow.StageContext, fieldContext)
	v.stage(f.AcrossClusters, workflow.StageCrossCluster, fieldAcrossClusters)
	gate := v.stage(f.Gate, workflow.StageGate, fieldGate)
	switch {
	case len(f.Gate) == 0:
		v.refuse(f.Pos, "flow %q has no gate", f.Name)
	case len(f.Gate) > 1:
		v.refuse(f.Gate[1].Pos, "flow %q has %d gate steps, and a gate is one step that runs the release check", f.Name, len(f.Gate))
	case !slices.ContainsFunc(gate, func(m step.Meta) bool { return m.MayStandAsGateRun }):
		v.refuse(f.Pos, "flow %q has a gate with no step that runs the release check", f.Name)
	}

	v.output(f.OnPass, "on_pass", workflow.OutputPullRequest)
	v.output(f.OnGiveUp, "on_give_up", workflow.OutputHandOver)
	if f.Approver.Name == "" {
		v.refuse(f.Pos, "flow %q has no approver, and a flow names the step that works out who approves its fixes (approver: node_owner)", f.Name)
	} else {
		v.stage([]workflow.StepRef{f.Approver}, workflow.StageApprover, fieldApprover)
	}

	v.classes(f, fmt.Sprintf("flow %q", f.Name), f.Context)
	for _, t := range listed {
		owner := fmt.Sprintf("type %q", t.Name)
		v.classes(f, owner, t.Context)
		v.promptClasses(f, owner, t.Agent.Prompt)
		v.chain(f, t)
	}
}

// modelMaySee refuses a flow that lists no data class for the model, or one
// that names a class that does not exist.
func (v *validator) modelMaySee(f workflow.Flow) {
	all := joinClasses(workflow.DataClasses())
	if len(f.ModelMaySee) == 0 {
		v.refuse(f.ModelMaySeePos, "flow %q has no model_may_see, and a flow lists the classes of content the model may see (%s)", f.Name, all)
	}
	for _, c := range f.ModelMaySee {
		if !c.IsValid() {
			v.refuse(f.ModelMaySeePos, "flow %q has model_may_see class %q, which is none of %s", f.Name, c, all)
		}
	}
}

// holdsClasses reports whether the flow's model_may_see is usable for checking
// the data classes of its steps: it lists at least one class and every class
// it lists exists. A flow whose list is not usable is refused once, by
// modelMaySee, and its steps are not checked against the list.
func holdsClasses(f workflow.Flow) bool {
	return len(f.ModelMaySee) > 0 && !slices.ContainsFunc(f.ModelMaySee, func(c workflow.DataClass) bool { return !c.IsValid() })
}

// classes refuses every context step of owner (a flow or a type) that provides
// evidence the flow does not let the model see: a key of a data class the
// flow's model_may_see leaves out, or a key with no class at all.
func (v *validator) classes(f workflow.Flow, owner string, refs []workflow.StepRef) {
	if !holdsClasses(f) {
		return
	}
	for _, r := range refs {
		_, m, ok := v.reg.Lookup(r.Name)
		if !ok || m.Kind != step.KindContext {
			continue
		}
		for _, key := range m.Provides {
			v.holdToClasses(f, owner, r, "provides", key)
		}
	}
}

// promptClasses refuses the prompt step of a type, at its line, for every
// evidence key it reads that the flow does not let the model see. The prompt
// builds the model's request from its keys, so the failure itself is among them.
func (v *validator) promptClasses(f workflow.Flow, owner string, prompt workflow.StepRef) {
	if !holdsClasses(f) || prompt.Name == "" {
		return
	}
	_, m, ok := v.reg.Lookup(prompt.Name)
	if !ok || m.Kind != step.KindPrompt {
		return
	}
	for _, key := range m.Needs {
		v.holdToClasses(f, owner, prompt, "reads", key)
	}
}

// holdToClasses refuses, at the line of r, a step of owner whose evidence key
// has no data class or one that the flow's model_may_see leaves out. verb says
// whether the step provides the key or reads it.
func (v *validator) holdToClasses(f workflow.Flow, owner string, r workflow.StepRef, verb string, key remediation.EvidenceKey) {
	class, known := v.reg.EvidenceClass(key)
	switch {
	case !known:
		v.refuse(r.Pos, "step %q %s %q, which has no data class", r.Name, verb, key)
	case !slices.Contains(f.ModelMaySee, class):
		v.refuse(r.Pos, "%s uses step %q, which %s %q (class %s), and flow %q does not let the model see %s (model_may_see: %s)",
			owner, r.Name, verb, key, class, f.Name, class, joinClasses(f.ModelMaySee))
	}
}

// overlaps refuses two listed types whose matches can select the same failure,
// naming the later type's match.
func (v *validator) overlaps(f workflow.Flow, listed []workflow.Type) {
	for i, a := range listed {
		for _, b := range listed[i+1:] {
			if a.Name != b.Name && a.Match.Overlaps(b.Match) {
				v.refuse(b.Match.Pos, "flow %q lists types %q and %q, whose matches can select the same failure", f.Name, a.Name, b.Name)
			}
		}
	}
}

func (v *validator) output(o workflow.Output, field string, want workflow.OutputAction) {
	switch {
	case o.Action == "":
		v.refuse(o.Pos, "output.%s is missing", field)
	case o.Action != want:
		v.refuse(o.Pos, "output.%s has action %q, and the only action is %q", field, o.Action, want)
	}
	if o.Emits != "" && !slices.ContainsFunc(streams.All, func(s string) bool { return baseName(s) == o.Emits }) {
		v.refuse(o.Pos, "output.%s emits %q, which is no stream in pkg/streams/contract.yaml", field, o.Emits)
	}
}

func (v *validator) typ(t workflow.Type) {
	if t.Version != workflow.SupportedVersion {
		v.refuse(t.Pos, "type %q has version %d, and only version %d is supported", t.Name, t.Version, workflow.SupportedVersion)
	}
	switch t.Match.Cluster {
	case workflow.ClusterSingle, workflow.ClusterSharedUpstream, workflow.ClusterCrossProject:
	default:
		v.refuse(t.Match.Pos, "type %q has match.cluster %q (single, shared_upstream or cross_project)", t.Name, t.Match.Cluster)
	}
	if t.Match.NodeTypes != nil && len(t.Match.NodeTypes) == 0 {
		v.refuse(t.Match.Pos, "type %q has an empty match.node_types, so it matches no node type", t.Name)
	}
	if t.Agent.Rounds < 1 {
		v.refuse(t.Agent.Pos, "type %q has rounds %d, and a type runs at least one pre-flight round, so rounds must be 1 or more", t.Name, t.Agent.Rounds)
	}
	for _, tool := range t.Agent.Tools {
		v.refuse(tool.Pos, "type %q lists agent.tools, which is reserved and must be empty", t.Name)
	}

	v.stage(t.Context, workflow.StageContext, fieldContext)
	if t.Agent.Prompt.Name == "" {
		v.refuse(t.Agent.Pos, "type %q has no agent.prompt", t.Name)
	} else {
		v.stage([]workflow.StepRef{t.Agent.Prompt}, workflow.StagePrompt, fieldPrompt)
	}
	v.stage(t.Preflight, workflow.StagePreflight, fieldPreflight)
}

// stage checks that every ref names a registered step that may stand at stage,
// and returns the Meta of those that do.
func (v *validator) stage(refs []workflow.StepRef, stage workflow.Stage, field string) []step.Meta {
	var metas []step.Meta
	for _, r := range refs {
		_, m, ok := v.reg.Lookup(r.Name)
		if !ok {
			v.refuse(r.Pos, "unknown step %q in %s", r.Name, field)
			continue
		}
		if wantKind := kindOf(stage); m.Kind != wantKind {
			v.refuse(r.Pos, "step %q is a %s step and cannot be used in %s", r.Name, m.Kind, field)
			continue
		}
		if m.Kind == step.KindCheck && !slices.Contains(m.Stages, stage) {
			v.refuse(r.Pos, "step %q cannot be used in %s (it may stand in: %s)", r.Name, field, joinStages(m.Stages))
			continue
		}
		metas = append(metas, m)
	}
	return metas
}

// kindOf is the Kind of step that may stand at stage.
func kindOf(stage workflow.Stage) step.Kind {
	switch stage {
	case workflow.StageContext:
		return step.KindContext
	case workflow.StagePrompt:
		return step.KindPrompt
	case workflow.StageApprover:
		return step.KindApprover
	default:
		return step.KindCheck
	}
}

func joinClasses(classes []workflow.DataClass) string {
	names := make([]string, len(classes))
	for i, c := range classes {
		names[i] = string(c)
	}
	return strings.Join(names, ", ")
}

func joinStages(stages []workflow.Stage) string {
	names := make([]string, len(stages))
	for i, s := range stages {
		names[i] = string(s)
	}
	return strings.Join(names, ", ")
}

// chain walks the steps one flow runs for one of its types in the order the
// runner runs them, and refuses a step that reads an evidence key no earlier
// step provides. The runner puts `failure` in first and `draft` in once the
// prompt has answered.
func (v *validator) chain(f workflow.Flow, t workflow.Type) {
	provided := map[remediation.EvidenceKey]bool{remediation.KeyFailure: true}
	walk := func(refs []workflow.StepRef) {
		for _, r := range refs {
			_, m, ok := v.reg.Lookup(r.Name)
			if !ok {
				continue
			}
			for _, need := range m.Needs {
				if !provided[need] {
					v.refuse(r.Pos, "step %q needs %q, which no earlier step provides (flow %q, type %q)", r.Name, need, f.Name, t.Name)
				}
			}
			for _, p := range m.Provides {
				provided[p] = true
			}
		}
	}
	walk(f.Context)
	walk(t.Context)
	if t.Agent.Prompt.Name != "" {
		walk([]workflow.StepRef{t.Agent.Prompt})
	}
	provided[remediation.KeyDraft] = true
	walk(t.Preflight)
	walk(f.AcrossClusters)
	walk(f.Gate)
}

// baseName is a stream name without its version ("heal.triggered" for
// "heal.triggered:v1").
func baseName(stream string) string {
	name, _, _ := strings.Cut(stream, ":")
	return name
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
