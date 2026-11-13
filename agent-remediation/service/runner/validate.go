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
)

// Validate checks every flow and type of set against the steps reg holds and
// returns every refusal, joined, each naming its file and line. A flow or type
// that cannot run is an error: an unknown step, a step at a stage it may not
// stand in, a missing stage, a flow that lists no loaded type, a gate with no
// step that runs the verification, a step that reads evidence no earlier step
// of its chain provides, `rounds` or `tries` below 1, an output that is not a
// pull request or a hand-over or that publishes an event outside the stream
// contract, an unsupported version or trigger, and two listed types that can
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
	if want := baseName(streams.RemediationRequestedV2); f.Trigger != want {
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

	v.stage(f.Context, workflow.StageContext, fieldContext)
	v.stage(f.AcrossClusters, workflow.StageCrossCluster, fieldAcrossClusters)
	gate := v.stage(f.Gate, workflow.StageGate, fieldGate)
	switch {
	case len(f.Gate) == 0:
		v.refuse(f.Pos, "flow %q has no gate", f.Name)
	case !slices.ContainsFunc(gate, func(m step.Meta) bool { return m.MayStandAsGateRun }):
		v.refuse(f.Pos, "flow %q has a gate with no step that runs the verification", f.Name)
	}

	v.output(f.OnPass, "on_pass", workflow.OutputPullRequest)
	v.output(f.OnGiveUp, "on_give_up", workflow.OutputHandOver)

	for _, t := range listed {
		v.chain(f, t)
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
	case workflow.ClusterSingle, workflow.ClusterSharedUpstream, workflow.ClusterCrossService:
	default:
		v.refuse(t.Match.Pos, "type %q has match.cluster %q (single, shared_upstream or cross_service)", t.Name, t.Match.Cluster)
	}
	if t.Match.NodeTypes != nil && len(t.Match.NodeTypes) == 0 {
		v.refuse(t.Match.Pos, "type %q has an empty match.node_types, so it matches no node type", t.Name)
	}
	if t.Agent.Rounds < 1 {
		v.refuse(t.Agent.Pos, "type %q has rounds %d, and rounds must be at least 1", t.Name, t.Agent.Rounds)
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
	default:
		return step.KindCheck
	}
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

// baseName is a stream name without its version ("remediation.requested" for
// "remediation.requested:v2").
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
