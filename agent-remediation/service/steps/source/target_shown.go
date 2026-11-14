package source

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// TargetFileWasShown is the check `target_file_was_shown`: the file the model
// named as the one to change must be one of the files it was shown.
type TargetFileWasShown struct{}

var _ step.Check = (*TargetFileWasShown)(nil)

// NewTargetFileWasShown builds the target_file_was_shown step.
func NewTargetFileWasShown() *TargetFileWasShown { return &TargetFileWasShown{} }

// Meta describes the step.
func (TargetFileWasShown) Meta() step.Meta {
	return step.Meta{
		Name:        "target_file_was_shown",
		Kind:        step.KindCheck,
		Description: "Fails a draft whose named target file is not exactly one of the source files the model was shown.",
		Needs:       []remediation.EvidenceKey{remediation.KeyDraft, remediation.KeySourceFiles},
		Stages:      []workflow.Stage{workflow.StagePreflight},
	}
}

// Check resolves the draft's named target against the shown files and finds one
// target_not_shown finding when it does not resolve to exactly one of them.
func (TargetFileWasShown) Check(_ context.Context, in step.CheckInput) (step.CheckOutput, error) {
	files, ok := remediation.Get[remediation.SourceFiles](in.Evidence, remediation.KeySourceFiles)
	if !ok {
		return step.CheckOutput{}, errors.New("target_file_was_shown: the evidence holds no source files")
	}
	res := remediation.CheckResult{Check: string(TargetFileWasShown{}.Meta().Name)}
	if _, shown := resolveTarget(in.Draft.NamedTarget, files); !shown {
		res.Findings = []remediation.Finding{{
			Code:    "target_not_shown",
			Message: fmt.Sprintf("the model named %q as the file to change, which is not one of the files it was shown", in.Draft.NamedTarget),
		}}
	}
	return step.CheckOutput{Result: res}, nil
}

// resolveTarget maps the model's target_file to exactly one of the files that
// were shown to it, returning ok=false when no safe choice exists. It never
// invents a target the model was not shown (guarding against a PR overwriting an
// arbitrary path), but it tolerates a differently-rooted spelling — the model
// may echo "models/x.sql" for a file shown as "services/svc/models/x.sql" — by
// accepting a path-suffix match when it is unambiguous:
//   - empty target: the offending file, but only when it is the sole shown file
//     (otherwise the intended file is ambiguous);
//   - exact key match: that file;
//   - exactly one shown file ending in "/"+target: that file;
//   - anything else (unknown path, or ambiguous suffix): no target.
func resolveTarget(target string, g remediation.SourceFiles) (string, bool) {
	if target == "" {
		if len(g.Order) == 1 {
			return g.Primary, true
		}
		return "", false
	}
	if _, ok := g.Files[target]; ok {
		return target, true
	}
	var matches []string
	for _, p := range g.Order {
		if strings.HasSuffix(p, "/"+target) {
			matches = append(matches, p)
		}
	}
	if len(matches) == 1 {
		return matches[0], true
	}
	return "", false
}
