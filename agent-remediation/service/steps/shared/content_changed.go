package shared

import (
	"context"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// contentUnchangedMessage is the finding's text.
const contentUnchangedMessage = "the model returned the file unchanged, which is not a fix"

// ContentChanged is the check `content_changed`: a draft must change something.
type ContentChanged struct{}

var _ step.Check = (*ContentChanged)(nil)

// NewContentChanged builds the content_changed step.
func NewContentChanged() *ContentChanged { return &ContentChanged{} }

// Meta describes the step.
func (ContentChanged) Meta() step.Meta {
	return step.Meta{
		Name:        "content_changed",
		Kind:        step.KindCheck,
		Description: "Fails a draft that has no edit, or whose edit is empty or equal to the file it was read from.",
		Needs:       []remediation.EvidenceKey{remediation.KeyDraft},
		Stages:      []workflow.Stage{workflow.StagePreflight},
	}
}

// Check finds one content_unchanged finding when the draft has no edit, or when
// an edit's content is empty or equals its original. The finding names the
// first such edit's file, and says nothing of the others: one unchanged file is
// one refusal, as it is for a single-file fix.
func (ContentChanged) Check(_ context.Context, in step.CheckInput) (step.CheckOutput, error) {
	res := remediation.CheckResult{Check: string(ContentChanged{}.Meta().Name)}
	finding := remediation.Finding{Code: "content_unchanged", Message: contentUnchangedMessage}
	if len(in.Draft.Edits) == 0 {
		res.Findings = []remediation.Finding{finding}
		return step.CheckOutput{Result: res}, nil
	}
	for _, e := range in.Draft.Edits {
		if e.Content == "" || e.Content == e.Original {
			finding.File = e.Path
			res.Findings = []remediation.Finding{finding}
			break
		}
	}
	return step.CheckOutput{Result: res}, nil
}
