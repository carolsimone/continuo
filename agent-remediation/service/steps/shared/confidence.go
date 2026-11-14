package shared

import (
	"context"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// lowConfidenceMessage is the finding's text.
const lowConfidenceMessage = "the model could not determine a safe fix with confidence"

// ConfidenceNotLow is the check `confidence_not_low`: a draft the model itself
// rates low is not offered.
type ConfidenceNotLow struct{}

var _ step.Check = (*ConfidenceNotLow)(nil)

// NewConfidenceNotLow builds the confidence_not_low step.
func NewConfidenceNotLow() *ConfidenceNotLow { return &ConfidenceNotLow{} }

// Meta describes the step.
func (ConfidenceNotLow) Meta() step.Meta {
	return step.Meta{
		Name:        "confidence_not_low",
		Kind:        step.KindCheck,
		Description: "Fails a draft whose confidence the model gave as low, its signal that it could not find a safe fix.",
		Needs:       []remediation.EvidenceKey{remediation.KeyDraft},
		Stages:      []workflow.Stage{workflow.StagePreflight},
	}
}

// Check finds one low_confidence finding when the draft's confidence is "low",
// whatever its case or surrounding space.
func (ConfidenceNotLow) Check(_ context.Context, in step.CheckInput) (step.CheckOutput, error) {
	res := remediation.CheckResult{Check: string(ConfidenceNotLow{}.Meta().Name)}
	if strings.EqualFold(strings.TrimSpace(in.Draft.Confidence), "low") {
		res.Findings = []remediation.Finding{{Code: "low_confidence", Message: lowConfidenceMessage}}
	}
	return step.CheckOutput{Result: res}, nil
}
