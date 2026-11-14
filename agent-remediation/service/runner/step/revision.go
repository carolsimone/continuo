package step

import (
	"errors"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
)

// errNoPreviousCall is the error of a revision round whose input does not carry
// the last model call of the round before it.
var errNoPreviousCall = errors.New("a later round revises the last model call of the round before it, and the input carries no previous request and answer")

// RevisionRequest is the request a prompt step makes in a round after the
// first: the previous round's last request, with the answer the model gave to it
// and every finding the checks made, as prompt.Revise builds it. It reports
// false, with no error, in the first round, where the step builds its request
// from its evidence. A later round whose input lacks the previous request or
// answer is an error, so a step never runs the first round again by mistake.
func RevisionRequest(in PromptInput) (prompt.ProposeRequest, bool, error) {
	if in.Round == 0 {
		return prompt.ProposeRequest{}, false, nil
	}
	if in.PreviousRequest == nil || in.PreviousAnswer == nil {
		return prompt.ProposeRequest{}, false, errNoPreviousCall
	}
	return prompt.Revise(*in.PreviousRequest, AnswerJSON(*in.PreviousAnswer), FindingLines(in.Findings)), true, nil
}
