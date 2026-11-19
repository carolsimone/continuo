package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// scriptedProvider answers each call with the next scripted answer, or fails
// it with the next scripted error.
type scriptedProvider struct {
	answers []ports.ProposeResult
	errs    []error
}

func (p *scriptedProvider) Propose(context.Context, prompt.ProposeRequest) (ports.ProposeResult, error) {
	var err error
	if len(p.errs) > 0 {
		err, p.errs = p.errs[0], p.errs[1:]
	}
	if err != nil {
		return ports.ProposeResult{}, err
	}
	res := p.answers[0]
	p.answers = p.answers[1:]
	return res, nil
}

func newTestModel(p *scriptedProvider) *model {
	return newModel(p, uuid.New(), 1, 1, 1, promptlog.Failure{}, nil)
}

func TestModel_RecordsTheLastRequestAndAnswer(t *testing.T) {
	first, second := prompt.ProposeRequest{User: "diagnose"}, prompt.ProposeRequest{User: "apply"}
	m := newTestModel(&scriptedProvider{answers: []ports.ProposeResult{{Rationale: "one"}, {Rationale: "two", Cached: true}}})

	_, err := m.Propose(context.Background(), first)
	require.NoError(t, err)
	assert.Equal(t, first, m.lastRequest)
	assert.Equal(t, ports.ProposeResult{Rationale: "one"}, m.lastAnswer)

	_, err = m.Propose(context.Background(), second)
	require.NoError(t, err)
	assert.Equal(t, second, m.lastRequest)
	assert.Equal(t, ports.ProposeResult{Rationale: "two", Cached: true}, m.lastAnswer, "a replayed answer is recorded like any other")
}

// The tokens of a call count when it returns, and a call that fails or that the
// response cache answers counts none.
func TestModel_CountsTheTokensOfEachCallTheModelAnswered(t *testing.T) {
	type tokens struct{ in, out int }
	var counted []tokens
	p := &scriptedProvider{
		answers: []ports.ProposeResult{
			{Usage: ports.TokenUsage{Input: 100, Output: 20}},
			{Usage: ports.TokenUsage{Input: 100, Output: 20}, Cached: true},
			{Usage: ports.TokenUsage{Input: 7, Output: 3}},
		},
		errs: []error{nil, nil, errors.New("boom"), nil},
	}
	m := newModel(p, uuid.New(), 1, 1, 1, promptlog.Failure{}, func(in, out int) { counted = append(counted, tokens{in, out}) })

	for range 4 {
		_, _ = m.Propose(context.Background(), prompt.ProposeRequest{User: "u"})
	}

	assert.Equal(t, []tokens{{100, 20}, {7, 3}}, counted, "the replayed answer and the failed call count none")
}

func TestModel_AFailedCallLeavesTheLastCallAsItWas(t *testing.T) {
	first := prompt.ProposeRequest{User: "diagnose"}
	boom := errors.New("boom")
	m := newTestModel(&scriptedProvider{answers: []ports.ProposeResult{{Rationale: "one"}}, errs: []error{nil, boom}})
	_, err := m.Propose(context.Background(), first)
	require.NoError(t, err)

	_, err = m.Propose(context.Background(), prompt.ProposeRequest{User: "apply"})

	require.ErrorIs(t, err, boom)
	assert.Equal(t, first, m.lastRequest)
	assert.Equal(t, ports.ProposeResult{Rationale: "one"}, m.lastAnswer)
	assert.Equal(t, 1, m.calls)
}

func TestPromptRecord_StoresTheLastCallOfAStepThatMadeOne(t *testing.T) {
	req := prompt.ProposeRequest{System: "s", User: "u", ToolName: "propose_fix"}
	m := newTestModel(&scriptedProvider{answers: []ports.ProposeResult{{ProposedContent: "select 2"}}})
	_, err := m.Propose(context.Background(), req)
	require.NoError(t, err)

	rec := promptRecord(step.PromptOutput{}, m)

	require.NotNil(t, rec.LastRequest)
	require.NotNil(t, rec.LastAnswer)
	assert.Equal(t, req, rec.LastRequest.Request())
	assert.Equal(t, ports.ProposeResult{ProposedContent: "select 2"}, rec.LastAnswer.Answer())
}

func TestPromptRecord_StoresNoCallForADeclineOrAStepThatMadeNone(t *testing.T) {
	m := newTestModel(&scriptedProvider{answers: []ports.ProposeResult{{ProposedContent: "select 2"}}})
	_, err := m.Propose(context.Background(), prompt.ProposeRequest{User: "u"})
	require.NoError(t, err)

	declined := promptRecord(step.PromptOutput{Declined: true, Decline: "no fix"}, m)
	assert.Equal(t, serialization.PromptRecord{Declined: true, Decline: "no fix"}, declined)

	none := promptRecord(step.PromptOutput{}, newTestModel(&scriptedProvider{}))
	assert.Equal(t, serialization.PromptRecord{}, none)
}
