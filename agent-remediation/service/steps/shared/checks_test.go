package shared_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

func TestContentChanged_MetaIsAPreflightCheckOnTheDraft(t *testing.T) {
	m := shared.NewContentChanged().Meta()
	assert.Equal(t, "content_changed", string(m.Name))
	assert.Equal(t, step.KindCheck, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyDraft}, m.Needs)
	assert.Empty(t, m.Provides)
	assert.Equal(t, []workflow.Stage{workflow.StagePreflight}, m.Stages)
}

func TestContentChanged(t *testing.T) {
	const msg = "the model returned the file unchanged, which is not a fix"
	cases := []struct {
		name  string
		draft remediation.Draft
		want  []remediation.Finding
	}{
		{"a changed edit passes", remediation.Draft{Edits: []remediation.Edit{{Path: "a.sql", Original: "a", Content: "b"}}}, nil},
		{"no edit fails", remediation.Draft{}, []remediation.Finding{{Code: "content_unchanged", Message: msg}}},
		{"empty content fails", remediation.Draft{Edits: []remediation.Edit{{Path: "a.sql", Original: "a"}}},
			[]remediation.Finding{{Code: "content_unchanged", Message: msg, File: "a.sql"}}},
		{"content equal to the original fails", remediation.Draft{Edits: []remediation.Edit{{Path: "a.sql", Original: "a", Content: "a"}}},
			[]remediation.Finding{{Code: "content_unchanged", Message: msg, File: "a.sql"}}},
		{"one unchanged edit among changed ones fails once", remediation.Draft{Edits: []remediation.Edit{
			{Path: "a.sql", Original: "a", Content: "b"},
			{Path: "b.sql", Original: "x", Content: "x"},
			{Path: "c.sql", Original: "y", Content: "y"},
		}}, []remediation.Finding{{Code: "content_unchanged", Message: msg, File: "b.sql"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := shared.NewContentChanged().Check(context.Background(), step.CheckInput{Draft: c.draft})
			require.NoError(t, err)
			assert.Equal(t, "content_changed", out.Result.Check)
			assert.Equal(t, c.want, out.Result.Findings)
			assert.Equal(t, len(c.want) == 0, out.Result.Passed())
		})
	}
}

func TestConfidenceNotLow_MetaIsAPreflightCheckOnTheDraft(t *testing.T) {
	m := shared.NewConfidenceNotLow().Meta()
	assert.Equal(t, "confidence_not_low", string(m.Name))
	assert.Equal(t, step.KindCheck, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyDraft}, m.Needs)
	assert.Equal(t, []workflow.Stage{workflow.StagePreflight}, m.Stages)
}

func TestConfidenceNotLow(t *testing.T) {
	low := []remediation.Finding{{Code: "low_confidence", Message: "the model could not determine a safe fix with confidence"}}
	cases := []struct {
		confidence string
		want       []remediation.Finding
	}{
		{"low", low}, {"LOW", low}, {" Low ", low},
		{"medium", nil}, {"high", nil}, {"", nil},
	}
	for _, c := range cases {
		t.Run("confidence "+c.confidence, func(t *testing.T) {
			out, err := shared.NewConfidenceNotLow().Check(context.Background(),
				step.CheckInput{Draft: remediation.Draft{Confidence: c.confidence}})
			require.NoError(t, err)
			assert.Equal(t, "confidence_not_low", out.Result.Check)
			assert.Equal(t, c.want, out.Result.Findings)
		})
	}
}
