package step_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestAnswerJSON_UsesTheWireNames(t *testing.T) {
	res := ports.ProposeResult{
		ProposedSQL:     "select 1",
		ProposedContent: "name,id\n",
		TargetFile:      "seeds/a.csv",
		Files: []ports.ProposedFile{
			{Path: "models/a.py", Content: "x = 1\n"},
			{Path: "models/b.py", Content: "y = 2\n"},
		},
		Rationale:              "qualify the table",
		Confidence:             "high",
		SuspectedRootCauseNode: "analytics.table_a",
		Model:                  "claude-x",
		Usage:                  ports.TokenUsage{Input: 10, Output: 5},
		Cached:                 true,
	}

	got := step.AnswerJSON(res)

	assert.JSONEq(t, `{
		"proposed_sql": "select 1",
		"proposed_content": "name,id\n",
		"target_file": "seeds/a.csv",
		"updated_files": [
			{"path": "models/a.py", "content": "x = 1\n"},
			{"path": "models/b.py", "content": "y = 2\n"}
		],
		"rationale": "qualify the table",
		"confidence": "high",
		"suspected_root_cause_node": "analytics.table_a"
	}`, got)
	assert.NotContains(t, got, "claude-x", "the model and usage are not part of the tool input")
	assert.NotContains(t, got, "cached")
}

func TestAnswerJSON_LeavesEmptyFieldsOut(t *testing.T) {
	got := step.AnswerJSON(ports.ProposeResult{
		ProposedSQL: "select 1",
		Rationale:   "r",
		Confidence:  "low",
	})

	assert.JSONEq(t, `{"proposed_sql":"select 1","rationale":"r","confidence":"low"}`, got)
	assert.Equal(t, "{}", step.AnswerJSON(ports.ProposeResult{}))
}

func TestAnswerJSON_IsCanonical(t *testing.T) {
	res := ports.ProposeResult{
		ProposedSQL: "select a < b and c > d & e from t",
		Files:       []ports.ProposedFile{{Path: "p.py", Content: "if a < b: pass"}},
		Rationale:   "keep <, > and & as they are",
		Confidence:  "medium",
	}

	got := step.AnswerJSON(res)

	assert.Equal(t,
		`{"proposed_sql":"select a < b and c > d & e from t","updated_files":[{"path":"p.py","content":"if a < b: pass"}],"rationale":"keep <, > and & as they are","confidence":"medium"}`,
		got, "keys come in the wire order, with no spaces, no trailing newline and no HTML escaping")
	assert.Equal(t, got, step.AnswerJSON(res), "the same answer always gives the same bytes")

	var back map[string]any
	require.NoError(t, json.Unmarshal([]byte(got), &back))
	assert.Equal(t, "select a < b and c > d & e from t", back["proposed_sql"])
}

func TestFindingLines_NamesTheCodeAndMessage(t *testing.T) {
	got := step.FindingLines([]remediation.Finding{
		{Code: "content_changed", Message: "the answer left the file unchanged"},
		{Code: "sql_parses", Message: "unexpected token", File: "models/a.sql", Line: 3},
		{Code: "confidence_not_low", Message: "the model is not sure", File: "models/b.sql"},
		{Code: "orphan_line", Message: "a line without a file", Line: 7},
	})

	assert.Equal(t, []string{
		"content_changed: the answer left the file unchanged",
		"sql_parses: unexpected token (models/a.sql:3)",
		"confidence_not_low: the model is not sure (models/b.sql)",
		"orphan_line: a line without a file",
	}, got)
}

func TestFindingLines_NoFindingsGivesNoLines(t *testing.T) {
	assert.Empty(t, step.FindingLines(nil))
	assert.Empty(t, step.FindingLines([]remediation.Finding{}))
}
