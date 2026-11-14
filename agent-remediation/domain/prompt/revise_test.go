package prompt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func reviseBaseRequest() ProposeRequest {
	return ProposeRequest{
		System:          "You fix dbt models.",
		User:            "Failed node: analytics.table_e\n\nPropose a corrected version of the failed model's SQL.",
		ToolName:        "propose_fix",
		ToolDescription: "Propose a corrected version of the failed dbt model's SQL.",
		ToolParams: []ToolParam{
			{Name: "proposed_sql", Type: "string", Description: "The complete corrected SQL.", Required: true},
			{Name: "confidence", Type: "string", Description: "low, medium, or high.", Required: true},
		},
	}
}

func TestRevise_KeepsTheToolAndAppendsTheAnswerAndFindings(t *testing.T) {
	req := reviseBaseRequest()

	got := Revise(req, `{"proposed_sql":"select 1","confidence":"high"}`, []string{
		"content_changed: the answer left the file unchanged (models/a.sql)",
		"sql_parses: unexpected token (models/a.sql:3)",
	})

	assert.Equal(t, req.System, got.System)
	assert.Equal(t, req.ToolName, got.ToolName)
	assert.Equal(t, req.ToolDescription, got.ToolDescription)
	assert.Equal(t, req.ToolParams, got.ToolParams)

	const want = "Failed node: analytics.table_e\n\nPropose a corrected version of the failed model's SQL.\n" +
		"\n" +
		"## Your previous answer\n" +
		"```json\n" +
		"{\"proposed_sql\":\"select 1\",\"confidence\":\"high\"}\n" +
		"```\n" +
		"\n" +
		"## What the checks found\n" +
		"- content_changed: the answer left the file unchanged (models/a.sql)\n" +
		"- sql_parses: unexpected token (models/a.sql:3)\n" +
		"\n" +
		"Correct every finding and call the tool again with your complete answer."
	assert.Equal(t, want, got.User)
}

func TestRevise_DoesNotChangeTheOriginalRequest(t *testing.T) {
	req := reviseBaseRequest()
	findings := []string{"a: first", "b: second"}

	got := Revise(req, `{"rationale":"r"}`, findings)

	assert.NotEqual(t, req.User, got.User)
	assert.Equal(t, reviseBaseRequest(), req)
	assert.Equal(t, []string{"a: first", "b: second"}, findings)
}

func TestRevise_RevisingARevisionKeepsTheOriginalTextFirst(t *testing.T) {
	req := reviseBaseRequest()

	first := Revise(req, `{"rationale":"one"}`, []string{"a: first"})
	second := Revise(first, `{"rationale":"two"}`, []string{"b: second"})

	assert.Contains(t, second.User, req.User+"\n\n## Your previous answer\n```json\n{\"rationale\":\"one\"}\n```")
	assert.Contains(t, second.User, "- b: second\n\nCorrect every finding and call the tool again with your complete answer.")
}
