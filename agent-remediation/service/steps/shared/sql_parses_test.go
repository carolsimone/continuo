package shared_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// fakeSQLParser answers each SQL text with the result results names, or no
// error, and records every text it was given.
type fakeSQLParser struct {
	results map[string]ports.SQLParseResult
	err     error
	calls   []string
}

var _ ports.SQLParser = (*fakeSQLParser)(nil)

func (f *fakeSQLParser) Parse(_ context.Context, sql string) (ports.SQLParseResult, error) {
	f.calls = append(f.calls, sql)
	if f.err != nil {
		return ports.SQLParseResult{}, f.err
	}
	return f.results[sql], nil
}

func TestSQLParses_MetaIsAPreflightCheckOnTheDraft(t *testing.T) {
	m := shared.NewSQLParses(&fakeSQLParser{}).Meta()
	assert.Equal(t, "sql_parses", string(m.Name))
	assert.Equal(t, step.KindCheck, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyDraft}, m.Needs)
	assert.Empty(t, m.Provides)
	assert.Equal(t, []workflow.Stage{workflow.StagePreflight}, m.Stages)
}

func TestSQLParses_ReportsEachErrorWithItsFile(t *testing.T) {
	parser := &fakeSQLParser{results: map[string]ports.SQLParseResult{
		"select from where": {Errors: []ports.SQLParseError{
			{Message: "Expected table name", Line: 1},
			{Message: "Invalid expression", Line: 2},
		}},
		"select (":   {Errors: []ports.SQLParseError{{Message: "Expecting )", Line: 3}}},
		"{% if x %}": {Skipped: "jinja statements: only dbt can render them"},
	}}
	draft := remediation.Draft{Edits: []remediation.Edit{
		{Path: "services/svc/models/a.sql", Content: "select from where"},
		{Path: "services/svc/models/b.sql", Content: "select 1"},
		{Path: "services/svc/models/schema.yml", Content: "select from where"},
		{Path: "services/svc/models/c.sql", Content: "{% if x %}"},
		{Path: "services/svc/models/d.sql", Content: "select ("},
	}}

	out, err := shared.NewSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft})
	require.NoError(t, err)

	assert.Equal(t, "sql_parses", out.Result.Check)
	assert.Equal(t, []remediation.Finding{
		{Code: "sql_does_not_parse", Message: "Expected table name", File: "services/svc/models/a.sql", Line: 1},
		{Code: "sql_does_not_parse", Message: "Invalid expression", File: "services/svc/models/a.sql", Line: 2},
		{Code: "sql_does_not_parse", Message: "Expecting )", File: "services/svc/models/d.sql", Line: 3},
	}, out.Result.Findings, "every error of every .sql edit is a finding; a skipped file and a file that is not .sql are none")
	assert.Equal(t, []string{"select from where", "select 1", "{% if x %}", "select ("}, parser.calls,
		"only the .sql edits are parsed, in edit order")
}

func TestSQLParses_ADraftThatParsesPasses(t *testing.T) {
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Content: "select 1"}}}
	out, err := shared.NewSQLParses(&fakeSQLParser{}).Check(context.Background(), step.CheckInput{Draft: draft})
	require.NoError(t, err)
	assert.True(t, out.Result.Passed())
}

// A parser that cannot answer is a step error, retried, never a finding the
// model would be asked to fix.
func TestSQLParses_AParserFailureIsAStepError(t *testing.T) {
	boom := errors.New("interpreter exited 1")
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Content: "select 1"}}}
	_, err := shared.NewSQLParses(&fakeSQLParser{err: boom}).Check(context.Background(), step.CheckInput{Draft: draft})
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "models/a.sql")
}
