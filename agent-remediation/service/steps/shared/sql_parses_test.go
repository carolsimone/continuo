package shared_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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

// repoArchive is a checkout that is never written to disk: the patcher answers
// the originals itself.
type repoArchive struct{ fetches int }

func (a *repoArchive) Fetch(context.Context, string, string) (string, func(), error) {
	a.fetches++
	return "/checkout", func() {}, nil
}

// repoPatcher answers each applied path with what the repository holds at it,
// empty for a path it does not have.
type repoPatcher struct{ files map[string]string }

func (p repoPatcher) Apply(_ string, files []ports.ProposedFile) (map[string]string, error) {
	out := make(map[string]string, len(files))
	for _, f := range files {
		out[f.Path] = p.files[f.Path]
	}
	return out, nil
}

// scratchOver is a pass's scratch checkout over a repository holding files.
func scratchOver(t *testing.T, files map[string]string) (*step.Scratch, *repoArchive) {
	t.Helper()
	a := &repoArchive{}
	s := step.NewScratch(a, repoPatcher{files: files}, "o/repo", "sha")
	t.Cleanup(s.Close)
	return s, a
}

func newSQLParses(parser ports.SQLParser) *shared.SQLParses {
	return shared.NewSQLParses(parser, discardLogger())
}

func TestSQLParses_MetaIsAPreflightCheckOnTheDraft(t *testing.T) {
	m := newSQLParses(&fakeSQLParser{}).Meta()
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
		{Path: "services/svc/models/a.sql", Original: "select 1", Content: "select from where"},
		{Path: "services/svc/models/b.sql", Original: "select 1", Content: "select 2"},
		{Path: "services/svc/models/schema.yml", Content: "select from where"},
		{Path: "services/svc/models/c.sql", Original: "select 1", Content: "{% if x %}"},
		{Path: "services/svc/models/d.sql", Original: "select 1", Content: "select ("},
	}}

	out, err := newSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft})
	require.NoError(t, err)

	assert.Equal(t, "sql_parses", out.Result.Check)
	assert.Equal(t, []remediation.Finding{
		{Code: "sql_does_not_parse", Message: "Expected table name", File: "services/svc/models/a.sql", Line: 1},
		{Code: "sql_does_not_parse", Message: "Invalid expression", File: "services/svc/models/a.sql", Line: 2},
		{Code: "sql_does_not_parse", Message: "Expecting )", File: "services/svc/models/d.sql", Line: 3},
	}, out.Result.Findings, "every error of every .sql edit is a finding; a skipped file and a file that is not .sql are none")
	assert.Equal(t, []string{"select from where", "select 1", "select 2", "{% if x %}", "select (", "select 1"}, parser.calls,
		"only the .sql edits are parsed, in edit order, and the original of each edit that does not parse")
}

func TestSQLParses_ADraftThatParsesPasses(t *testing.T) {
	parser := &fakeSQLParser{}
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Content: "select 1"}}}

	out, err := newSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft})

	require.NoError(t, err)
	assert.True(t, out.Result.Passed())
	assert.Equal(t, []string{"select 1"}, parser.calls, "an edit that parses needs no original")
}

// An edit that breaks a file whose original parses is the model's error.
func TestSQLParses_AnEditThatBreaksAFileThatParsedIsAFinding(t *testing.T) {
	parser := &fakeSQLParser{results: map[string]ports.SQLParseResult{
		"select id form t": {Errors: []ports.SQLParseError{{Message: "Invalid expression / Unexpected token", Line: 1}}},
	}}
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Original: "select id from t", Content: "select id form t"}}}

	out, err := newSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft})

	require.NoError(t, err)
	assert.Equal(t, []remediation.Finding{
		{Code: "sql_does_not_parse", Message: "Invalid expression / Unexpected token", File: "models/a.sql", Line: 1},
	}, out.Result.Findings)
	assert.Equal(t, []string{"select id form t", "select id from t"}, parser.calls)
}

// A file whose original does not parse under the same reduction holds
// something the parser cannot read, such as a macro that renders a whole
// clause, so an error in the edit says nothing about the model's change: the
// file is skipped, and the skip is logged.
func TestSQLParses_AFileWhoseOriginalDoesNotParseIsSkipped(t *testing.T) {
	const (
		original = "select a, count(*) from {{ ref('x') }} where a > 1 {{ dbt_utils.group_by(1) }}"
		edit     = "select a, count(*) from {{ ref('x') }} where a > 2 {{ dbt_utils.group_by(1) }}"
	)
	clauseMacro := ports.SQLParseResult{Errors: []ports.SQLParseError{{Message: "Invalid expression / Unexpected token", Line: 1}}}
	parser := &fakeSQLParser{results: map[string]ports.SQLParseResult{original: clauseMacro, edit: clauseMacro}}
	var logs bytes.Buffer
	check := shared.NewSQLParses(parser, slog.New(slog.NewTextHandler(&logs, nil)))
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Original: original, Content: edit}}}

	out, err := check.Check(context.Background(), step.CheckInput{Draft: draft})

	require.NoError(t, err)
	assert.True(t, out.Result.Passed(), "an edit of a file whose original does not parse makes no finding")
	assert.Contains(t, logs.String(), "level=INFO")
	assert.Contains(t, logs.String(), "file=models/a.sql")
	assert.Contains(t, logs.String(), `reason="its original does not parse either: Invalid expression / Unexpected token (line 1)"`)
}

// An original the parser skipped (it holds a {% %} statement) does not parse
// cleanly either, so the edit is not judged.
func TestSQLParses_AFileWhoseOriginalWasSkippedIsSkipped(t *testing.T) {
	parser := &fakeSQLParser{results: map[string]ports.SQLParseResult{
		"{% set x = 1 %}select": {Skipped: "jinja statements: only dbt can render them"},
		"select from":           {Errors: []ports.SQLParseError{{Message: "Expected table name", Line: 1}}},
	}}
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Original: "{% set x = 1 %}select", Content: "select from"}}}

	out, err := newSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft})

	require.NoError(t, err)
	assert.True(t, out.Result.Passed())
}

// A file the edit creates has no original to blame, so its errors are always
// the model's.
func TestSQLParses_ANewFileThatDoesNotParseIsAFinding(t *testing.T) {
	parser := &fakeSQLParser{results: map[string]ports.SQLParseResult{
		"select (": {Errors: []ports.SQLParseError{{Message: "Expecting )", Line: 1}}},
	}}
	scratch, archive := scratchOver(t, map[string]string{})
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/new.sql", Content: "select ("}}}

	out, err := newSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft, Scratch: scratch})

	require.NoError(t, err)
	assert.Equal(t, []remediation.Finding{
		{Code: "sql_does_not_parse", Message: "Expecting )", File: "models/new.sql", Line: 1},
	}, out.Result.Findings)
	assert.Equal(t, 1, archive.fetches, "the pass's checkout says the file is new")
	assert.Equal(t, []string{"select ("}, parser.calls, "a new file has no original to parse")
}

// An edit that names no original is judged against the file the pass's
// checkout read for its path.
func TestSQLParses_AnEditWithNoOriginalIsJudgedAgainstTheCheckout(t *testing.T) {
	clauseMacro := ports.SQLParseResult{Errors: []ports.SQLParseError{{Message: "Invalid expression / Unexpected token", Line: 1}}}
	parser := &fakeSQLParser{results: map[string]ports.SQLParseResult{
		"select * from x {{ incremental_where() }}":  clauseMacro,
		"select id from x {{ incremental_where() }}": clauseMacro,
		"select id form y":                           clauseMacro,
	}}
	scratch, _ := scratchOver(t, map[string]string{
		"models/a.sql": "select * from x {{ incremental_where() }}",
		"models/b.sql": "select id from y",
	})
	draft := remediation.Draft{Edits: []remediation.Edit{
		{Path: "models/a.sql", Content: "select id from x {{ incremental_where() }}"},
		{Path: "models/b.sql", Content: "select id form y"},
	}}

	out, err := newSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft, Scratch: scratch})

	require.NoError(t, err)
	assert.Equal(t, []remediation.Finding{
		{Code: "sql_does_not_parse", Message: "Invalid expression / Unexpected token", File: "models/b.sql", Line: 1},
	}, out.Result.Findings, "a.sql's original does not parse and is skipped; b.sql's does, so its edit is judged")
}

// The checkout is read only for an edit that does not parse and names no
// original; without one the step cannot judge it, which is an error.
func TestSQLParses_AnEditWithNoOriginalAndNoCheckoutIsAStepError(t *testing.T) {
	parser := &fakeSQLParser{results: map[string]ports.SQLParseResult{
		"select (": {Errors: []ports.SQLParseError{{Message: "Expecting )", Line: 1}}},
	}}
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Content: "select ("}}}

	_, err := newSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "models/a.sql")
}

// A file the parser skips is logged with its reason.
func TestSQLParses_ASkippedFileIsLogged(t *testing.T) {
	parser := &fakeSQLParser{results: map[string]ports.SQLParseResult{
		"{% if x %}": {Skipped: "jinja statements: only dbt can render them"},
	}}
	var logs bytes.Buffer
	check := shared.NewSQLParses(parser, slog.New(slog.NewTextHandler(&logs, nil)))
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/c.sql", Content: "{% if x %}"}}}

	out, err := check.Check(context.Background(), step.CheckInput{Draft: draft})

	require.NoError(t, err)
	assert.True(t, out.Result.Passed())
	assert.Contains(t, logs.String(), "level=INFO")
	assert.Contains(t, logs.String(), "file=models/c.sql")
	assert.Contains(t, logs.String(), `reason="jinja statements: only dbt can render them"`)
}

// A parser that cannot answer is a step error, retried, never a finding the
// model would be asked to fix.
func TestSQLParses_AParserFailureIsAStepError(t *testing.T) {
	boom := errors.New("interpreter exited 1")
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Content: "select 1"}}}
	_, err := newSQLParses(&fakeSQLParser{err: boom}).Check(context.Background(), step.CheckInput{Draft: draft})
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "models/a.sql")
}

// A parser that cannot answer for the original is a step error too.
func TestSQLParses_AParserFailureOnTheOriginalIsAStepError(t *testing.T) {
	boom := errors.New("interpreter exited 1")
	parser := &originalFailingParser{err: boom, broken: "select ("}
	draft := remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Original: "select 1", Content: "select ("}}}

	_, err := newSQLParses(parser).Check(context.Background(), step.CheckInput{Draft: draft})

	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "original of models/a.sql")
}

// originalFailingParser reports broken as not parsing and fails on any other
// text.
type originalFailingParser struct {
	err    error
	broken string
}

func (p *originalFailingParser) Parse(_ context.Context, sql string) (ports.SQLParseResult, error) {
	if sql == p.broken {
		return ports.SQLParseResult{Errors: []ports.SQLParseError{{Message: "Expecting )", Line: 1}}}, nil
	}
	return ports.SQLParseResult{}, p.err
}
