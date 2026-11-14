package shared

import (
	"context"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// sqlDoesNotParse is the code of a finding sql_parses makes.
const sqlDoesNotParse = "sql_does_not_parse"

// SQLParses is the check `sql_parses`: every SQL file a draft writes must
// parse in the dialect of the install's warehouse.
type SQLParses struct {
	parser ports.SQLParser
}

var _ step.Check = (*SQLParses)(nil)

// NewSQLParses builds the sql_parses step over parser, which parses in the
// install's dialect.
func NewSQLParses(parser ports.SQLParser) *SQLParses { return &SQLParses{parser: parser} }

// Meta describes the step.
func (SQLParses) Meta() step.Meta {
	return step.Meta{
		Name: "sql_parses",
		Kind: step.KindCheck,
		Description: "Fails a draft whose .sql edit does not parse in the warehouse's dialect, with one finding per parse " +
			"error, its file and line. dbt's config block is removed and every other {{ }} expression read as a name; " +
			"a file holding a {% %} statement is not parsed.",
		Needs:  []remediation.EvidenceKey{remediation.KeyDraft},
		Stages: []workflow.Stage{workflow.StagePreflight},
	}
}

// Check parses every edit whose path ends in .sql, in edit order. Each parse
// error is a sql_does_not_parse finding with the edit's file and the error's
// line; a file the parser skipped makes no finding. A parser that cannot
// answer fails the step, so the step is run again rather than the model asked
// to fix SQL nobody read.
func (s SQLParses) Check(ctx context.Context, in step.CheckInput) (step.CheckOutput, error) {
	res := remediation.CheckResult{Check: string(SQLParses{}.Meta().Name)}
	for _, e := range in.Draft.Edits {
		if !strings.HasSuffix(strings.ToLower(e.Path), ".sql") {
			continue
		}
		parsed, err := s.parser.Parse(ctx, e.Content)
		if err != nil {
			return step.CheckOutput{}, fmt.Errorf("parse %s: %w", e.Path, err)
		}
		for _, pe := range parsed.Errors {
			res.Findings = append(res.Findings, remediation.Finding{
				Code: sqlDoesNotParse, Message: pe.Message, File: e.Path, Line: pe.Line,
			})
		}
	}
	return step.CheckOutput{Result: res}, nil
}
