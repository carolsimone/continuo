package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// sqlDoesNotParse is the code of a finding sql_parses makes.
const sqlDoesNotParse = "sql_does_not_parse"

// errNoCheckout is returned for an edit that does not parse, names no original
// and comes with no scratch checkout to read the original from.
var errNoCheckout = errors.New("the edit names no original, and the input holds no checkout to read it from")

// SQLParses is the check `sql_parses`: every SQL file a draft writes must
// parse in the dialect of the install's warehouse.
type SQLParses struct {
	parser ports.SQLParser
	logger *slog.Logger
}

var _ step.Check = (*SQLParses)(nil)

// NewSQLParses builds the sql_parses step over parser, which parses in the
// install's dialect. It logs each file it does not judge on logger, or on the
// process default logger when logger is nil.
func NewSQLParses(parser ports.SQLParser, logger *slog.Logger) *SQLParses {
	return &SQLParses{parser: parser, logger: LoggerOrDefault(logger)}
}

// Meta describes the step.
func (SQLParses) Meta() step.Meta {
	return step.Meta{
		Name: "sql_parses",
		Kind: step.KindCheck,
		Description: "Fails a draft whose .sql edit does not parse in the warehouse's dialect, with one finding per parse " +
			"error, its file and line. Jinja comments and dbt's config block are removed and every other {{ }} " +
			"expression read as a name; " +
			"a file holding a {% %} statement is not parsed, and an edit of a file whose original does not parse " +
			"the same way is not judged.",
		Needs:  []remediation.EvidenceKey{remediation.KeyDraft},
		Stages: []workflow.Stage{workflow.StagePreflight},
	}
}

// Check parses every edit whose path ends in .sql, in edit order. An edit that
// parses makes no finding, and neither does a file the parser skipped. An edit
// that does not parse is judged against its original: the edit's own, else the
// file the pass's checkout read for its path. When that original does not parse
// cleanly under the same reduction either (a macro that renders a whole clause,
// say), an error says nothing about the model's change, and the file is skipped;
// otherwise, and always for a file the edit creates, each parse error is a
// sql_does_not_parse finding with the edit's file and the error's line. Every
// skipped file is logged with its reason. A parser that cannot answer, or a
// checkout that cannot be read, fails the step, so the step is run again rather
// than the model asked to fix SQL nobody read.
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
		if parsed.Skipped != "" {
			s.skip(e.Path, parsed.Skipped)
			continue
		}
		if len(parsed.Errors) == 0 {
			continue
		}
		reason, err := s.originalFails(ctx, in, e)
		if err != nil {
			return step.CheckOutput{}, err
		}
		if reason != "" {
			s.skip(e.Path, reason)
			continue
		}
		for _, pe := range parsed.Errors {
			res.Findings = append(res.Findings, remediation.Finding{
				Code: sqlDoesNotParse, Message: pe.Message, File: e.Path, Line: pe.Line,
			})
		}
	}
	return step.CheckOutput{Result: res}, nil
}

// originalFails returns why the original of e does not parse cleanly, or ""
// when it does or when e creates the file.
func (s SQLParses) originalFails(ctx context.Context, in step.CheckInput, e remediation.Edit) (string, error) {
	original, err := originalOf(ctx, in, e)
	if err != nil {
		return "", fmt.Errorf("read the original of %s: %w", e.Path, err)
	}
	if original == "" {
		return "", nil
	}
	parsed, err := s.parser.Parse(ctx, original)
	if err != nil {
		return "", fmt.Errorf("parse the original of %s: %w", e.Path, err)
	}
	switch {
	case parsed.Skipped != "":
		return "its original was not parsed: " + parsed.Skipped, nil
	case len(parsed.Errors) > 0:
		first := parsed.Errors[0]
		return fmt.Sprintf("its original does not parse either: %s (line %d)", first.Message, first.Line), nil
	}
	return "", nil
}

// originalOf is what the file of e held before the draft: the edit's own
// original, else what the pass's checkout read for its path, empty for a file
// the repository does not have or a path outside the checkout.
func originalOf(ctx context.Context, in step.CheckInput, e remediation.Edit) (string, error) {
	if e.Original != "" {
		return e.Original, nil
	}
	if in.Scratch == nil {
		return "", errNoCheckout
	}
	files := make([]ports.ProposedFile, len(in.Draft.Edits))
	for i, d := range in.Draft.Edits {
		files[i] = ports.ProposedFile{Path: d.Path, Content: d.Content}
	}
	_, originals, err := in.Scratch.PatchedCheckout(ctx, files)
	if errors.Is(err, step.ErrPathOutsideCheckout) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return originals[e.Path], nil
}

// skip logs that the check does not judge the file at path, and why.
func (s SQLParses) skip(path, reason string) {
	s.logger.Info("sql_parses does not judge a file", "file", path, "reason", reason)
}
