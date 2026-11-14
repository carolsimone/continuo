package step

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// answerWire is a model answer as the fix tool's input carries it: the field
// names are the ones the LLM adapters read from a tool call, in their order,
// and a field the model left empty is left out. The model, token usage and
// cache flag of a ProposeResult describe the call, not the tool input, so they
// have no field here.
type answerWire struct {
	ProposedSQL            string          `json:"proposed_sql,omitempty"`
	ProposedContent        string          `json:"proposed_content,omitempty"`
	TargetFile             string          `json:"target_file,omitempty"`
	UpdatedFiles           []answerFileRow `json:"updated_files,omitempty"`
	Rationale              string          `json:"rationale,omitempty"`
	Confidence             string          `json:"confidence,omitempty"`
	SuspectedRootCauseNode string          `json:"suspected_root_cause_node,omitempty"`
}

// answerFileRow is one element of the tool input's updated_files array.
type answerFileRow struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// AnswerJSON renders res as the tool input the model sent: canonical JSON with
// the provider wire names, the fields in a fixed order, no insignificant
// whitespace and no HTML escaping, so one answer always gives the same bytes.
// It is shown back to the model in a revision request, which makes it part of
// a cached request.
func AnswerJSON(res ports.ProposeResult) string {
	wire := answerWire{
		ProposedSQL:            res.ProposedSQL,
		ProposedContent:        res.ProposedContent,
		TargetFile:             res.TargetFile,
		Rationale:              res.Rationale,
		Confidence:             res.Confidence,
		SuspectedRootCauseNode: res.SuspectedRootCauseNode,
	}
	for _, f := range res.Files {
		wire.UpdatedFiles = append(wire.UpdatedFiles, answerFileRow{Path: f.Path, Content: f.Content})
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	// A struct of strings and a slice of such structs always encodes, so the
	// error is nil.
	_ = enc.Encode(wire)
	return string(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
}

// FindingLines renders each finding as one line, "<code>: <message>", followed
// by " (<file>:<line>)" when the finding names a file and a line, or
// " (<file>)" when it names a file only. A line number without a file is not
// shown.
func FindingLines(fs []remediation.Finding) []string {
	lines := make([]string, 0, len(fs))
	for _, f := range fs {
		line := f.Code + ": " + f.Message
		switch {
		case f.File != "" && f.Line > 0:
			line += fmt.Sprintf(" (%s:%d)", f.File, f.Line)
		case f.File != "":
			line += fmt.Sprintf(" (%s)", f.File)
		}
		lines = append(lines, line)
	}
	return lines
}
