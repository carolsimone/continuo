package serialization

import (
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// A prompt step's row stores the last model call of its run, so a later draft
// can revise it. The request and the answer are not domain values with tags, so
// they are stored as the wire values below: each field has its own key, and
// RequestOf and AnswerOf convert one way, Request and Answer the other, without
// loss.

// RequestWire is a model request as a journal row stores it.
type RequestWire struct {
	System          string          `json:"system"`
	User            string          `json:"user"`
	ToolName        string          `json:"tool_name"`
	ToolDescription string          `json:"tool_description"`
	ToolParams      []ToolParamWire `json:"tool_params,omitempty"`
}

// ToolParamWire is one parameter of a request's forced tool as a journal row
// stores it. Items holds the properties of each element of an array parameter.
type ToolParamWire struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Description string          `json:"description"`
	Required    bool            `json:"required,omitempty"`
	Items       []ToolParamWire `json:"items,omitempty"`
}

// RequestOf is req as a journal row stores it.
func RequestOf(req prompt.ProposeRequest) RequestWire {
	return RequestWire{
		System:          req.System,
		User:            req.User,
		ToolName:        req.ToolName,
		ToolDescription: req.ToolDescription,
		ToolParams:      toolParamsOf(req.ToolParams),
	}
}

// Request is the request the wire value was made from.
func (w RequestWire) Request() prompt.ProposeRequest {
	return prompt.ProposeRequest{
		System:          w.System,
		User:            w.User,
		ToolName:        w.ToolName,
		ToolDescription: w.ToolDescription,
		ToolParams:      toolParams(w.ToolParams),
	}
}

func toolParamsOf(params []prompt.ToolParam) []ToolParamWire {
	if len(params) == 0 {
		return nil
	}
	out := make([]ToolParamWire, 0, len(params))
	for _, p := range params {
		out = append(out, ToolParamWire{
			Name:        p.Name,
			Type:        p.Type,
			Description: p.Description,
			Required:    p.Required,
			Items:       toolParamsOf(p.Items),
		})
	}
	return out
}

func toolParams(wire []ToolParamWire) []prompt.ToolParam {
	if len(wire) == 0 {
		return nil
	}
	out := make([]prompt.ToolParam, 0, len(wire))
	for _, p := range wire {
		out = append(out, prompt.ToolParam{
			Name:        p.Name,
			Type:        p.Type,
			Description: p.Description,
			Required:    p.Required,
			Items:       toolParams(p.Items),
		})
	}
	return out
}

// AnswerWire is a model answer as a journal row stores it, with the call's
// model, token usage and cache flag.
type AnswerWire struct {
	ProposedSQL            string     `json:"proposed_sql,omitempty"`
	ProposedContent        string     `json:"proposed_content,omitempty"`
	TargetFile             string     `json:"target_file,omitempty"`
	Files                  []FileWire `json:"files,omitempty"`
	Rationale              string     `json:"rationale,omitempty"`
	Confidence             string     `json:"confidence,omitempty"`
	SuspectedRootCauseNode string     `json:"suspected_root_cause_node,omitempty"`
	Model                  string     `json:"model,omitempty"`
	InputTokens            int        `json:"input_tokens,omitempty"`
	OutputTokens           int        `json:"output_tokens,omitempty"`
	Cached                 bool       `json:"cached,omitempty"`
}

// FileWire is one file of a model answer as a journal row stores it.
type FileWire struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// AnswerOf is res as a journal row stores it.
func AnswerOf(res ports.ProposeResult) AnswerWire {
	w := AnswerWire{
		ProposedSQL:            res.ProposedSQL,
		ProposedContent:        res.ProposedContent,
		TargetFile:             res.TargetFile,
		Rationale:              res.Rationale,
		Confidence:             res.Confidence,
		SuspectedRootCauseNode: res.SuspectedRootCauseNode,
		Model:                  res.Model,
		InputTokens:            res.Usage.Input,
		OutputTokens:           res.Usage.Output,
		Cached:                 res.Cached,
	}
	for _, f := range res.Files {
		w.Files = append(w.Files, FileWire{Path: f.Path, Content: f.Content})
	}
	return w
}

// Answer is the answer the wire value was made from.
func (w AnswerWire) Answer() ports.ProposeResult {
	res := ports.ProposeResult{
		ProposedSQL:            w.ProposedSQL,
		ProposedContent:        w.ProposedContent,
		TargetFile:             w.TargetFile,
		Rationale:              w.Rationale,
		Confidence:             w.Confidence,
		SuspectedRootCauseNode: w.SuspectedRootCauseNode,
		Model:                  w.Model,
		Usage:                  ports.TokenUsage{Input: w.InputTokens, Output: w.OutputTokens},
		Cached:                 w.Cached,
	}
	for _, f := range w.Files {
		res.Files = append(res.Files, ports.ProposedFile{Path: f.Path, Content: f.Content})
	}
	return res
}
