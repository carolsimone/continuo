// Package prompt assembles the LLM request for a fix proposal from failure
// evidence. It is pure: it produces a provider-agnostic ProposeRequest that the
// LLM adapters translate to their wire format. The propose_fix tool schema is
// defined here so every provider forces the same structured result.
package prompt

import (
	"fmt"
	"strings"
)

type Ancestor struct {
	NodeID        string
	ServiceName   string
	LastCommitSHA string
	FilePath      string
	LastChangedAt string
	Depth         int
}

type Evidence struct {
	NodeID         string
	ErrorSignature string
	CandidateSQL   string
	DBTLog         string
	Repo           string
	CommitSHA      string
	FilePath       string
	Ancestors      []Ancestor
}

type ToolParam struct {
	Name        string
	Type        string
	Description string
	Required    bool
}

type ProposeRequest struct {
	System          string
	User            string
	ToolName        string
	ToolDescription string
	ToolParams      []ToolParam
}

const systemPrompt = `You are a data-engineering assistant that proposes a fix for a failed dbt model.
You are given the failed model's SQL, the dbt error, and metadata about which upstream
models changed recently. Propose a corrected version of the failed model's SQL that makes
validation pass.

Rules:
- Fix the model so validation passes without weakening tests or contracts.
- Return the COMPLETE corrected SQL for the failed model, not a diff.
- Do not invent columns, sources, or refs that are not justified by the evidence.
- If you cannot determine a safe fix, return the original SQL unchanged with a low confidence and an explanation.
- Always respond by calling the propose_fix tool.`

// Assemble builds the provider-agnostic request. The user content embeds the
// evidence; the propose_fix tool schema forces a structured result.
func Assemble(ev Evidence) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Failed node: %s\n\n", ev.NodeID)
	fmt.Fprintf(&u, "Failed model SQL:\n```sql\n%s\n```\n\n", ev.CandidateSQL)
	fmt.Fprintf(&u, "dbt error:\n```\n%s\n```\n\n", ev.DBTLog)
	if len(ev.Ancestors) > 0 {
		u.WriteString("Upstream models, most-recently-changed first:\n")
		for _, a := range ev.Ancestors {
			fmt.Fprintf(&u, "- %s (service=%s, depth=%d, last_changed=%s, commit=%s)\n",
				a.NodeID, a.ServiceName, a.Depth, a.LastChangedAt, a.LastCommitSHA)
		}
		u.WriteString("\n")
	}
	u.WriteString("Propose a corrected version of the failed model's SQL.")

	return ProposeRequest{
		System:          systemPrompt,
		User:            u.String(),
		ToolName:        "propose_fix",
		ToolDescription: "Propose a corrected version of the failed dbt model's SQL.",
		ToolParams: []ToolParam{
			{Name: "proposed_sql", Type: "string", Description: "The complete corrected SQL for the failed model.", Required: true},
			{Name: "rationale", Type: "string", Description: "A short explanation of the fix. Do not include warehouse data values.", Required: true},
			{Name: "confidence", Type: "string", Description: "Your confidence: low, medium, or high.", Required: true},
			{Name: "suspected_root_cause_node", Type: "string", Description: "Optional: the upstream node id you believe caused the failure, or empty.", Required: false},
		},
	}
}
