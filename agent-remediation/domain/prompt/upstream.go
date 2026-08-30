package prompt

import (
	"fmt"
	"strings"
)

// MemberFailure is one failing descendant shown to the model as a symptom of
// the upstream change it is asked to repair.
type MemberFailure struct {
	NodeID       string
	ErrorExcerpt string
}

// UpstreamEvidence is what the model sees when several nodes failed the same
// way below one node that changed in this release: that node's source, what
// changed in it, and the descendants with their errors.
type UpstreamEvidence struct {
	TargetNodeID  string
	TargetSource  string
	OwnChangeDiff string
	Members       []MemberFailure
	Precedents    []Precedent
}

const upstreamFixSystemPrompt = `You are a data-engineering assistant that repairs a dbt model whose change broke the models downstream of it.
You are given the changed model's SQL, the diff of what this release changed in it, and the downstream models that now fail validation with their errors. The downstream models did not change; the fix belongs in the changed model.

Rules:
- Correct the changed model so every listed downstream model validates again — typically by restoring a column or relation the change removed or renamed — without reverting unrelated parts of the change.
- Return the COMPLETE corrected SQL for the changed model, not a diff, and never a downstream model's SQL.
- Reference upstream tables by their physical schema.table name; never introduce {{ ref(...) }} or {{ source(...) }} (the dbt projects are independent and these do not resolve across them).
- Do not invent columns or relations the evidence does not justify.
- When past precedents are shown, weigh how the same error was resolved before; follow a precedent's approach only where it fits the code you are shown.
- If you cannot determine a safe fix, return the SQL unchanged with a low confidence and an explanation.
- Always respond by calling the propose_fix tool.`

// AssembleUpstreamFix builds the request for a shared-upstream cluster: one
// call that asks for the changed ancestor's corrected source. It reuses the
// propose_fix{proposed_sql, rationale, confidence} tool shape so every
// provider adapter parses the answer unchanged.
func AssembleUpstreamFix(ev UpstreamEvidence) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Upstream node: %s\n\n", ev.TargetNodeID)
	fmt.Fprintf(&u, "Upstream node source:\n```sql\n%s\n```\n\n", ev.TargetSource)
	if ev.OwnChangeDiff != "" {
		fmt.Fprintf(&u, "What this release changed in %s (last promoted -> candidate):\n```diff\n%s\n```\n\n", ev.TargetNodeID, ev.OwnChangeDiff)
	}
	u.WriteString("Downstream models that fail validation because of this change:\n")
	for _, m := range ev.Members {
		fmt.Fprintf(&u, "- %s: %s\n", m.NodeID, m.ErrorExcerpt)
	}
	u.WriteString("\n")
	renderPrecedents(&u, ev.Precedents)
	fmt.Fprintf(&u, "Return the complete corrected SQL for %s so every downstream model listed validates.", ev.TargetNodeID)

	return ProposeRequest{
		System:          upstreamFixSystemPrompt,
		User:            u.String(),
		ToolName:        "propose_fix",
		ToolDescription: "Return the corrected SQL of the changed upstream model.",
		ToolParams: []ToolParam{
			{Name: "proposed_sql", Type: "string", Description: "The complete corrected SQL for the upstream model.", Required: true},
			{Name: "rationale", Type: "string", Description: "A short explanation of the fix. Do not include warehouse data values.", Required: true},
			{Name: "confidence", Type: "string", Description: "Your confidence: low, medium, or high.", Required: true},
		},
	}
}
