package prompt

import (
	"fmt"
	"strings"
)

// MemberFailure is one failing descendant shown to the model as a symptom of
// the upstream change it is asked to repair. Service is the member's owning
// service; rendered when set.
type MemberFailure struct {
	NodeID       string
	Service      string
	ErrorExcerpt string
}

// UpstreamEvidence is what the model sees when nodes failed below one node that
// changed in this release: that node's source, what changed in it, and the
// descendants with their errors. CrossService marks the descendants as living
// in other services, which cannot change in this release.
type UpstreamEvidence struct {
	TargetNodeID  string
	TargetSource  string
	OwnChangeDiff string
	Members       []MemberFailure
	Precedents    []Precedent
	CrossService  bool
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

// crossServiceClause is appended to the upstream system prompt when the
// failing descendants live in other services. A release changes one service,
// so those descendants cannot change here and the producer must keep serving
// what they read.
const crossServiceClause = `

The failing downstream models live in other services and cannot change in this release: a fix in their own service can never ship before this change.
Repair the changed model so it keeps what it now produces AND keeps the column or relation the downstream models read — keep both, typically by selecting the old name alongside the new one. Do not revert the change and do not edit the downstream models.`

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
		if m.Service != "" {
			fmt.Fprintf(&u, "- %s (service %s): %s\n", m.NodeID, m.Service, m.ErrorExcerpt)
			continue
		}
		fmt.Fprintf(&u, "- %s: %s\n", m.NodeID, m.ErrorExcerpt)
	}
	u.WriteString("\n")
	renderPrecedents(&u, ev.Precedents)
	fmt.Fprintf(&u, "Return the complete corrected SQL for %s so every downstream model listed validates.", ev.TargetNodeID)

	system := upstreamFixSystemPrompt
	if ev.CrossService {
		system += crossServiceClause
	}

	return ProposeRequest{
		System:          system,
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
