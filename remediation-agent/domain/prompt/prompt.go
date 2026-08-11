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
	LastRepo      string
	FilePath      string
	LastChangedAt string
	Depth         int
}

// UpstreamDiff is the diff of a recent change to one upstream ancestor, shown to
// the model as diagnostic context for a validation failure.
type UpstreamDiff struct {
	NodeID      string
	ServiceName string
	Diff        string
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
	UpstreamDiffs  []UpstreamDiff
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

const sourceFixSystemPrompt = `You are a data-engineering assistant that applies a diagnosed fix to a dbt model's ACTUAL source file.
You are given the original model source and a diagnosis of the fix to apply. The dbt projects in this system are independent and unaware of each other, so models reference upstream tables by their physical schema-qualified name (e.g. analytics.table_a), NOT with dbt {{ ref(...) }} / {{ source(...) }}.

Rules:
- Apply the diagnosed fix to the original source, preserving its formatting style, its {{ config(...) }} block, and any other macros that are not table references.
- Reference every upstream table by its physical schema.table name, in the same style the existing model already uses. NEVER introduce {{ ref(...) }} or {{ source(...) }}: these do not resolve across the independent dbt projects and would break the model.
- Return the complete corrected source for the model, not a diff and not a fragment.
- If the diagnosis cannot be safely applied, return the original source unchanged with low confidence and an explanation.
- Always respond by calling the propose_fix tool.`

// AssembleSourceFix builds the Step-2 request: apply the Step-1 diagnosis to the
// real model source. It reuses the propose_fix tool schema (proposed_sql holds
// the complete corrected source) so the provider adapters parse it unchanged.
func AssembleSourceFix(originalSource, nodeID, diagnosis string) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Model: %s\n\n", nodeID)
	fmt.Fprintf(&u, "Original model source:\n```sql\n%s\n```\n\n", originalSource)
	fmt.Fprintf(&u, "Diagnosis to apply:\n%s\n\n", diagnosis)
	u.WriteString("Return the complete corrected source for this model.")

	return ProposeRequest{
		System:          sourceFixSystemPrompt,
		User:            u.String(),
		ToolName:        "propose_fix",
		ToolDescription: "Return the complete corrected source for the dbt model.",
		ToolParams: []ToolParam{
			{Name: "proposed_sql", Type: "string", Description: "The complete corrected model source.", Required: true},
			{Name: "rationale", Type: "string", Description: "A short explanation of the change. No warehouse data values.", Required: true},
			{Name: "confidence", Type: "string", Description: "Your confidence: low, medium, or high.", Required: true},
		},
	}
}

// NamedFile is one source file shown to the model in a multi-file prompt.
type NamedFile struct {
	Path    string
	Content string
}

const compileFixSystemPrompt = `You are a data-engineering assistant that fixes a dbt project that failed to compile.
You are given every candidate source file (the offending model and its co-located schema.yml and the project's dbt_project.yml) and the dbt compile error. The dbt projects are independent and reference upstream tables by their physical schema-qualified name (e.g. analytics.table_a), NEVER with {{ ref(...) }} / {{ source(...) }}.

Rules:
- Decide which ONE file must change to make dbt compile succeed, and return its path in target_file. It must be one of the files shown to you.
- Return the COMPLETE corrected content of that file in proposed_content, preserving formatting and unrelated content.
- Never introduce {{ ref(...) }} or {{ source(...) }}.
- If you cannot determine a safe fix, return the offending file unchanged with low confidence and an explanation.
- Always respond by calling the propose_fix tool.`

// AssembleCompileFix builds a multi-file compile-fix request. The model chooses
// which shown file to change (target_file) and returns its corrected content.
func AssembleCompileFix(files []NamedFile, dbtLog, nodeID string) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Service: %s\n\n", nodeID)
	for _, f := range files {
		fmt.Fprintf(&u, "File %s:\n```\n%s\n```\n\n", f.Path, f.Content)
	}
	fmt.Fprintf(&u, "dbt compile error:\n```\n%s\n```\n\n", dbtLog)
	u.WriteString("Return the complete corrected content of the ONE file that must change.")

	return ProposeRequest{
		System:          compileFixSystemPrompt,
		User:            u.String(),
		ToolName:        "propose_fix",
		ToolDescription: "Return the corrected content of the one file that fixes dbt compile.",
		ToolParams: []ToolParam{
			{Name: "target_file", Type: "string", Description: "The path of the file to change; must be one of the files shown.", Required: true},
			{Name: "proposed_content", Type: "string", Description: "The complete corrected content of target_file.", Required: true},
			{Name: "rationale", Type: "string", Description: "A short explanation of the change. No warehouse data values.", Required: true},
			{Name: "confidence", Type: "string", Description: "Your confidence: low, medium, or high.", Required: true},
			{Name: "suspected_root_cause_node", Type: "string", Description: "Optional: the upstream node id you believe caused the failure, or empty.", Required: false},
		},
	}
}

const seedFixSystemPrompt = `You are a data-engineering assistant that fixes a dbt seed CSV that failed to load.
You are given the seed CSV file and the dbt seed error. dbt seed loads a comma-separated file into a table; loads fail on quoting problems (a stray comma inside an unquoted text field), a malformed row (wrong column count), or a value that does not match its column type.

Rules:
- Return the COMPLETE corrected CSV in proposed_content, changing only what the error requires (fix quoting, repair the malformed row). Preserve every other row and the header exactly.
- Do NOT invent or guess data values. If the failure is a genuinely wrong or missing data value that cannot be inferred from the file and error alone, return the CSV UNCHANGED with low confidence and explain why in rationale.
- Always respond by calling the propose_fix tool.`

// AssembleSeedFix builds a CSV-specific seed-fix request.
func AssembleSeedFix(csvPath, csvContent, dbtLog, nodeID string) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Seed: %s (node %s)\n\n", csvPath, nodeID)
	fmt.Fprintf(&u, "CSV content:\n```\n%s\n```\n\n", csvContent)
	fmt.Fprintf(&u, "dbt seed error:\n```\n%s\n```\n\n", dbtLog)
	u.WriteString("Return the complete corrected CSV, or the CSV unchanged with low confidence if the bad value cannot be inferred.")

	return ProposeRequest{
		System:          seedFixSystemPrompt,
		User:            u.String(),
		ToolName:        "propose_fix",
		ToolDescription: "Return the complete corrected seed CSV.",
		ToolParams: []ToolParam{
			{Name: "proposed_content", Type: "string", Description: "The complete corrected CSV content.", Required: true},
			{Name: "rationale", Type: "string", Description: "A short explanation. No warehouse data values.", Required: true},
			{Name: "confidence", Type: "string", Description: "Your confidence: low, medium, or high. Use low when the bad value cannot be inferred.", Required: true},
		},
	}
}

const duplicateTableSystemPrompt = `You are a data-engineering assistant that resolves a naming collision in a dbt or python data model.

Two models in the release produce the same warehouse relation (<schema>.<table>). A relation may be produced by exactly one model, so one of them must produce a different one. You are shown only the model that is changing in this release; that is the one to rename. You are told which other service already produces the relation, but not its source.

Rules:
- Change ONLY what is needed to make this model produce a different relation: its alias, its configured schema, or its model name as the project's conventions require. Do not alter the model's logic, its columns, or its ordering.
- Pick a name that describes what THIS model produces and could not collide again — prefer qualifying it with the owning service or the grain, not a numeric suffix.
- Return the COMPLETE corrected file content in proposed_content.
- If the file gives you no safe way to change the relation it produces, return it UNCHANGED with low confidence and explain why in rationale.
- Always respond by calling the propose_fix tool.`

// AssembleDuplicateTableFix builds a rename request for the one model that must
// stop producing a contested relation. Only the changing model's source is
// shown; the competing producer is named by service and path — never by its
// content — so the model can choose a distinguishing name without reading the
// other source. The path matters because both producers can belong to one
// service, where the service name alone identifies nothing.
func AssembleDuplicateTableFix(file NamedFile, uniqueID, otherService, otherFilePath string) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Relation: %s\n", uniqueID)
	fmt.Fprintf(&u, "Also produced by: %s (%s)\n\n", otherService, otherFilePath)
	fmt.Fprintf(&u, "File %s:\n```\n%s\n```\n\n", file.Path, file.Content)
	u.WriteString("Return the complete corrected content of this file, renaming what it produces so it no longer collides.")

	return ProposeRequest{
		System:          duplicateTableSystemPrompt,
		User:            u.String(),
		ToolName:        "propose_fix",
		ToolDescription: "Return the corrected file content that makes this model produce a different relation.",
		ToolParams: []ToolParam{
			{Name: "target_file", Type: "string", Description: "The path of the file to change; must be the file shown.", Required: true},
			{Name: "proposed_content", Type: "string", Description: "The complete corrected content of target_file.", Required: true},
			{Name: "rationale", Type: "string", Description: "A short explanation of the change. No warehouse data values.", Required: true},
			{Name: "confidence", Type: "string", Description: "Your confidence: low, medium, or high.", Required: true},
		},
	}
}

const systemPrompt = `You are a data-engineering assistant that proposes a fix for a failed dbt model.
You are given the failed model's SQL, the dbt error, and metadata about which upstream
models changed recently. Propose a corrected version of the failed model's SQL that makes
validation pass.

Rules:
- Fix the model so validation passes without weakening tests or contracts.
- Return the COMPLETE corrected SQL for the failed model, not a diff.
- Reference upstream tables by their physical schema.table name; never introduce {{ ref(...) }} or {{ source(...) }} (the dbt projects are independent and these do not resolve across them).
- Do not invent columns, sources, or refs that are not justified by the evidence.
- When upstream change diffs are provided, treat them as the most likely root cause and account for what changed upstream when correcting the failed model.
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
	if len(ev.UpstreamDiffs) > 0 {
		u.WriteString("Recent upstream changes (diffs of what changed in those upstream models):\n")
		for _, d := range ev.UpstreamDiffs {
			fmt.Fprintf(&u, "Upstream %s (service=%s):\n```diff\n%s\n```\n\n", d.NodeID, d.ServiceName, d.Diff)
		}
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
