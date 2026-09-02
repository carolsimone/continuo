package prompt

import (
	"fmt"
	"strings"
)

// CsvEvidence is the failure evidence for a python-csv node whose contract
// failed validation. A python-csv node has no script at all: it is an entry
// under "nodes:" naming its schema and table, exactly one read — the csv: key
// pointing at the file the runtime loads — and the output_columns it promises
// that file to carry. The evidence therefore has the same shape as a python
// node's (a normalized ContractEntry paired with the declaring file's verbatim
// YAMLText at YAMLPath), because the same contract-yaml-plus-verification-run
// mechanism verifies both; only what the model is told a fix may touch
// differs, in the system prompt.
//
// Every field but NodeID is optional: an absent one renders no section at all.
type CsvEvidence struct {
	NodeID string
	// ErrorExcerpt is the classifier's key error line for this failure.
	ErrorExcerpt string
	// RunnerLog is the failing validation run's full log, already sanitized.
	// For a python-csv node this is where the header check's own message lives
	// — "csv header missing declared column(s): [...]" — alongside any
	// csv_header_extra_columns warning the runner logged.
	RunnerLog string
	// ContractEntry is the node's entry from the release's code bundle: its
	// normalized contract as canonical JSON.
	ContractEntry string
	// YAMLPath and YAMLText locate and carry the contract file that declares
	// the node, verbatim as the repository holds it at the failing commit.
	YAMLPath        string
	YAMLText        string
	UpstreamChanges []UpstreamChange
	Precedents      []Precedent
	// PriorAttempts are the earlier attempts at this same failure, oldest
	// first.
	PriorAttempts []PriorAttempt
}

const csvContractFixSystemPrompt = `You are a data-engineering assistant that fixes a Continuo python-csv node whose contract failed blue/green validation.

A python-csv node is contract-only: an entry under "nodes:" naming its schema and table (kind: python-csv), exactly one read — a "csv" key whose value is the uri of the CSV file the node loads — and the output_columns it promises that file to carry. There is NO script: nothing runs to produce the relation. Validation fetches the CSV file's header line and rejects the release when a declared output column is missing from it.

THE CSV FILE IS THE SOURCE OF TRUTH. The header the runtime read is a fact about a file that already exists; the contract is what may be wrong. So the fix corrects the contract to match the file: rename or remove a mis-declared output_columns entry so it matches the file's real header, or — only when the evidence itself shows the uri is stale, such as a dated export path that has since rolled forward — correct the "csv:" uri to point at the right file.

Rules:
- Never change schema, table, owner, schedule, criticality, or the node's kind — those identify the node, and changing one makes it a different node rather than a fixed one.
- The node's single "csv" read may be corrected — its uri is exactly what a stale-path fix touches — but it may never be deleted or renamed to another key. A python-csv node with no "csv" read is not a python-csv node; the runtime would have nothing to load.
- Do not invent a column the evidence does not show the file carrying, and do not drop a declared column merely to make the check pass unless the evidence shows that column is genuinely absent from the file's header.
- A contract file may declare several nodes. Leave every node other than the failing one byte-for-byte unchanged.
- Return the COMPLETE new content of every file you change, never a diff and never a fragment. A file you do not change must not appear in your answer at all.
- When earlier attempts are shown, read what each one changed and why its verification failed, and do not repeat a change that has already been rejected.
- When past precedents are shown, weigh how the same error was resolved before; follow a precedent's approach only where it fits the contract you are shown.
- Always respond by calling the propose_csv_fix tool.`

// AssembleCsvContractFix builds the request that asks the model to correct
// the contract yaml declaring a failed python-csv node. The answer is a list
// of complete files rather than one file's content, because a fix can
// legitimately span the declaring file and a sibling it shares definitions
// with.
func AssembleCsvContractFix(ev CsvEvidence) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Failed python-csv node: %s\n\n", ev.NodeID)

	if ev.ErrorExcerpt != "" {
		fmt.Fprintf(&u, "Validation error:\n```\n%s\n```\n\n", ev.ErrorExcerpt)
	}
	if ev.RunnerLog != "" {
		fmt.Fprintf(&u, "Full runner log:\n```\n%s\n```\n\n", ev.RunnerLog)
	}
	if ev.ContractEntry != "" {
		fmt.Fprintf(&u, "Contract entry recorded for this node by the release:\n```json\n%s\n```\n\n", ev.ContractEntry)
	}
	if ev.YAMLText != "" {
		fmt.Fprintf(&u, "Contract file %s that declares it:\n```yaml\n%s\n```\n\n", ev.YAMLPath, ev.YAMLText)
	}

	renderUpstreamChanges(&u, ev.UpstreamChanges)
	renderPrecedents(&u, ev.Precedents)
	renderPriorAttempts(&u, ev.PriorAttempts)

	u.WriteString("Return the complete new content of every file you change.")

	return ProposeRequest{
		System:          csvContractFixSystemPrompt,
		User:            u.String(),
		ToolName:        "propose_csv_fix",
		ToolDescription: "Return the complete new content of every contract file that must change.",
		ToolParams: []ToolParam{
			{
				Name:        "updated_files",
				Type:        "array",
				Description: "Every file you changed, each with its repository path and its complete new content. Omit files you did not change.",
				Required:    true,
				Items: []ToolParam{
					{Name: "path", Type: "string", Description: "The file's repository path, exactly as shown to you."},
					{Name: "content", Type: "string", Description: "The complete new content of that file."},
				},
			},
			{Name: "rationale", Type: "string", Description: "A short explanation of the change. No warehouse data values.", Required: true},
			{Name: "confidence", Type: "string", Description: "Your confidence: low, medium, or high.", Required: true},
		},
	}
}
