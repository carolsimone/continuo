package prompt

import (
	"fmt"
	"strings"
)

// AttemptDiff is one file's unified diff from an earlier fix attempt, named by
// the repository path that attempt changed.
type AttemptDiff struct {
	Path string
	Diff string
}

// PriorAttempt is one earlier fix attempt for the same failing node: the diffs
// it applied and the error the shadow release that verified it reported back.
// Showing both is what makes a later attempt better informed than the one
// before it — without them the model would keep re-proposing the change that
// has already been tried and rejected.
type PriorAttempt struct {
	Attempt     int
	VerifyError string
	Diffs       []AttemptDiff
}

// PythonEvidence is the failure evidence for a python node whose contract
// failed validation. A python node's fix is made in the yaml that declares it,
// not in a SQL file, so the evidence pairs the control plane's normalized view
// of the node (ContractEntry, canonical JSON built by the release's parse) with
// the declaring file's verbatim text (YAMLText at YAMLPath) — the model reads
// the first to see what the release recorded and edits the second.
//
// Every other field is optional: an absent one renders no section at all.
type PythonEvidence struct {
	NodeID string
	// ErrorExcerpt is the classifier's key error line for this failure.
	ErrorExcerpt string
	// RunnerLog is the failing validation run's full log, already sanitized.
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

const pythonContractFixSystemPrompt = `You are a data-engineering assistant that fixes a Continuo python node whose contract failed blue/green validation.

A python node is declared in a contract yaml file: an entry under "nodes:" naming its schema and table, the script that produces it, the upstream relations it reads (each with the SQL that selects them), and the output_columns it promises to produce. Validation runs the node's script against the candidate schema and checks that what it produced matches what the contract declares. It does NOT run the script's python source, which you are not shown and cannot change.

Rules:
- Change ONLY what validation checks: the node's declared reads (including their SQL), its output_columns, and its config. Never touch its schema, table, script path, owner, schedule, or criticality — those identify the node, and changing one makes it a different node rather than a fixed one.
- A contract file may declare several nodes. Leave every node other than the failing one byte-for-byte unchanged.
- Return the COMPLETE new content of every file you change, never a diff and never a fragment. A file you do not change must not appear in your answer at all.
- Fix the contract to match what the node genuinely produces; do not weaken a declared column type or drop a column merely to make the check pass, unless the evidence shows the declaration itself is what is wrong.
- When earlier attempts are shown, read what each one changed and why its verification failed, and do not repeat a change that has already been rejected.
- When past precedents are shown, weigh how the same error was resolved before; follow a precedent's approach only where it fits the contract you are shown.
- Always respond by calling the propose_python_fix tool.`

// AssemblePythonContractFix builds the request that asks the model to correct
// the contract yaml declaring a failed python node. The answer is a list of
// complete files rather than one file's content, because a fix can legitimately
// span the declaring file and a sibling it shares definitions with.
func AssemblePythonContractFix(ev PythonEvidence) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Failed python node: %s\n\n", ev.NodeID)

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
		System:          pythonContractFixSystemPrompt,
		User:            u.String(),
		ToolName:        "propose_python_fix",
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

// renderUpstreamChanges writes the recently-changed-ancestor section: each
// ancestor's code and resolved-config diff, most recent first. No changes → no
// section.
func renderUpstreamChanges(b *strings.Builder, cs []UpstreamChange) {
	if len(cs) == 0 {
		return
	}
	b.WriteString("Recent upstream changes, most recent first (each ancestor's code and resolved-config diff):\n")
	for _, c := range cs {
		fmt.Fprintf(b, "Upstream %s (depth=%d):\n", c.NodeID, c.Depth)
		if c.CodeDiff != "" {
			fmt.Fprintf(b, "```diff\n%s\n```\n", c.CodeDiff)
		}
		if c.ConfigDiff != "" {
			fmt.Fprintf(b, "Config change:\n```diff\n%s\n```\n", c.ConfigDiff)
		}
		if c.Truncated {
			b.WriteString("(diff truncated)\n")
		}
	}
	b.WriteString("\n")
}

// renderPriorAttempts writes the earlier-attempts section: what each attempt
// changed and why its shadow verification rejected it. No attempts → no
// section.
func renderPriorAttempts(b *strings.Builder, as []PriorAttempt) {
	if len(as) == 0 {
		return
	}
	b.WriteString("Previous fix attempts for this node, oldest first — do not repeat a change that was already rejected:\n")
	for _, a := range as {
		fmt.Fprintf(b, "Attempt %d", a.Attempt)
		if a.VerifyError != "" {
			fmt.Fprintf(b, " — verification failed: %s", a.VerifyError)
		}
		b.WriteString("\n")
		for _, d := range a.Diffs {
			fmt.Fprintf(b, "  Changed %s:\n```diff\n%s\n```\n", d.Path, d.Diff)
		}
	}
	b.WriteString("\n")
}
