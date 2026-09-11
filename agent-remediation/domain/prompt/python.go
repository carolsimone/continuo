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
// it applied and the error the verification run that judged it reported back.
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
- Keep every read the node declares. Correcting a read's SQL is a fix, and adding a read is allowed, but deleting a read or renaming its key is not: the script still performs that read, and you cannot change the script. A contract that no longer declares it passes validation while the node stays broken.
- A contract file may declare several nodes. Leave every node other than the failing one byte-for-byte unchanged.
- Return the COMPLETE new content of every file you change, never a diff and never a fragment. A file you do not change must not appear in your answer at all.
- Fix the contract to match what the node genuinely produces; do not weaken a declared column type or drop a column merely to make the check pass, unless the evidence shows the declaration itself is what is wrong.
- When earlier attempts are shown, read what each one changed and why its verification failed, and do not repeat a change that has already been rejected.
- When past precedents are shown, weigh how the same error was resolved before; follow a precedent's approach only where it fits the contract you are shown.
- Always respond by calling the propose_python_fix tool.`

const pythonParseFixSystemPrompt = `You are a data-engineering assistant that fixes a Continuo python node one of whose declared reads has SQL that the release's SQL parser rejected before any run.

A python node is declared in a contract yaml file: an entry under "nodes:" naming its schema and table, the script that produces it, the upstream relations it reads (each with the SQL that selects them), and the output_columns it promises to produce. Before a release runs anything, every read's SQL is parsed and every relation it names must be schema-qualified so the release can resolve it to an upstream node. The parser rejected one of this node's reads, and its error names the line and column where parsing stopped. The script's python source is not parsed, is not shown to you, and cannot be changed.

Rules:
- Change ONLY the SQL of the node's declared reads, so that every read parses and every relation it references is schema-qualified. Never touch the node's schema, table, script path, owner, schedule, criticality, or output_columns — those identify the node or are checked later by validation, not by the parser.
- Keep every read the node declares. Correcting a read's SQL is the fix; deleting a read or renaming its key is not: the script still performs that read, and you cannot change the script. A contract that no longer declares it parses while the node stays broken.
- Qualify a relation with the schema the evidence shows it lives in; do not invent a schema, and do not rewrite what the read selects beyond what parsing requires.
- A contract file may declare several nodes. Leave every node other than the failing one byte-for-byte unchanged.
- Return the COMPLETE new content of every file you change, never a diff and never a fragment. A file you do not change must not appear in your answer at all.
- When earlier attempts are shown, read what each one changed and why its verification failed, and do not repeat a change that has already been rejected.
- When past precedents are shown, weigh how the same error was resolved before; follow a precedent's approach only where it fits the contract you are shown.
- Always respond by calling the propose_python_fix tool.`

// AssemblePythonContractFix builds the request that asks the model to correct
// the contract yaml declaring a python node that failed validation. The answer
// is a list of complete files rather than one file's content, because a fix
// can legitimately span the declaring file and a sibling it shares definitions
// with.
func AssemblePythonContractFix(ev PythonEvidence) ProposeRequest {
	return pythonContractRequest(pythonContractFixSystemPrompt, "Validation error", ev)
}

// AssemblePythonParseFix builds the request that asks the model to correct a
// read's SQL in the contract yaml declaring a python node the release's SQL
// parser rejected. It has the validation fix's answer shape — the same tool
// and the same complete-files contract — so one adapter parses both; the
// evidence differs in that the error is the parser's own text and there is no
// runner log, since no Job ran.
func AssemblePythonParseFix(ev PythonEvidence) ProposeRequest {
	return pythonContractRequest(pythonParseFixSystemPrompt, "SQL parse error", ev)
}

// pythonContractRequest renders the one request shape every python contract
// fix uses: the failure under errorLabel, then each evidence section that has
// something to say, then the tool that returns complete files.
func pythonContractRequest(system, errorLabel string, ev PythonEvidence) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Failed python node: %s\n\n", ev.NodeID)

	if ev.ErrorExcerpt != "" {
		fmt.Fprintf(&u, "%s:\n```\n%s\n```\n\n", errorLabel, ev.ErrorExcerpt)
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
		System:          system,
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
			{Name: "rationale", Type: "string", Description: "One sentence describing the change you made. No warehouse data values.", Required: true},
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

// renderReleaseUpstreamChanges writes what this release changed upstream of
// the failing node, nearest first, or the one line stating that nothing did.
// Nothing is written when the lane could not know (known is false).
func renderReleaseUpstreamChanges(b *strings.Builder, nodeID string, cs []ReleaseUpstreamChange, known bool) {
	if !known {
		return
	}
	if len(cs) == 0 {
		fmt.Fprintf(b, "No upstream of %s changed in this release.\n\n", nodeID)
		return
	}
	b.WriteString("What this release changed upstream of the failing model (nearest first; last promoted -> candidate):\n")
	for _, c := range cs {
		fmt.Fprintf(b, "Upstream %s (service %s, depth=%d):\n", c.NodeID, c.Service, c.Depth)
		if c.Diff == "" {
			b.WriteString("(diff unavailable)\n")
			continue
		}
		fmt.Fprintf(b, "```diff\n%s\n```\n", c.Diff)
	}
	b.WriteString("\n")
}

// renderPriorAttempts writes the earlier-attempts section: what each attempt
// changed and why its verification run failed it. No attempts → no
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
