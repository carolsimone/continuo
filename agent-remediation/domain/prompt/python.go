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
// failed validation. A python node's fix is made in its ODCS contract file, not
// in a SQL file, so the evidence pairs the control plane's normalized view of
// the node (ContractEntry, canonical JSON built by the release's parse) with
// the contract file's verbatim text (YAMLText at YAMLPath) — the model reads
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
	// YAMLPath and YAMLText locate and carry the ODCS contract file that
	// declares the node, verbatim as the repository holds it at the failing
	// commit.
	YAMLPath string
	YAMLText string
	// CurrentContract is the node's contract as continuo last promoted it,
	// the published ODCS YAML the catalog holds; empty when the node has
	// none yet.
	CurrentContract string
	UpstreamChanges []UpstreamChange
	Precedents      []Precedent
	// PriorAttempts are the earlier attempts at this same failure, oldest
	// first.
	PriorAttempts []PriorAttempt
	// NoReads is set when the node declares no reads and fetches its own data.
	NoReads bool
}

// pythonContractDescription describes the ODCS contract file a python node is
// declared in, for every python prompt. Its reads clause is the one
// noReadsSystemPrompt rewrites.
const pythonContractDescription = `A python node is declared in its own ODCS contract file, contracts/<table>.odcs.yaml: one contract per node. Its id is "<schema>.<table>". Its continuo customProperties name the script that produces it and, under readQueries, the upstream relations it reads: each query is one SELECT that names every table it reads with its schema. The columns it promises to produce are the properties of its one schema object (schema[0].properties), each with a name, a physicalType and whether it is required.`

const pythonContractFixSystemPrompt = `You are a data-engineering assistant that fixes a Continuo python node whose contract failed blue/green validation.

` + pythonContractDescription + ` Validation runs the node's script against the candidate schema and checks that what it produced matches what the contract declares. It does NOT run the script's python source, which you are not shown and cannot change.

Rules:
- Change ONLY what validation checks: the node's declared reads (its readQueries, including their SQL), the columns in schema[0].properties, and its config. Never touch its id, the schema object's name, its script, kind, team, schedule, criticality, or secretRef — those identify the node (or name the credentials it runs with), and changing one makes it a different node rather than a fixed one.
- Keep every read the node declares. Correcting a query's SQL is a fix, and adding a read is allowed, but deleting a read or renaming its key in readQueries is not: the script still performs that read, and you cannot change the script. A contract that no longer declares it passes validation while the node stays broken.
- Each contract file declares exactly one node. Leave the contract of every node other than the failing one unchanged, and never declare the failing node in a second file.
- Return the COMPLETE new content of every file you change, never a diff and never a fragment. A file you do not change must not appear in your answer at all.
- Fix the contract to match what the node genuinely produces; do not weaken a declared column type or drop a column merely to make the check pass, unless the evidence shows the declaration itself is what is wrong.
- When earlier attempts are shown, read what each one changed and why its verification failed, and do not repeat a change that has already been rejected.
- When past precedents are shown, weigh how the same error was resolved before; follow a precedent's approach only where it fits the contract you are shown.
- Always respond by calling the propose_python_fix tool.`

const pythonParseFixSystemPrompt = `You are a data-engineering assistant that fixes a Continuo python node one of whose declared reads has SQL that the release's SQL parser rejected before any run.

` + pythonContractDescription + ` Before a release runs anything, every query's SQL is parsed and every relation it names must be schema-qualified so the release can resolve it to an upstream node. The parser rejected one of this node's reads, and its error names the line and column where parsing stopped. The script's python source is not parsed, is not shown to you, and cannot be changed.

Rules:
- Change ONLY the SQL of the node's readQueries, so that every query parses and every relation it references is schema-qualified. Never touch the node's id, the schema object's name, its script, kind, team, schedule, criticality, secretRef, or the columns in schema[0].properties — those identify the node (or name the credentials it runs with) or are checked later by validation, not by the parser.
- Keep every read the node declares. Correcting a query's SQL is the fix; deleting a read or renaming its key in readQueries is not: the script still performs that read, and you cannot change the script. A contract that no longer declares it parses while the node stays broken.
- Qualify a relation with the schema the evidence shows it lives in; do not invent a schema, and do not rewrite what the query selects beyond what parsing requires.
- Each contract file declares exactly one node. Leave the contract of every node other than the failing one unchanged, and never declare the failing node in a second file.
- Return the COMPLETE new content of every file you change, never a diff and never a fragment. A file you do not change must not appear in your answer at all.
- When earlier attempts are shown, read what each one changed and why its verification failed, and do not repeat a change that has already been rejected.
- When past precedents are shown, weigh how the same error was resolved before; follow a precedent's approach only where it fits the contract you are shown.
- Always respond by calling the propose_python_fix tool.`

// pythonNoReadsContractFixSystemPrompt is the validation prompt for a node that
// declares no reads. It is built once at package initialisation, so a drifted
// base prompt panics at startup rather than while handling a message.
var pythonNoReadsContractFixSystemPrompt = noReadsSystemPrompt(pythonContractFixSystemPrompt)

// AssemblePythonContractFix builds the request that asks the model to correct
// the ODCS contract declaring a python node that failed validation. The answer
// is a list of complete files rather than one file's content, the shape every
// python contract fix shares.
func AssemblePythonContractFix(ev PythonEvidence) ProposeRequest {
	system := pythonContractFixSystemPrompt
	if ev.NoReads {
		system = pythonNoReadsContractFixSystemPrompt
	}
	return pythonContractRequest(system, "Validation error", ev)
}

// AssemblePythonParseFix builds the request that asks the model to correct a
// read's SQL in the ODCS contract declaring a python node the release's SQL
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
	renderCurrentContract(&u, ev.CurrentContract)

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

// renderCurrentContract writes the node's current contract under the "Current
// contract" heading. No contract → no section.
func renderCurrentContract(b *strings.Builder, current string) {
	if current == "" {
		return
	}
	fmt.Fprintf(b, "Current contract (the node's contract as continuo last promoted it):\n```yaml\n%s\n```\n\n", current)
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

// noReadsSystemPrompt rewrites the python validation system prompt for a node
// that declares no reads: the clause describing the reads it performs goes, the
// description says the script fetches its own data, the edit scope shrinks to
// the columns and config, and the keep-every-read rule becomes a rule against
// adding one. Each substitution must find its text, so a wording change
// in the base prompt fails loudly (a panic at package initialisation)
// instead of silently leaving the reads wording behind.
func noReadsSystemPrompt(system string) string {
	const (
		readsClause = "the script that produces it and, under readQueries, the upstream relations it reads: each query is one SELECT that names every table it reads with its schema."
		noReadsDesc = "the script that produces it. This node declares no reads (it has no readQueries): its script fetches its own data (for example from an HTTP API)."
		scopeBefore = "the node's declared reads (its readQueries, including their SQL), the columns in schema[0].properties, and its config."
		scopeAfter  = "the columns in schema[0].properties and its config."
	)
	system = mustReplace(system, readsClause, noReadsDesc)
	system = mustReplace(system, scopeBefore, scopeAfter)

	lines := strings.Split(system, "\n")
	for i, l := range lines {
		if strings.HasPrefix(l, "- Keep every read the node declares.") {
			lines[i] = "- Never add a read: this node declares none, and its script fetches its own data. Only the columns in schema[0].properties and config may change."
			return strings.Join(lines, "\n")
		}
	}
	panic("prompt: python system prompt has no keep-every-read rule to rewrite")
}

// mustReplace substitutes old with new exactly once and panics when old is
// absent, so a drifted base prompt cannot turn the rewrite into a no-op.
func mustReplace(s, old, replacement string) string {
	if !strings.Contains(s, old) {
		panic("prompt: python system prompt no longer contains " + old)
	}
	return strings.Replace(s, old, replacement, 1)
}
