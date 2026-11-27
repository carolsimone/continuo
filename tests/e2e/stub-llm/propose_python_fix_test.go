package main

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// The loop scenario's contract fixture, as this test's working directory sees
// it and as the repository path the prompt names it by.
const (
	loopFixtureFile = "../fixtures/py-remediation-repo/services/svc-py-e2e-loop/contracts/py_loop_read.odcs.yaml"
	loopFixturePath = "services/svc-py-e2e-loop/contracts/py_loop_read.odcs.yaml"
)

// pythonFixPrompt renders the user message the python-contract prompt step
// sends, in the same order and with the same markers
// prompt.AssemblePythonContractFix writes: the failing node, the validation
// error, the declaring contract file verbatim inside a ```yaml fence, then the
// closing instruction.
//
// It is written out here rather than imported because stub-llm is its own Go
// module with no dependencies — the container image it builds carries nothing
// but the standard library.
func pythonFixPrompt(contract string) string {
	var b strings.Builder
	b.WriteString("Failed python node: e2e_schema.py_loop_read\n\n")
	b.WriteString("Validation error:\n```\nrelation \"public.loop_wrong_name\" does not exist\n```\n\n")
	b.WriteString("Contract file " + loopFixturePath + " that declares it:\n```yaml\n" + contract + "\n```\n\n")
	b.WriteString("Return the complete new content of every file you change.")
	return b.String()
}

// proposedFile runs one request through the handler and returns the single file
// its propose_python_fix answer carries.
func proposedFile(t *testing.T, userContent string) (path, content string) {
	t.Helper()
	rec := httptest.NewRecorder()
	writeProposePythonFixResponse(rec, userContent)

	var resp struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode completion: %v", err)
	}
	if len(resp.Choices) != 1 || len(resp.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("want exactly one tool call, got %s", rec.Body.String())
	}
	call := resp.Choices[0].Message.ToolCalls[0]
	if call.Function.Name != "propose_python_fix" {
		t.Fatalf("want the propose_python_fix tool, got %q", call.Function.Name)
	}

	var args struct {
		UpdatedFiles []struct {
			Path    string `json:"path"`
			Content string `json:"content"`
		} `json:"updated_files"`
	}
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil {
		t.Fatalf("decode tool arguments: %v", err)
	}
	if len(args.UpdatedFiles) != 1 {
		t.Fatalf("want exactly one updated file, got %d", len(args.UpdatedFiles))
	}
	return args.UpdatedFiles[0].Path, args.UpdatedFiles[0].Content
}

// readLoopFixture returns the loop scenario's contract file exactly as the
// repository holds it — the same bytes the prompt renders verbatim.
func readLoopFixture(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(loopFixtureFile)
	if err != nil {
		t.Fatalf("read the loop contract fixture: %v", err)
	}
	return string(raw)
}

// TestProposePythonFix_LoopFixture_FirstAttemptStaysBroken is the guard on the
// property the loop e2e test rests on: shown the loop fixture with no earlier
// attempt, the stub must answer with a read that still cannot bind, so the
// release check checking it is rejected and its error becomes the second
// attempt's evidence.
//
// It reads the PRISTINE fixture from disk rather than an inline copy, because
// the way this has broken before is a fixture drifting away from what the stub
// assumes about it: a comment added to the file named the still-broken relation,
// the stub took that as proof of an earlier attempt, and the very first attempt
// got the answer meant for the retry. The whole loop then validated on its
// first release check and the e2e test timed out waiting for a rejection
// it could no longer get. Reading the real file is what makes this test notice.
func TestProposePythonFix_LoopFixture_FirstAttemptStaysBroken(t *testing.T) {
	path, content := proposedFile(t, pythonFixPrompt(readLoopFixture(t)))

	if path != loopFixturePath {
		t.Errorf("want the contract file the prompt showed (%s), got %s", loopFixturePath, path)
	}
	if !strings.Contains(content, "select id from "+stillBrokenRead) {
		t.Errorf("a first attempt must declare the read that still cannot bind; got:\n%s", content)
	}
	if strings.Contains(content, bindingRead) {
		t.Errorf("a first attempt must not declare the binding read — that is the retry's answer; got:\n%s", content)
	}
}

// TestProposePythonFix_LoopFixture_CommentsCannotAnswerForTheModel pins the
// rule directly: text inside the contract file decides nothing. A fixture whose
// comments name every relation the stub knows about must still get a first
// attempt's answer.
func TestProposePythonFix_LoopFixture_CommentsCannotAnswerForTheModel(t *testing.T) {
	contract := strings.Replace(readLoopFixture(t), "customProperties:\n", "customProperties:\n"+
		"  # the first answer points at "+stillBrokenRead+", the retry at "+bindingRead+"\n"+
		"  # "+earlierTriesHeading+"\n", 1)

	_, content := proposedFile(t, pythonFixPrompt(contract))

	if !strings.Contains(content, "select id from "+stillBrokenRead) {
		t.Errorf("a comment in the contract file must not turn a first attempt into a retry; got:\n%s", content)
	}
}

// earlierTriesSection is the section a try after the first appends to its
// request, rendered as prompt.WithPreviousTries renders it: the heading,
// its instruction, then one part per earlier try with the diff that try
// applied and why the gate failed it.
const earlierTriesSection = "\n\n## Earlier tries at this failure\n" +
	"These tries at the same failure did not pass. Do not repeat a change that did not pass; correct what each try was refused for.\n" +
	"\n### Round 1, try 1\n" +
	"Changed " + loopFixturePath + ":\n" +
	"```diff\n" +
	"-      missing: select id from public.loop_wrong_name\n" +
	"+      missing: select id from public.still_wrong_name\n" +
	"```\n" +
	"Why the gate failed: e2e_schema.py_loop_read: relation \"public.still_wrong_name\" does not exist"

// pythonFixPromptAfter is the request a later try or a later pre-flight round
// sends for the loop fixture: the contract-fix request with the given section
// (the earlier tries, or the previous answer with the checks' findings)
// appended after it.
func pythonFixPromptAfter(contract, section string) string {
	return pythonFixPrompt(contract) + section
}

// TestProposePythonFix_LoopFixture_RetryShownTheEarlierTryBinds is the other
// half of the first-attempt test: a retry carries the earlier tries under their
// heading, with the read the first try declared in that try's diff and gate
// error, so the stub answers with the relation the e2e test creates in the
// warehouse and the second release check validates.
func TestProposePythonFix_LoopFixture_RetryShownTheEarlierTryBinds(t *testing.T) {
	_, content := proposedFile(t, pythonFixPromptAfter(readLoopFixture(t), earlierTriesSection))

	if !strings.Contains(content, "select id from "+bindingRead) {
		t.Errorf("a retry shown the earlier try must declare the binding read; got:\n%s", content)
	}
	if strings.Contains(content, stillBrokenRead) {
		t.Errorf("a retry must not repeat the read the earlier try failed for; got:\n%s", content)
	}
}

// TestProposePythonFix_LoopFixture_RetryWithoutTheEvidenceKeepsFailing proves
// the loop test measures what it claims to: the earlier-tries heading alone,
// with no earlier try's diff or gate error behind it, earns no binding read, so
// if the rejected try's own error stops reaching the prompt the retry keeps
// getting the fix that already failed.
func TestProposePythonFix_LoopFixture_RetryWithoutTheEvidenceKeepsFailing(t *testing.T) {
	const emptyTries = "\n\n## Earlier tries at this failure\n" +
		"These tries at the same failure did not pass. Do not repeat a change that did not pass; correct what each try was refused for.\n" +
		"\n### Round 1, try 1\nNothing was recorded for this try."

	_, content := proposedFile(t, pythonFixPromptAfter(readLoopFixture(t), emptyTries))

	if !strings.Contains(content, "select id from "+stillBrokenRead) {
		t.Errorf("a retry shown no evidence must keep getting the answer that already failed; got:\n%s", content)
	}
}

// TestProposePythonFix_LoopFixture_RevisionInTheFirstTryStaysBroken: a later
// pre-flight round of the first try carries the model's previous answer, which
// names the still-broken read, but no earlier try, so it is no retry.
func TestProposePythonFix_LoopFixture_RevisionInTheFirstTryStaysBroken(t *testing.T) {
	revision := "\n\n## Your previous answer\n```json\n" +
		`{"updated_files":[{"path":"` + loopFixturePath + `","content":"select id from public.still_wrong_name"}]}` +
		"\n```\n\n## What the checks found\n- reads_kept: a read was dropped\n" +
		"\nCorrect every finding and call the tool again with your complete answer."

	_, content := proposedFile(t, pythonFixPromptAfter(readLoopFixture(t), revision))

	if !strings.Contains(content, "select id from "+stillBrokenRead) {
		t.Errorf("a revision within the first try is not a retry; got:\n%s", content)
	}
}

// TestProposePythonFix_BadReadFixtureBindsImmediately covers the other e2e
// scenario, whose node is repaired in a single attempt: the fixture declaring
// badReadBrokenRead gets the binding relation straight away.
func TestProposePythonFix_BadReadFixtureBindsImmediately(t *testing.T) {
	raw, err := os.ReadFile("../fixtures/py-remediation-repo/services/svc-py-e2e/contracts/py_bad_read.odcs.yaml")
	if err != nil {
		t.Fatalf("read the single-attempt contract fixture: %v", err)
	}

	_, content := proposedFile(t, pythonFixPrompt(string(raw)))

	if !strings.Contains(content, "select id from "+bindingRead) {
		t.Errorf("the single-attempt fixture must be repaired on its first attempt; got:\n%s", content)
	}
}

// TestProposePythonFix_NoContractFileYieldsNoFiles pins the degenerate case the
// runner turns into a legible failed attempt rather than a malformed write: a
// prompt carrying no contract file gets an answer carrying no files.
func TestProposePythonFix_NoContractFileYieldsNoFiles(t *testing.T) {
	rec := httptest.NewRecorder()
	writeProposePythonFixResponse(rec, "Failed python node: e2e_schema.py_loop_read\n")

	if !strings.Contains(rec.Body.String(), `\"updated_files\":[]`) &&
		!strings.Contains(rec.Body.String(), `\"updated_files\":null`) {
		t.Errorf("a prompt with no contract file must yield no updated files; got %s", rec.Body.String())
	}
}

// pythonParseFixPrompt renders the shape prompt.AssemblePythonParseFix produces
// for the parse fixture: the parser's error under the "SQL parse error:"
// heading in place of a validation error, then the declaring contract.
func pythonParseFixPrompt(contract string) string {
	var b strings.Builder
	b.WriteString("Failed python node: e2e_schema.py_unqualified_read\n\n")
	b.WriteString("SQL parse error:\n```\nunqualified table reference `right_name` in node `py_unqualified_read`\n```\n\n")
	b.WriteString("Contract file services/svc-py-e2e-parse/contracts/py_unqualified_read.odcs.yaml that declares it:\n```yaml\n" + contract + "\n```\n\n")
	b.WriteString("Return the complete new content of every file you change.")
	return b.String()
}

// TestProposePythonFix_ParseFixtureQualifiesTheRead pins the parse repair: a
// prompt carrying the parser's error gets the fixture's read back with its
// relation schema-qualified, and nothing else about the contract changed.
func TestProposePythonFix_ParseFixtureQualifiesTheRead(t *testing.T) {
	raw, err := os.ReadFile("../fixtures/py-remediation-repo/services/svc-py-e2e-parse/contracts/py_unqualified_read.odcs.yaml")
	if err != nil {
		t.Fatalf("read the parse contract fixture: %v", err)
	}

	path, content := proposedFile(t, pythonParseFixPrompt(string(raw)))

	if path != "services/svc-py-e2e-parse/contracts/py_unqualified_read.odcs.yaml" {
		t.Errorf("the answer must name the declaring contract file; got %q", path)
	}
	if !strings.Contains(content, "select id from "+bindingRead) {
		t.Errorf("the parse fixture's read must come back schema-qualified; got:\n%s", content)
	}
	if strings.Contains(content, "select id from "+unqualifiedRead+"\n") {
		t.Errorf("the unqualified read must not survive the repair; got:\n%s", content)
	}
	if !strings.Contains(content, "id: e2e_schema.py_unqualified_read") {
		t.Errorf("the repair must keep declaring the same node; got:\n%s", content)
	}
}
