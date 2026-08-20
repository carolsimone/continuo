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
	loopFixtureFile = "../fixtures/py-remediation-repo/services/svc-py-e2e-loop/contracts/py_loop_read.yml"
	loopFixturePath = "services/svc-py-e2e-loop/contracts/py_loop_read.yml"
)

// priorAttemptSection is the earlier-attempts section a retry's prompt carries,
// rendered exactly as prompt.AssemblePythonContractFix renders it: the heading,
// the attempt line with the error its shadow release reported, and the diff that
// attempt applied.
const priorAttemptSection = "Previous fix attempts for this node, oldest first — do not repeat a change that was already rejected:\n" +
	"Attempt 1 — verification failed: relation \"public.still_wrong_name\" does not exist\n" +
	"  Changed " + loopFixturePath + ":\n" +
	"```diff\n" +
	"-      missing: select id from public.loop_wrong_name\n" +
	"+      missing: select id from public.still_wrong_name\n" +
	"```\n\n"

// pythonFixPrompt renders the user message the python contract fixer sends,
// in the same order and with the same markers prompt.AssemblePythonContractFix
// writes: the failing node, the validation error, the declaring contract file
// verbatim inside a ```yaml fence, then the optional earlier-attempts section.
//
// It is written out here rather than imported because stub-llm is its own Go
// module with no dependencies — the container image it builds carries nothing
// but the standard library.
func pythonFixPrompt(contract, priorAttempts string) string {
	var b strings.Builder
	b.WriteString("Failed python node: e2e_schema.py_loop_read\n\n")
	b.WriteString("Validation error:\n```\nrelation \"public.loop_wrong_name\" does not exist\n```\n\n")
	b.WriteString("Contract file " + loopFixturePath + " that declares it:\n```yaml\n" + contract + "\n```\n\n")
	b.WriteString(priorAttempts)
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
// shadow release verifying it is rejected and its error becomes the second
// attempt's evidence.
//
// It reads the PRISTINE fixture from disk rather than an inline copy, because
// the way this has broken before is a fixture drifting away from what the stub
// assumes about it: a comment added to the file named the still-broken relation,
// the stub took that as proof of an earlier attempt, and the very first attempt
// got the answer meant for the retry. The whole loop then validated on its first
// shadow release and the e2e test timed out waiting for a rejection it could no
// longer get. Reading the real file is what makes this test notice.
func TestProposePythonFix_LoopFixture_FirstAttemptStaysBroken(t *testing.T) {
	path, content := proposedFile(t, pythonFixPrompt(readLoopFixture(t), ""))

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
	contract := strings.Replace(readLoopFixture(t), "nodes:\n", "nodes:\n"+
		"  # the first answer points at "+stillBrokenRead+", the retry at "+bindingRead+"\n"+
		"  # "+priorAttemptsHeading+"\n", 1)

	_, content := proposedFile(t, pythonFixPrompt(contract, ""))

	if !strings.Contains(content, "select id from "+stillBrokenRead) {
		t.Errorf("a comment in the contract file must not turn a first attempt into a retry; got:\n%s", content)
	}
}

// TestProposePythonFix_LoopFixture_RetryShownTheErrorBinds is the other half:
// once the prompt carries what the rejected attempt changed and why it failed,
// the stub answers with the relation the e2e test creates in the warehouse, so
// the second shadow release validates.
func TestProposePythonFix_LoopFixture_RetryShownTheErrorBinds(t *testing.T) {
	_, content := proposedFile(t, pythonFixPrompt(readLoopFixture(t), priorAttemptSection))

	if !strings.Contains(content, "select id from "+bindingRead) {
		t.Errorf("a retry shown the rejected attempt must declare the binding read; got:\n%s", content)
	}
	if strings.Contains(content, stillBrokenRead) {
		t.Errorf("a retry must not repeat the read that was already rejected; got:\n%s", content)
	}
}

// TestProposePythonFix_RetryWithoutTheRejectedEvidenceKeepsFailing proves the
// loop test measures what it claims to. An attempt row alone — the heading with
// no error and no diff behind it — is not evidence, and must not earn the
// answer that passes: if the rejected attempt's own error stops reaching the
// prompt, the retry keeps getting the fix that already failed.
func TestProposePythonFix_RetryWithoutTheRejectedEvidenceKeepsFailing(t *testing.T) {
	const emptyAttempt = "Previous fix attempts for this node, oldest first — do not repeat a change that was already rejected:\n" +
		"Attempt 1\n\n"

	_, content := proposedFile(t, pythonFixPrompt(readLoopFixture(t), emptyAttempt))

	if !strings.Contains(content, "select id from "+stillBrokenRead) {
		t.Errorf("a retry shown no evidence must keep getting the answer that already failed; got:\n%s", content)
	}
}

// TestProposePythonFix_BadReadFixtureBindsImmediately covers the other e2e
// scenario, whose node is repaired in a single attempt: the fixture declaring
// badReadBrokenRead gets the binding relation straight away.
func TestProposePythonFix_BadReadFixtureBindsImmediately(t *testing.T) {
	raw, err := os.ReadFile("../fixtures/py-remediation-repo/services/svc-py-e2e/contracts/py_bad_read.yml")
	if err != nil {
		t.Fatalf("read the single-attempt contract fixture: %v", err)
	}

	_, content := proposedFile(t, pythonFixPrompt(string(raw), ""))

	if !strings.Contains(content, "select id from "+bindingRead) {
		t.Errorf("the single-attempt fixture must be repaired on its first attempt; got:\n%s", content)
	}
}

// TestProposePythonFix_NoContractFileYieldsNoFiles pins the degenerate case the
// fixer turns into a legible failed attempt rather than a malformed write: a
// prompt carrying no contract file gets an answer carrying no files.
func TestProposePythonFix_NoContractFileYieldsNoFiles(t *testing.T) {
	rec := httptest.NewRecorder()
	writeProposePythonFixResponse(rec, "Failed python node: e2e_schema.py_loop_read\n")

	if !strings.Contains(rec.Body.String(), `\"updated_files\":[]`) &&
		!strings.Contains(rec.Body.String(), `\"updated_files\":null`) {
		t.Errorf("a prompt with no contract file must yield no updated files; got %s", rec.Body.String())
	}
}
