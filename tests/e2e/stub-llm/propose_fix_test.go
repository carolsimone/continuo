package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// singleFileToolParams is the parameter set the compile AND the parse lane
// declare on propose_fix: prompt.AssembleParseFix builds its request from
// AssembleCompileFix and swaps only the system prompt, the user body and the
// tool description, so the declared parameters cannot tell the two apart. The
// prompt body is the only discriminator, which is what the routing below and
// this test are about.
var singleFileToolParams = map[string]bool{
	"target_file":               true,
	"proposed_content":          true,
	"rationale":                 true,
	"confidence":                true,
	"suspected_root_cause_node": true,
}

// shownFile is the offending file both prompts below render first, and so the
// file firstShownFile must pick as target_file.
const shownFile = "services/service-2/models/ftable_e.sql"

// singleFilePrompt renders the user message for a compile or a parse fix in the
// order prompt.AssembleCompileFix / prompt.AssembleParseFix write it: a header,
// each shown file inside a fence, then the error under its own heading —
// "dbt compile error:" for a compile fix, "SQL parse error:" for a parse fix.
//
// It is written out here rather than imported because stub-llm is its own Go
// module with no dependencies — the container image it builds carries nothing
// but the standard library.
func singleFilePrompt(header, errorHeading, errorText string) string {
	var b strings.Builder
	b.WriteString(header + "\n\n")
	b.WriteString("File " + shownFile + ":\n```\nselect a b, c from t\n```\n\n")
	b.WriteString("File services/service-2/dbt_project.yml:\n```\nname: service_2\n```\n\n")
	b.WriteString(errorHeading + "\n```\n" + errorText + "\n```\n\n")
	b.WriteString("Return the complete corrected content of the ONE file that must change.")
	return b.String()
}

// proposeFixArgs runs one request through the handler and returns the arguments
// of the single propose_fix tool call its answer carries.
func proposeFixArgs(t *testing.T, userContent string, params map[string]bool) map[string]string {
	t.Helper()
	rec := httptest.NewRecorder()
	writeProposeFixResponse(rec, userContent, params)

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
	if call.Function.Name != "propose_fix" {
		t.Fatalf("want the propose_fix tool, got %q", call.Function.Name)
	}
	var args map[string]string
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil {
		t.Fatalf("decode tool arguments: %v", err)
	}
	return args
}

// TestProposeFix_SingleFileLaneRoutesOnThePromptHeading is the guard on the
// property the parse e2e test rests on: a parse fix and a compile fix declare
// the same tool parameters, so only the heading their prompt renders separates
// them. Answering a parse prompt with the compile fixture would lay a model
// body carrying its own schedule tag over the node under repair, and answering
// a compile prompt with the parse fixture would replace the malformed-Jinja
// fixture with a model that never exercised the Jinja error at all.
func TestProposeFix_SingleFileLaneRoutesOnThePromptHeading(t *testing.T) {
	for name, tc := range map[string]struct {
		userContent string
		wantContent string
	}{
		"parse prompt": {
			userContent: singleFilePrompt("Service: service-2\nNode: e2e_schema.ftable_e",
				parseFixMarker, "Required keyword: 'this' missing for Where. Line 1, Col: 44."),
			wantContent: parseFixContent,
		},
		"compile prompt": {
			userContent: singleFilePrompt("Service: service-2",
				"dbt compile error:", "Compilation Error in model daily_transactions"),
			wantContent: compileFixContent,
		},
	} {
		args := proposeFixArgs(t, tc.userContent, singleFileToolParams)
		if got := args["target_file"]; got != shownFile {
			t.Errorf("%s: want the first shown file (%s) as target_file, got %q", name, shownFile, got)
		}
		if got := args["proposed_content"]; got != tc.wantContent {
			t.Errorf("%s: wrong proposed_content:\ngot:\n%s\nwant:\n%s", name, got, tc.wantContent)
		}
		if args["confidence"] != "high" {
			t.Errorf("%s: a low-confidence answer is never turned into a proposal; got %q", name, args["confidence"])
		}
		if args["rationale"] == "" {
			t.Errorf("%s: the tool requires a rationale", name)
		}
	}
}

// TestProposeFix_ParseMarkerDoesNotDivertTheSeedOrValidationLanes pins that the
// new routing is scoped to the single-file lane: the seed and validation lanes
// declare different parameters, and a prompt of theirs that happened to carry
// the parse heading must still get its own answer.
func TestProposeFix_ParseMarkerDoesNotDivertTheSeedOrValidationLanes(t *testing.T) {
	withMarker := singleFilePrompt("Seed: seeds/customers.csv", parseFixMarker, "boom")

	seed := proposeFixArgs(t, withMarker, map[string]bool{"proposed_content": true, "rationale": true, "confidence": true})
	if seed["proposed_content"] != seedFixContent {
		t.Errorf("the seed lane must still answer with the corrected CSV, got %q", seed["proposed_content"])
	}
	if _, ok := seed["target_file"]; ok {
		t.Error("the seed lane's tool declares no target_file, so the answer must not carry one")
	}

	validation := proposeFixArgs(t, withMarker, map[string]bool{"proposed_sql": true, "rationale": true, "confidence": true})
	if validation["proposed_sql"] == "" {
		t.Error("the validation lane must still answer with proposed_sql")
	}
	if _, ok := validation["proposed_content"]; ok {
		t.Error("the validation lane's tool declares no proposed_content, so the answer must not carry one")
	}
}
