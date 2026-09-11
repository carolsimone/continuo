package fixer

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

func parseInput() Input {
	return Input{Source: "parse", NodeID: "analytics.fx", Service: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/fx.sql", ErrorExcerpt: "Expecting ). Line 3, Col: 12.\n  select a b",
		ReleaseID: "r", Attempt: 1}
}

func TestParse_ResolvesRepoPrefixFromService_NotNodeID(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/fx.sql": "select a b, c from t"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/fx.sql"}},
	}
	llm := &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/fx.sql", ProposedContent: "select a, b, c from t", Confidence: "high", Rationale: "added the missing comma"}}}
	svc := Services{
		Source: fs, LLM: llm, Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	r, err := parseFixer{}.Propose(context.Background(), svc, parseInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusProposed, r.Proposal.Status)
	require.Equal(t, "services/svc/models/fx.sql", r.Proposal.FilePath)
	require.Len(t, r.Proposal.Edits, 1)
}

func TestParse_PromptCarriesTheParserDetailNotADbtLog(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/fx.sql": "select a b, c from t"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/fx.sql"}},
	}
	llm := &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/fx.sql", ProposedContent: "select a, b, c from t", Confidence: "high"}}}
	svc := Services{
		Source: fs, LLM: llm, Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	_, err := parseFixer{}.Propose(context.Background(), svc, parseInput())
	require.NoError(t, err)
	require.Len(t, llm.requests, 1)
	user := llm.requests[0].User
	require.True(t, strings.Contains(user, "SQL parse error:"), user)
	require.True(t, strings.Contains(user, "Expecting ). Line 3, Col: 12."), user)
	require.True(t, strings.Contains(user, "Node: analytics.fx"), user)
	require.False(t, strings.Contains(user, "dbt compile error"), user)
}

func TestParse_SkipsWithoutFilePathOrService(t *testing.T) {
	svc := Services{Source: &fakeSourceMap{}, Logger: testLogger(), ServiceRepoPaths: map[string]string{"svc": "services/svc"}}
	for name, in := range map[string]Input{
		"no file path": func() Input { i := parseInput(); i.FilePath = ""; return i }(),
		"no service":   func() Input { i := parseInput(); i.Service = ""; return i }(),
		"unmapped":     func() Input { i := parseInput(); i.Service = "other"; return i }(),
	} {
		r, err := parseFixer{}.Propose(context.Background(), svc, in)
		require.NoError(t, err, name)
		require.Equal(t, proposal.StatusSkipped, r.Proposal.Status, name)
	}
}

// TestParse_PythonTarget_NeverReadsOrCallsLLM proves the parse lane refuses a
// python node before any read. topology-controller reports a python node's
// parse failure against the script named by the node's contract entry, but the
// SQL the parser rejected is one of that entry's `reads`, which lives in the
// service's contract.yaml — a file this system carries no repository path for.
// Editing the script would therefore leave the rejected SQL untouched, and the
// proposal would package no VerificationContract, so the driver would submit a
// dbt verification run for a python service.
//
// This is a non-negotiable project invariant: no remediation path may ever
// produce an LLM call or a proposal for a python node, so the source, LLM and
// precedent fakes must all show zero calls, not just the terminal status.
func TestParse_PythonTarget_NeverReadsOrCallsLLM(t *testing.T) {
	for _, nodeType := range []string{"python-model", "python-csv"} {
		t.Run(nodeType, func(t *testing.T) {
			fs := &fakeSourceMap{files: map[string]string{"services/svc/scripts/fx.py": "print('hello')"}}
			llm := &fakeLLM{}
			precedents := &fakePrecedents{}
			svc := Services{
				Source: fs, LLM: llm, Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
				Artifacts: &fakeArtifacts{}, Logger: testLogger(),
				ServiceRepoPaths: map[string]string{"svc": "services/svc"},
				Precedents:       precedents,
			}
			in := parseInput()
			in.NodeType = nodeType
			in.FilePath = "scripts/fx.py"

			r, err := parseFixer{}.Propose(context.Background(), svc, in)
			require.NoError(t, err)
			require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
			require.Nil(t, r.VerificationContract)
			require.Empty(t, fs.readPaths(), "want no source read for a python target")
			require.Zero(t, llm.calls, "want no LLM call for a python target")
			require.Zero(t, precedents.calls, "want no precedent lookup for a python target")
		})
	}
}

// TestParse_DbtModelTarget_StillGathers pins that the python refusal is scoped
// to the python node kinds: a dbt model carrying an explicit node_type still
// reads its source and reaches the model.
func TestParse_DbtModelTarget_StillGathers(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/fx.sql": "select a b, c from t"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/fx.sql"}},
	}
	llm := &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/fx.sql", ProposedContent: "select a, b, c from t", Confidence: "high", Rationale: "added the missing comma"}}}
	svc := Services{
		Source: fs, LLM: llm, Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := parseInput()
	in.NodeType = "dbt-model"

	r, err := parseFixer{}.Propose(context.Background(), svc, in)
	require.NoError(t, err)
	require.Equal(t, proposal.StatusProposed, r.Proposal.Status)
	require.Contains(t, fs.readPaths(), "services/svc/models/fx.sql")
	require.Len(t, llm.requests, 1)
}
