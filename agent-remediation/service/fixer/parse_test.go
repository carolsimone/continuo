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
