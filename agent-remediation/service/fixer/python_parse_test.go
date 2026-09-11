package fixer

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/stretchr/testify/require"
)

// parseRejectedYAML declares the failing node with a read whose SQL names its
// relation without a schema — the shape topology-controller's parser rejects
// as unqualified_reference before any Job runs.
const parseRejectedYAML = `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    reads:
      orders: select id from orders
    output_columns:
      - name: revenue
`

// parseCorrectedYAML is the same declaration with the read qualified.
const parseCorrectedYAML = `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    reads:
      orders: select id from analytics.orders
    output_columns:
      - name: revenue
`

// pythonParseInput is the trigger a python-model node's parse rejection
// produces: the parser's own text as the excerpt, the script as the file
// path, and no log or bundle — the parse leg precedes both.
func pythonParseInput() Input {
	return Input{
		Source: "parse", ReleaseID: "rel-1", NodeID: "analytics.py_daily_kpis",
		NodeType: "python-model", Service: "svc-py", Repo: "o/demo", CommitSHA: "deadbeef",
		FilePath:       "scripts/py_daily_kpis.py",
		ErrorExcerpt:   "unqualified table reference `orders` in read `orders`. Line 1, Col: 16.",
		ErrorSignature: "sig-parse-1", Attempt: 1,
	}
}

// TestFor_ParseDispatchesOnNodeType pins the parse lane's routing: a
// python-model node's rejected SQL lives in its contract yaml, so it takes
// the contract-fix lane; every other node kind, python-csv included, keeps
// the source-file lane.
func TestFor_ParseDispatchesOnNodeType(t *testing.T) {
	py, err := For("parse", "python-model")
	require.NoError(t, err)
	require.IsType(t, pythonParseFixer{}, py)

	for _, nodeType := range []string{"dbt-model", "dbt-seed", "dbt-snapshot", "python-csv", ""} {
		f, ferr := For("parse", nodeType)
		require.NoError(t, ferr)
		require.IsType(t, parseFixer{}, f, "node type %q must keep the source-file parse fixer", nodeType)
	}
}

// TestPythonParse_HappyPath walks the lane end to end: the checkout is
// fetched, the declaring contract file located from the node id alone (the
// trigger's file path names the script, which the parser never reads), the
// one model call sees the parser's error, the corrected yaml is written before
// packaging, and the merged contract comes back as VerificationContract so
// the driver submits a python verification run — whose parse leg is what
// proves the fix.
func TestPythonParse_HappyPath(t *testing.T) {
	root := t.TempDir()
	writeRepoFile(t, root, "services/service-py/contracts/py_daily_kpis.yml", parseRejectedYAML)
	writeRepoFile(t, root, "services/service-py/contracts/other.yml", siblingYAML)
	writeRepoFile(t, root, "services/service-py/scripts/py_daily_kpis.py", "print('hi')\n")

	svc, arch, pkgr, rel, arts := pythonSvc(t, root)
	llm := &fakeLLM{queue: []ports.ProposeResult{{
		Files: []ports.ProposedFile{
			{Path: "services/service-py/contracts/py_daily_kpis.yml", Content: parseCorrectedYAML},
		},
		Rationale: "qualified the orders read with its schema", Confidence: "high", Model: "test-model",
	}}}
	svc.LLM = llm
	// A parse rejection precedes every Job and the code bundle: the trigger
	// carries no log URI and no bundle URI, and the lane must consult neither
	// reader — a bundle miss it went looking for would be logged as one.
	evidence := &countingEvidence{}
	bundle := &fakeCandidateSource{err: ports.ErrNotFound}
	svc.Evidence = evidence
	svc.CandidateSource = bundle

	r, err := pythonParseFixer{}.Propose(context.Background(), svc, pythonParseInput())
	require.NoError(t, err)
	require.Zero(t, evidence.calls, "no log exists for a parse rejection, so none may be fetched")
	require.Zero(t, bundle.calls, "no code bundle exists for a parse rejection, so none may be read")

	require.Equal(t, 1, arch.calls)
	require.Equal(t, "o/demo", arch.gotRepo)
	require.Equal(t, "deadbeef", arch.gotCommit)
	require.Equal(t, 1, arch.cleanups)

	// The model saw the parser's error and the declaring yaml, and was asked
	// for a parse fix rather than a validation fix.
	require.Equal(t, 1, llm.calls)
	require.Contains(t, llm.lastRequest.User, "unqualified table reference `orders`")
	require.Contains(t, llm.lastRequest.User, parseRejectedYAML)
	require.Contains(t, llm.lastRequest.System, "parser rejected")
	require.NotContains(t, llm.lastRequest.User, "Full runner log")

	// Packaging ran once over the patched contract directory.
	require.Len(t, pkgr.calls, 1)
	call := pkgr.calls[0]
	require.Equal(t, filepath.Join(root, "services", "service-py", "contracts"), call.contractDir)
	require.Equal(t, parseCorrectedYAML, call.sawOnDisk["py_daily_kpis.yml"])
	require.Equal(t, siblingYAML, call.sawOnDisk["other.yml"])
	require.Equal(t, []byte("merged: contract\n"), r.VerificationContract)

	// The sibling check read the original rejected release.
	require.Equal(t, []string{"rel-1"}, rel.failingNodesCalls)

	p := r.Proposal
	require.Equal(t, proposal.StatusProposed, p.Status)
	require.True(t, p.SourceResolved)
	require.Equal(t, "qualified the orders read with its schema", p.Rationale)
	require.Len(t, p.Edits, 1)
	require.Equal(t, "services/service-py/contracts/py_daily_kpis.yml", p.Edits[0].Path)
	require.Equal(t, "analytics.py_daily_kpis", p.Edits[0].TargetNodeID)
	require.Equal(t, parseCorrectedYAML, arts.written["proposed-fix/rel-1/analytics.py_daily_kpis/attempt-1/edit-0.content"])
	require.Contains(t, arts.written["proposed-fix/rel-1/analytics.py_daily_kpis/attempt-1/edit-0.diff"], "+      orders: select id from analytics.orders")
}

// TestPythonParse_AnswerThatDropsTheRejectedRead_Fails pins that the parse
// lane keeps the validation lane's declaration guard: deleting the read whose
// SQL the parser rejected would make the next parse pass while the script
// still performs that read, so the answer is refused with the reason recorded.
func TestPythonParse_AnswerThatDropsTheRejectedRead_Fails(t *testing.T) {
	root := t.TempDir()
	writeRepoFile(t, root, "services/service-py/contracts/py_daily_kpis.yml", parseRejectedYAML)
	writeRepoFile(t, root, "services/service-py/scripts/py_daily_kpis.py", "print('hi')\n")

	svc, _, pkgr, _, _ := pythonSvc(t, root)
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		Files: []ports.ProposedFile{{
			Path: "services/service-py/contracts/py_daily_kpis.yml",
			Content: "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n    script: scripts/py_daily_kpis.py\n" +
				"    reads: {}\n    output_columns:\n      - name: revenue\n",
		}},
		Rationale: "removed the read", Confidence: "high",
	}}}
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}

	r, err := pythonParseFixer{}.Propose(context.Background(), svc, pythonParseInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusFailed, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, "orders")
	require.Nil(t, r.VerificationContract)
	require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
}

// TestPythonParse_NodeNotDeclared_SkipsWithReason verifies the parse lane
// records its skip as the proposal's rationale, so the operator reading the
// release sees why no fix was attempted.
func TestPythonParse_NodeNotDeclared_SkipsWithReason(t *testing.T) {
	root := t.TempDir()
	writeRepoFile(t, root, "services/service-py/contracts/other.yml", siblingYAML)

	svc, _, pkgr, _, _ := pythonSvc(t, root)
	llm := &fakeLLM{}
	svc.LLM = llm
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}

	r, err := pythonParseFixer{}.Propose(context.Background(), svc, pythonParseInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
	require.NotEmpty(t, r.Proposal.Rationale)
	require.Zero(t, llm.calls)
	require.Empty(t, pkgr.calls)
}
