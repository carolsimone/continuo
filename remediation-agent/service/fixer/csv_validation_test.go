package fixer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/adapters/repofs"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// --- fixture ----------------------------------------------------------------

// csvDeclaringYAML declares one python-csv node: no script, a single "csv"
// read naming the file the runtime loads, and the output_columns it promises
// that file to carry. customer_id is the mis-declared column the failing
// release's header check rejected.
const csvDeclaringYAML = `nodes:
  - schema: analytics
    table: py_csv_orders
    kind: python-csv
    reads:
      csv: s3://exports/orders/orders.csv
    output_columns:
      - name: order_id
      - name: customer_id
`

// csvCorrectedYAML is the answer a fix that renames the mis-declared column to
// match the file's real header would return.
const csvCorrectedYAML = `nodes:
  - schema: analytics
    table: py_csv_orders
    kind: python-csv
    reads:
      csv: s3://exports/orders/orders.csv
    output_columns:
      - name: order_id
      - name: cust_id
`

// csvSiblingYAML is the second node declared in the same contract directory:
// not the node under repair, but packaged and re-validated alongside it.
const csvSiblingYAML = `nodes:
  - schema: analytics
    table: other
    reads:
      customers: select id from analytics.customers
`

// csvRepoTree writes a repository checkout holding one python service whose
// contracts directory declares the failing csv node, and returns its root.
func csvRepoTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(root, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o750))
		require.NoError(t, os.WriteFile(full, []byte(content), 0o600))
	}
	write("services/service-csv/contracts/py_csv_orders.yml", csvDeclaringYAML)
	write("services/service-csv/contracts/other.yml", csvSiblingYAML)
	return root
}

const wantCsvShadowID = "shadow-rel-2-analytics.py_csv_orders-a1"
const wantCsvArtifactKey = "svc-csv/" + wantCsvShadowID + "/contract.yaml"

// csvInput is the trigger a rejected python-csv node produces.
func csvInput() Input {
	return Input{
		Source: "validation", ReleaseID: "rel-2", NodeID: "analytics.py_csv_orders",
		NodeType: "python-csv", Service: "svc-csv", Repo: "o/demo", CommitSHA: "deadbeef",
		ErrorExcerpt: "csv header missing declared column(s): ['customer_id']", ErrorSignature: "sig-2",
		DBTLogURI: "s3://log", CodeBundleURI: "s3://bundle", Attempt: 1,
	}
}

// csvSvc wires every collaborator for a csv contract fix happy path.
func csvSvc(t *testing.T, root string) (Services, *fakeArchive, *fakePackager, *fakeReleases, *fakeArtifacts) {
	t.Helper()
	arch := &fakeArchive{root: root}
	locator := repofs.NewLocator(testLogger())
	pkgr := &fakePackager{merged: []byte("merged: contract\n")}
	arts := &fakeArtifacts{}
	rel := &fakeReleases{
		tag: "ghcr.io/o/service-csv:v3", artifacts: arts,
		artifactKey: func(id string) string { return "svc-csv/" + id + "/contract.yaml" },
	}
	svc := Services{
		LLM: &fakeLLM{queue: []ports.ProposeResult{{
			Files: []ports.ProposedFile{
				{Path: "services/service-csv/contracts/py_csv_orders.yml", Content: csvCorrectedYAML},
			},
			Rationale: "renamed the column to match the file's real header", Confidence: "high", Model: "test-model",
		}}},
		Evidence:          fakeEvidence{data: map[string]string{"s3://log": "runner log body"}},
		Sanitizer:         fakeSanitizer{},
		Artifacts:         arts,
		Logger:            testLogger(),
		Upstream:          &fakeUpstream{},
		Precedents:        &fakePrecedents{},
		CandidateSource:   &fakeCandidateSource{src: ports.CandidateSource{RawCode: `{"output_columns":[]}`, Runtime: "python"}},
		Archive:           arch,
		ContractLocator:   locator,
		ContractInspector: locator,
		Packager:          pkgr,
		Releases:          rel,
		PriorAttempts:     &fakeAttempts{},
		SQLDialect:        "postgres",
	}
	return svc, arch, pkgr, rel, arts
}

// --- tests ------------------------------------------------------------------

// TestCsvValidation_HappyPath walks the whole lane end to end: an answer that
// only corrects output_columns is applied, packaged from the whole contract
// directory, uploaded, and submitted as a shadow release — and the LLM
// request it was built from carries the csv-specific system prompt and tool
// name, not the python-model ones.
func TestCsvValidation_HappyPath(t *testing.T) {
	root := csvRepoTree(t)
	svc, arch, pkgr, rel, arts := csvSvc(t, root)
	llm := svc.LLM.(*fakeLLM)

	r, err := csvValidationFixer{}.Propose(context.Background(), svc, csvInput())
	require.NoError(t, err)

	require.Equal(t, 1, arch.calls)
	require.Equal(t, 1, arch.cleanups)

	require.Len(t, pkgr.calls, 1)
	call := pkgr.calls[0]
	require.Equal(t, filepath.Join(root, "services", "service-csv", "contracts"), call.contractDir)
	require.Equal(t, "svc-csv", call.service)
	require.Equal(t, csvCorrectedYAML, call.sawOnDisk["py_csv_orders.yml"])
	require.Equal(t, csvSiblingYAML, call.sawOnDisk["other.yml"],
		"a file the model did not return must be left exactly as the repository holds it")

	require.Equal(t, "merged: contract\n", arts.written[wantCsvArtifactKey])

	require.Len(t, rel.submissions, 1)
	require.Equal(t, wantCsvShadowID, rel.submissions[0].ReleaseID)

	p := r.Proposal
	require.Equal(t, proposal.StatusVerifying, p.Status,
		"an answer that only edits output_columns must reach a shadow release, not be refused as a false breach")
	require.Equal(t, wantCsvShadowID, p.ShadowReleaseID)
	require.Len(t, p.Edits, 1)
	require.Equal(t, "services/service-csv/contracts/py_csv_orders.yml", p.Edits[0].Path)

	// The prompt sent to the model is the csv lane's own: its rules, not the
	// python-model script rules, and its own tool name.
	require.Len(t, llm.requests, 1)
	require.Contains(t, llm.requests[0].System, "python-csv")
	require.Contains(t, llm.requests[0].System, "csv")
	require.Equal(t, "propose_csv_fix", llm.requests[0].ToolName)
}

// TestCsvValidation_AnswerDeletesTheCsvRead_Fails covers the cheapest way to
// make a shadow release pass without repairing anything: delete the "csv"
// read entirely, or rename it, and there is nothing left for the runtime to
// load — the release validates on a node that is no longer a csv node at all.
func TestCsvValidation_AnswerDeletesTheCsvRead_Fails(t *testing.T) {
	const target = "services/service-csv/contracts/py_csv_orders.yml"

	cases := map[string]string{
		"the csv read is removed entirely": `nodes:
  - schema: analytics
    table: py_csv_orders
    kind: python-csv
    output_columns:
      - name: order_id
      - name: cust_id
`,
		"the csv read is renamed": `nodes:
  - schema: analytics
    table: py_csv_orders
    kind: python-csv
    reads:
      data: s3://exports/orders/orders.csv
    output_columns:
      - name: order_id
      - name: cust_id
`,
	}

	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			root := csvRepoTree(t)
			svc, _, pkgr, rel, arts := csvSvc(t, root)
			svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
				Files:      []ports.ProposedFile{{Path: target, Content: content}},
				Confidence: "high",
			}}}

			r, err := csvValidationFixer{}.Propose(context.Background(), svc, csvInput())
			require.NoError(t, err)
			require.Equal(t, proposal.StatusFailed, r.Proposal.Status,
				"an answer that deletes or renames the csv read must never be verified as a fix")
			require.Contains(t, r.Proposal.Rationale, "csv",
				"a failed attempt must name the read the answer dropped")
			require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
			require.Empty(t, rel.submissions, "no release slot is spent proving a deletion")
			require.Empty(t, arts.written, "a refused answer writes no artifacts")
		})
	}
}

// TestCsvValidation_AnswerThatChangesNodeIdentity_Fails verifies that an
// answer which re-identifies the failing node — here by renaming its table —
// is refused before any shadow release is spent proving it, the same identity
// guard the python-model lane already enforces.
func TestCsvValidation_AnswerThatChangesNodeIdentity_Fails(t *testing.T) {
	root := csvRepoTree(t)
	svc, _, pkgr, rel, arts := csvSvc(t, root)
	const target = "services/service-csv/contracts/py_csv_orders.yml"
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		Files: []ports.ProposedFile{{Path: target, Content: `nodes:
  - schema: analytics
    table: py_csv_orders_v2
    kind: python-csv
    reads:
      csv: s3://exports/orders/orders.csv
    output_columns:
      - name: order_id
      - name: cust_id
`}},
		Confidence: "high",
	}}}

	r, err := csvValidationFixer{}.Propose(context.Background(), svc, csvInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusFailed, r.Proposal.Status,
		"an answer that re-identifies a node must never be verified as a fix")
	require.Contains(t, r.Proposal.Rationale, "analytics.py_csv_orders")
	require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
	require.Empty(t, rel.submissions, "no release slot is spent proving a deletion")
	require.Empty(t, arts.written)
}

// TestCsvValidation_MalformedNodeID_Skips verifies that a node id from which
// no schema and table can be read stops with a recorded reason rather than
// searching the tree for an empty name.
func TestCsvValidation_MalformedNodeID_Skips(t *testing.T) {
	root := csvRepoTree(t)
	svc, arch, _, _, _ := csvSvc(t, root)

	in := csvInput()
	in.NodeID = "py_csv_orders"

	r, err := csvValidationFixer{}.Propose(context.Background(), svc, in)
	require.NoError(t, err)
	require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
	require.NotEmpty(t, r.Proposal.Rationale)
	require.Equal(t, 0, arch.calls, "a node id that names no schema and table is decided before any fetch")
}

// TestCsvValidation_NodeNotDeclared_Skips verifies that a node no contract
// file declares is recorded as skipped with the reason kept as the
// rationale, and that nothing downstream of the search runs.
func TestCsvValidation_NodeNotDeclared_Skips(t *testing.T) {
	root := csvRepoTree(t)
	svc, arch, pkgr, rel, _ := csvSvc(t, root)
	llm := &fakeLLM{}
	svc.LLM = llm

	in := csvInput()
	in.NodeID = "analytics.not_here"

	r, err := csvValidationFixer{}.Propose(context.Background(), svc, in)
	require.NoError(t, err)
	require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, "not declared")
	require.Equal(t, 0, llm.calls)
	require.Empty(t, pkgr.calls)
	require.Empty(t, rel.submissions)
	require.Equal(t, 1, arch.cleanups)
}
