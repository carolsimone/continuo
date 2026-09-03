package fixer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
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
// a python-model node, not the csv node under repair, but packaged and
// re-validated alongside it. It carries kind and script explicitly so a test
// that edits it while preserving its identity can do so without those fields
// appearing to change from their zero value.
const csvSiblingYAML = `nodes:
  - schema: analytics
    table: other
    kind: python-model
    script: scripts/other.py
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
	rel := &fakeReleases{}
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
// only corrects output_columns is applied and packaged from the whole
// contract directory into the merged contract returned as
// Result.VerificationContract for the driver to submit — and the LLM request it
// was built from carries the csv-specific system prompt and tool name, not
// the python-model ones.
func TestCsvValidation_HappyPath(t *testing.T) {
	root := csvRepoTree(t)
	svc, arch, pkgr, _, arts := csvSvc(t, root)
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

	// The merged contract is returned in memory for the driver, not uploaded.
	require.Equal(t, []byte("merged: contract\n"), r.VerificationContract)

	p := r.Proposal
	require.Equal(t, proposal.StatusProposed, p.Status,
		"an answer that only edits output_columns must be proposed, not refused as a false breach")
	require.Len(t, p.Edits, 1)
	require.Equal(t, "services/service-csv/contracts/py_csv_orders.yml", p.Edits[0].Path)
	require.Equal(t, "analytics.py_csv_orders", p.Edits[0].TargetNodeID)
	for key := range arts.written {
		require.NotContains(t, key, "contract.yaml",
			"this lane never uploads the merged contract itself; the driver does, from Result.VerificationContract")
	}

	// The prompt sent to the model is the csv lane's own: its rules, not the
	// python-model script rules, and its own tool name.
	require.Len(t, llm.requests, 1)
	require.Contains(t, llm.requests[0].System, "python-csv")
	require.NotContains(t, llm.requests[0].System, "propose_python_fix")
	require.NotContains(t, llm.requests[0].System, "the script still performs that read",
		"a python-csv node has no script, so the python-model lane's read-preservation rationale must not leak into this prompt")
	require.Equal(t, "propose_csv_fix", llm.requests[0].ToolName)
}

// TestCsvValidation_AnswerCorrectsTheCsvURI_Verifies covers the second
// sanctioned repair: when the evidence shows the declared uri itself is
// stale (rather than output_columns being wrong), the fix corrects the "csv"
// read's VALUE instead. The key survives, so the guard passes and the fix
// reaches a verification run exactly as an output_columns-only fix does.
func TestCsvValidation_AnswerCorrectsTheCsvURI_Verifies(t *testing.T) {
	root := csvRepoTree(t)
	svc, _, pkgr, _, _ := csvSvc(t, root)
	const target = "services/service-csv/contracts/py_csv_orders.yml"
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		Files: []ports.ProposedFile{{Path: target, Content: `nodes:
  - schema: analytics
    table: py_csv_orders
    kind: python-csv
    reads:
      csv: s3://exports/orders/2026-08-01/orders.csv
    output_columns:
      - name: order_id
      - name: customer_id
`}},
		Confidence: "high",
	}}}

	r, err := csvValidationFixer{}.Propose(context.Background(), svc, csvInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusProposed, r.Proposal.Status,
		"correcting the csv read's uri is a sanctioned repair and must be proposed")
	require.Len(t, pkgr.calls, 1)
	require.NotEmpty(t, r.VerificationContract)
}

// TestCsvValidation_AnswerDeletesTheCsvRead_Fails covers the cheapest way to
// make a verification run pass without repairing anything: delete the "csv"
// read entirely, or rename it, and there is nothing left for the runtime to
// load — the run passes on a node that is no longer a csv node at all.
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
			svc, _, pkgr, _, arts := csvSvc(t, root)
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
			require.Empty(t, arts.written, "a refused answer writes no artifacts")
		})
	}
}

// TestCsvValidation_SiblingReadDropped_Fails covers what the csv lane's
// relaxed read rule must NOT protect: a python-model SIBLING declared in the
// same contract directory whose read the answer deletes. The csv lane only
// relaxes the rule for the failing node's own "csv" key — a python-model
// sibling's script is never edited by this fix, so its reads are still held
// to the blanket "no read may be dropped" rule declarationBreach applies for
// the python-model lane. The guard's per-node loop must examine every node in
// the answer, not only the ones carrying a "csv" key, or a sibling's dropped
// read would go unnoticed.
func TestCsvValidation_SiblingReadDropped_Fails(t *testing.T) {
	root := csvRepoTree(t)
	svc, _, pkgr, _, arts := csvSvc(t, root)
	const sibling = "services/service-csv/contracts/other.yml"
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		Files: []ports.ProposedFile{
			{Path: "services/service-csv/contracts/py_csv_orders.yml", Content: csvCorrectedYAML},
			// Same identity as csvSiblingYAML (schema, table, kind, script) but
			// its "customers" read is gone.
			{Path: sibling, Content: `nodes:
  - schema: analytics
    table: other
    kind: python-model
    script: scripts/other.py
`},
		},
		Confidence: "high",
	}}}

	r, err := csvValidationFixer{}.Propose(context.Background(), svc, csvInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusFailed, r.Proposal.Status,
		"deleting a sibling's read must be refused even though the failing csv node's own fix is otherwise correct")
	require.Contains(t, r.Proposal.Rationale, "customers",
		"a failed attempt must name the read the answer dropped")
	require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
	require.Empty(t, arts.written)
}

// TestCsvValidation_AnswerChangesKind_Fails verifies that an answer which
// flips the failing node's declared kind — the field that says which set of
// fix rules govern it, script-preserving for python-model or
// csv-read-preserving for python-csv — is refused as an identity change, the
// same as renaming its table would be. Kind is compared as part of
// NodeIdentity (ports.NodeIdentity), so this also protects the python-model
// lane from the opposite flip: an answer that quietly turns a python-model
// node into a python-csv one.
func TestCsvValidation_AnswerChangesKind_Fails(t *testing.T) {
	root := csvRepoTree(t)
	svc, _, pkgr, _, arts := csvSvc(t, root)
	const target = "services/service-csv/contracts/py_csv_orders.yml"
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		Files: []ports.ProposedFile{{Path: target, Content: `nodes:
  - schema: analytics
    table: py_csv_orders
    kind: python-model
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
		"an answer that flips the node's kind must never be verified as a fix")
	require.Contains(t, r.Proposal.Rationale, "kind")
	require.Contains(t, r.Proposal.Rationale, "analytics.py_csv_orders")
	require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
	require.Empty(t, arts.written)
}

// TestCsvValidation_AnswerThatChangesNodeIdentity_Fails verifies that an
// answer which re-identifies the failing node — here by renaming its table —
// is refused before any verification run is spent proving it, the same
// identity guard the python-model lane already enforces.
func TestCsvValidation_AnswerThatChangesNodeIdentity_Fails(t *testing.T) {
	root := csvRepoTree(t)
	svc, _, pkgr, _, arts := csvSvc(t, root)
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
	svc, arch, pkgr, _, _ := csvSvc(t, root)
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
	require.Equal(t, 1, arch.cleanups)
}

// TestCsvValidation_UnusableTrigger_Skips mirrors the python-model lane's
// equivalent (TestPythonValidation_UnusableTrigger_Skips): a trigger this
// lane can never act on stops with the reason recorded instead of being
// redelivered forever. Both cases exercise locateContractForFix, the helper
// now shared by both python lanes.
func TestCsvValidation_UnusableTrigger_Skips(t *testing.T) {
	t.Run("no service", func(t *testing.T) {
		svc, arch, _, _, _ := csvSvc(t, csvRepoTree(t))
		in := csvInput()
		in.Service = ""

		r, err := csvValidationFixer{}.Propose(context.Background(), svc, in)
		require.NoError(t, err)
		require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
		require.Contains(t, r.Proposal.Rationale, "service")
		require.Equal(t, 0, arch.calls)
	})

	t.Run("repository or commit gone", func(t *testing.T) {
		svc, arch, _, _, _ := csvSvc(t, csvRepoTree(t))
		arch.err = ports.ErrSourceNotFound

		r, err := csvValidationFixer{}.Propose(context.Background(), svc, csvInput())
		require.NoError(t, err)
		require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
		require.NotEmpty(t, r.Proposal.Rationale)
		require.Equal(t, 1, arch.calls, "the fetch was attempted once even though it failed")
	})
}

// TestCsvValidation_SiblingFailureInSameDirectory_Skips verifies that a
// second node failing validation in the same contract directory as the
// failing csv node stops the attempt before any model call — the same
// unverifiable-fix guard the python-model lane already enforces
// (TestPythonValidation_SiblingFailureInSameDirectory_Skips), now reached
// through the shared locateContractForFix helper.
func TestCsvValidation_SiblingFailureInSameDirectory_Skips(t *testing.T) {
	root := csvRepoTree(t)
	svc, _, pkgr, rel, arts := csvSvc(t, root)
	llm := svc.LLM.(*fakeLLM)

	// other.yml sits in the same contracts/ directory as the failing node's
	// own declaring file, so packaging one packages both.
	rel.failingNodes = map[string]string{
		"analytics.py_csv_orders": "csv header missing declared column(s): ['customer_id']",
		"analytics.other":         "column country does not exist",
	}

	r, err := csvValidationFixer{}.Propose(context.Background(), svc, csvInput())
	require.NoError(t, err)

	require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, "analytics.other",
		"the rationale must name the node that makes this fix unverifiable")
	require.Equal(t, []string{"rel-2"}, rel.failingNodesCalls,
		"the sibling check reads the ORIGINAL failing release, not a verification run")
	require.Equal(t, 0, llm.calls, "no model call is worth making for an unverifiable fix")
	require.Empty(t, pkgr.calls)
	require.Empty(t, r.VerificationContract, "no release-queue slot is spent on a fix that cannot pass")
	require.Empty(t, arts.written)
}
