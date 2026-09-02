package e2e

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// The remediation fixture repository: the working tree stub-github serves from
// GET /repos/{owner}/{repo}/tarball/{ref}, and the source of the contract each
// of these tests posts a release for. Both readers take the same files, so the
// contract a release is rejected on and the contract the fixer finds in the
// checkout cannot describe different nodes.
//
// Its two services exist so the two scenarios below never share a contract
// directory. The packaging step merges a whole directory into one release
// artifact, so a second, still-broken node beside the node under repair would
// sink the verification run that is verifying the repair.
//
//go:embed fixtures/py-remediation-repo
var pyRemediationRepo embed.FS

const (
	// pyRemediationRepoRoot is the embedded fixture's prefix. The paths below
	// are relative to it and are also the paths inside the served tarball,
	// which is what makes the assertions on a proposal's edited file readable.
	pyRemediationRepoRoot = "fixtures/py-remediation-repo"

	// The single-node service whose declared read cannot bind, and the file
	// that declares it. One fix attempt repairs it.
	pyBadReadService  = "svc-py-e2e"
	pyBadReadUniqueID = "e2e_schema.py_bad_read"
	pyBadReadContract = "services/svc-py-e2e/contracts/py_bad_read.yml"
	pyBadReadScript   = "services/svc-py-e2e/scripts/py_bad_read.py"

	// The second service, whose first repair is also wrong: it takes two
	// attempts, and the second only succeeds because it is shown the first
	// one's verification-run error.
	pyLoopService  = "svc-py-e2e-loop"
	pyLoopUniqueID = "e2e_schema.py_loop_read"
	pyLoopContract = "services/svc-py-e2e-loop/contracts/py_loop_read.yml"
	pyLoopScript   = "services/svc-py-e2e-loop/scripts/py_loop_read.py"

	// pyBindingRelation is the relation a repaired contract must read from. It
	// exists in the warehouse only because these tests create it, which is
	// what makes a verification run's verdict a real measurement rather than a
	// foregone conclusion.
	pyBindingRelation = "public.right_name"

	// pyBrokenRelation and pyLoopBrokenRelation are what the two fixtures
	// declare before repair; pyStillBrokenRelation is the canned model's first,
	// still-wrong answer for the loop fixture. None of the three exists.
	pyBrokenRelation      = "public.wrong_name"
	pyLoopBrokenRelation  = "public.loop_wrong_name"
	pyStillBrokenRelation = "public.still_wrong_name"
)

// Stage wait budgets. pollUntil does not select on the test's context, so a
// context that expires before a stage's own budget turns every remaining stage
// into a silent stall that ends on the wrong assertion — and a suite timeout
// that expires first replaces a named assertion with a goroutine dump. Each
// test's context deadline is therefore strictly greater than the sum of the
// budgets it uses, and the suite's go test -timeout strictly greater than the
// sum of both contexts (see the Makefile and .github/workflows/ci.yml).
//
// Each budget is sized for the stage it covers and no more: a budget ten times
// a stage's normal duration buys nothing when the stage works and costs exactly
// that much when it breaks.
const (
	// pyReleaseVerdictBudget covers a python release from POST to a terminal
	// verdict: queue activation, the contract parse, candidate-artifact upload,
	// the ensure-schema Job, one build_from_columns Job, and teardown. A python
	// release skips the compile leg entirely, which is the slow half of the dbt
	// budget the rest of this suite uses; the margin here is for cold Job
	// scheduling in kind, and this is the first release of each test.
	pyReleaseVerdictBudget = 6 * time.Minute
	// pyVerifyVerdictBudget covers the same shape for a verification run
	// submitted later in the same test, against a stack that is warm by
	// then — including the poll that confirms the pipeline names it while
	// it runs, which resolves in the first cycle or two once the run is
	// active and does not meaningfully add to this budget.
	pyVerifyVerdictBudget = 5 * time.Minute
	// pyAttemptStartBudget covers trigger to recorded attempt: classification,
	// the agent consuming it, the repository fetch, the model call, packaging
	// via the runtime CLI, the artifact upload, and the verification-run
	// submission.
	pyAttemptStartBudget = 3 * time.Minute
	// pyAttemptFinalizeBudget covers a terminal verification verdict reaching
	// the proposal row: one reconciler tick, 5s in this compose stack.
	pyAttemptFinalizeBudget = 2 * time.Minute
	// pyRecordVisibleBudget covers a write becoming readable through another
	// service's API or stream: a submitted release appearing in the release
	// list, an emitted event reaching its stream, a classification decision
	// landing in Postgres.
	pyRecordVisibleBudget = 1 * time.Minute
)

// TestE2E_PythonValidationFailure_VerifiedFix drives the whole python
// remediation lane on a single-attempt repair:
//
//	POST /releases (kind=python, one node whose declared read cannot bind)
//	→ the validation Job's bind check fails → release rejected
//	→ classifier emits remediation.requested:v2 (one node, node_type=python-model)
//	→ agent-remediation routes it to the python contract fixer: fetches the
//	  repository tarball at the failing commit from stub-github, finds the yaml
//	  declaring the node, has the model correct it, packages the directory with
//	  continuo-runtime, uploads it and POSTs it back as a verification run
//	→ the proposal parks in 'verifying' naming that verification run
//	→ the run runs the real parse + candidate-schema + validation pipeline and
//	  stops at 'passed' — it never promotes and is never readable as a release
//	→ the reconciler finalizes the proposal to 'proposed' and emits
//	  remediation.proposed:v1
//
// It is the feature's definition of done: every stage above has to work for it
// to pass. It also asserts the two things that must NOT happen — the
// verification run produced no remediation trigger of its own, and wrote no
// promoted code version — because a verification run that fed the classifier
// would heal its own failed fix forever, and one that reached the graph would
// put an unapproved contract into production history.
func TestE2E_PythonValidationFailure_VerifiedFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// 22 minutes: strictly greater than the 17 this test's stage budgets sum to
	// (release 6 + attempt 3 + verify 5 + finalize 2 + event 1).
	// assertNotListedAsRelease is a plain read, not a poll, so it adds no
	// budget of its own; the pipeline-naming check shares the verification
	// stage's budget rather than stacking one on top of it.
	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// The relation a correct fix reads from has to exist before the
	// verification run bind-checks it. Both cleanups are deferred after
	// clients.close(ctx) above, so LIFO runs them while the pools are open.
	dropRelation := ensureBindingRelation(t, ctx, clients)
	defer dropRelation()

	// Release ids are kept short on purpose: the candidate schema of a
	// verification run is "_candidate_" plus its id, and a run id embeds the
	// failing release id, the node id, and the attempt number.
	releaseID := "pyfix-" + uuid.NewString()[:8]
	t.Logf("release_id=%s service=%s node=%s", releaseID, pyBadReadService, pyBadReadUniqueID)

	clearProd := rejectPythonFixtureRelease(t, ctx, clients,
		pyBadReadService, pyBadReadContract, pyBadReadScript, releaseID)
	defer clearProd()

	// 1. The fixer produced an attempt and parked it awaiting its
	//    verification run. The run id is read off the row rather than
	//    re-derived here, so the naming rule is asserted as the system's, not
	//    the test's.
	verifying := waitForProposal(t, ctx, clients, releaseID, pyBadReadUniqueID, 1, "verifying", pyAttemptStartBudget)
	runID := verifying.VerificationRunID
	require.NotEmpty(t, runID, "a verifying proposal must name the run that is judging it")
	require.True(t, strings.HasPrefix(runID, "verify-"),
		"a verification run id must be recognisable as one in every log line and UI row; got %q", runID)
	require.Contains(t, runID, releaseID,
		"a verification run id must name the release it is repairing; got %q", runID)
	require.True(t, strings.HasSuffix(runID, "-a1"),
		"the first attempt's verification run id must name attempt 1; got %q", runID)
	t.Logf("proposal attempt 1 is verifying under verification run %s", runID)

	// 2. That run is not a release: it 404s at GET /releases/{id} and is
	//    absent from GET /releases. The pipeline names it while it is active.
	assertNotListedAsRelease(t, ctx, clients, runID)
	assertPipelineNamedVerification(t, ctx, clients, runID, pyVerifyVerdictBudget)

	// 3. It runs the real pipeline and stops at passed.
	waitForVerificationStatus(t, ctx, clients, runID, "passed", pyVerifyVerdictBudget)

	// 4. The reconciler turns the verified attempt into a reviewable fix.
	proposed := waitForProposal(t, ctx, clients, releaseID, pyBadReadUniqueID, 1, "proposed", pyAttemptFinalizeBudget)
	require.Empty(t, proposed.VerifyError, "a verified fix records no verification error")
	require.Equal(t, runID, proposed.VerificationRunID,
		"finalizing an attempt must not change which run verified it")

	verifications := decodeVerifications(t, proposed.Verifications)
	require.Len(t, verifications, 1,
		"the fix edits one service, so exactly one verification run judged it")
	require.Equal(t, pyBadReadService, verifications[0].Service,
		"the verification must name the service whose files the attempt edited")
	require.Equal(t, runID, verifications[0].RunID,
		"the verification must name the same run the row parked on")

	edits := decodeFileEdits(t, proposed.FileEdits)
	require.Len(t, edits, 1, "the fix changes exactly the one file that declares the node")
	require.Equal(t, pyBadReadContract, edits[0].Path,
		"the edit must name the declaring contract file as the repository holds it")

	// Assert on the declared read itself, not on the relation name anywhere in
	// the file: the fixture's own comments name these relations too, so a
	// bare substring check would pass on an unrepaired contract.
	content := string(getS3ObjectByKey(t, ctx, clients, stripS3Prefix(edits[0].ContentURI)))
	require.Contains(t, content, "select id from "+pyBindingRelation,
		"the proposed contract must declare a read against a relation that exists")
	require.NotContains(t, content, "select id from "+pyBrokenRelation,
		"the proposed contract must no longer declare the read that failed to bind")
	require.Contains(t, content, "table: py_bad_read",
		"the fix must keep declaring the same node, not replace it with a different one")

	// 5. The fix is announced for human review.
	waitForRemediationProposed(t, ctx, clients, releaseID, pyBadReadUniqueID, pyRecordVisibleBudget)

	// 6. Nothing from the verification run reached production. The proof that
	//    the no-promote gate held is the run reaching "passed" above rather
	//    than "promoted" — a status only a candidate release can reach; these
	//    two checks confirm the consequences of that, and would also catch a
	//    promotion that took some other route.
	var prodRows int
	require.NoError(t, clients.releaseDB.GetContext(ctx, &prodRows,
		`SELECT count(*) FROM service_prod WHERE service_name = $1`, pyBadReadService))
	require.Zero(t, prodRows, "a verification run must never write the service's production pointer")
	assertNoNodeVersionsFor(t, ctx, clients, runID)

	// 7. No remediation trigger names the verification run. This scan cannot
	//    fail here — a passed run emits no rejection, so no classifier path
	//    could produce one — and it is kept as a cheap guard against a future
	//    trigger source, not as evidence. The anti-loop rule is proved where it
	//    can actually be violated: the sibling test below, whose first
	//    attempt's verification run genuinely fails.
	assertNoRemediationTriggerFor(t, ctx, clients, runID)

	t.Log("✅ a rejected python node was repaired, verified by a real verification run, and proposed for review")
}

// TestE2E_PythonValidationFailure_VerificationErrorFeedsNextAttempt drives the
// same lane when the first repair is wrong, which is the property that makes
// the python lane a loop rather than a single shot: attempt n+1 is shown what
// attempt n changed and the error its verification run reported back.
//
// The canned model answers the first attempt with a read that still cannot
// bind, and answers a retry with the binding one — recognising a retry by the
// prompt's earlier-attempts section BOTH existing AND naming the relation the
// rejected attempt declared, read from the part of the prompt that is not the
// contract file itself. That relation name reaches the prompt only through the
// first attempt's recorded verification error or the diff it applied. Attempt 2
// reaching 'proposed' is therefore proof that the failed attempt's evidence was
// assembled AND shown, not merely that an attempt row existed; a retry whose
// prompt lost that evidence gets the answer that already failed, and this test
// never goes green.
//
// It also proves the loop cannot eat itself: a failed verification run
// reaches no classifier at all — no decision row, no remediation trigger —
// which is what stops a failed fix from remediating itself.
func TestE2E_PythonValidationFailure_VerificationErrorFeedsNextAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// 34 minutes: strictly greater than the 26 this test's stage budgets sum to
	// (release 6 + attempt 3 + verify 5 + finalize 2, then attempt 3 + verify 5
	// + finalize 2). assertNotListedAsRelease and the classifier reads below
	// are plain queries, not polls, so neither adds a budget of its own; the
	// pipeline-naming check shares the verification stage's budget rather than
	// stacking one on top of it.
	ctx, cancel := context.WithTimeout(context.Background(), 34*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	dropRelation := ensureBindingRelation(t, ctx, clients)
	defer dropRelation()

	releaseID := "pyloop-" + uuid.NewString()[:8]
	t.Logf("release_id=%s service=%s node=%s", releaseID, pyLoopService, pyLoopUniqueID)

	clearProd := rejectPythonFixtureRelease(t, ctx, clients,
		pyLoopService, pyLoopContract, pyLoopScript, releaseID)
	defer clearProd()

	// 1. Attempt 1's verification run fails the fix, and its error is
	//    recorded on the attempt as the evidence attempt 2 will be shown.
	firstVerifying := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 1, "verifying", pyAttemptStartBudget)
	firstRunID := firstVerifying.VerificationRunID
	require.NotEmpty(t, firstRunID)
	require.True(t, strings.HasSuffix(firstRunID, "-a1"),
		"the first attempt's verification run id must name attempt 1; got %q", firstRunID)
	assertNotListedAsRelease(t, ctx, clients, firstRunID)
	assertPipelineNamedVerification(t, ctx, clients, firstRunID, pyVerifyVerdictBudget)
	waitForVerificationStatus(t, ctx, clients, firstRunID, "failed", pyVerifyVerdictBudget)

	firstFailed := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 1, "failed", pyAttemptFinalizeBudget)
	require.NotEmpty(t, firstFailed.VerifyError,
		"a failed attempt must record why, or the next attempt has nothing new to learn from")
	require.NotContains(t, firstFailed.VerifyError, "timed out",
		"the recorded reason must be the verification run's own verdict, not a wait that expired")
	require.Contains(t, firstFailed.VerifyError, "still_wrong_name",
		"the recorded reason must be the error the verification run reported for this node; got %q",
		firstFailed.VerifyError)
	t.Logf("attempt 1 failed verification: %s", firstFailed.VerifyError)

	// 2. Attempt 2 runs against a second verification run and this one passes.
	secondVerifying := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 2, "verifying", pyAttemptStartBudget)
	secondRunID := secondVerifying.VerificationRunID
	require.NotEmpty(t, secondRunID)
	require.NotEqual(t, firstRunID, secondRunID,
		"each attempt must be verified by its own run, or one verdict would answer for two fixes")
	require.True(t, strings.HasSuffix(secondRunID, "-a2"),
		"the second attempt's verification run id must name attempt 2; got %q", secondRunID)
	assertNotListedAsRelease(t, ctx, clients, secondRunID)
	waitForVerificationStatus(t, ctx, clients, secondRunID, "passed", pyVerifyVerdictBudget)

	secondProposed := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 2, "proposed", pyAttemptFinalizeBudget)
	edits := decodeFileEdits(t, secondProposed.FileEdits)
	require.Len(t, edits, 1)
	require.Equal(t, pyLoopContract, edits[0].Path)

	content := string(getS3ObjectByKey(t, ctx, clients, stripS3Prefix(edits[0].ContentURI)))
	require.Contains(t, content, "select id from "+pyBindingRelation,
		"the second attempt must declare a read against a relation that exists")
	require.NotContains(t, content, "select id from "+pyStillBrokenRelation,
		"the second attempt must not repeat the read the first attempt failed for")
	require.Contains(t, content, "table: py_loop_read",
		"the fix must keep declaring the same node, not replace it with a different one")

	// 3. The failed verification run reached no classifier: no decision row
	//    was written for it and no remediation trigger names it, which is
	//    what stops a failed fix from remediating itself.
	var n int
	require.NoError(t, clients.remediationDB.GetContext(ctx, &n,
		`SELECT count(*) FROM classification_decision WHERE release_id = $1`, firstRunID))
	require.Zero(t, n, "a verification run's failure must never reach the classifier")
	msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV2, "-", "+").Result()
	require.NoError(t, err)
	for _, m := range msgs {
		require.NotContains(t, m.Values["payload"], firstRunID, "no remediation trigger may name a verification run")
	}
	assertNoNodeVersionsFor(t, ctx, clients, firstRunID)
	assertNoNodeVersionsFor(t, ctx, clients, secondRunID)

	t.Log("✅ a rejected fix's verification error fed the next attempt, which was verified and proposed")
}

// rejectPythonFixtureRelease posts one of the remediation fixture repository's
// services as a python release and waits for validation to reject it. The
// contract it uploads is built from the same file the served repository
// checkout holds, so the node the release fails on is the node the fixer will
// later find in the checkout.
//
// It clears both python fixture services' production pointers before posting
// and returns a cleanup that clears them again — a leftover pointer would drag
// this fixture's contract into every later test's assembled manifest set. The
// cleanup is returned rather than registered with t.Cleanup because it needs
// the release-controller pool, and t.Cleanup functions run after every
// deferred call, including the one that closes every pool.
func rejectPythonFixtureRelease(
	t *testing.T, ctx context.Context, clients *testClients,
	service, contractPath, scriptPath, releaseID string,
) func() {
	t.Helper()

	// Baseline: every dbt service keeps its live pointer; the python fixture
	// service is unknown to production, so its single node is a changed node.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "content_hash": n.contentHash,
			})
		}
	}

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, service)
	clearPythonServiceProd(t, ctx, clients)
	cleanup := func() { clearPythonServiceProd(t, context.Background(), clients) }

	contractKey := fmt.Sprintf("%s/%s/contract.yaml", service, releaseID)
	putS3Object(t, ctx, clients, contractKey,
		[]byte(pyRemediationContractYAML(t, service, contractPath, scriptPath)))
	postPythonRelease(t, clients, service, releaseID, pyFixtureImage)

	waitForReleaseRejected(t, ctx, clients, releaseID, pyReleaseVerdictBudget)
	return cleanup
}

// pyRemediationContractYAML renders the merged contract_version-1 wire artifact
// a python service's CI uploads before POST /releases, from the fixture
// repository's own authored contract file and script.
//
// It applies the same per-node hash recipe as the python release test's
// pythonContractYAML: source_hash is the real sha256 of the script, so editing
// the script genuinely re-fingerprints the node; shared_code_hash is empty (the
// script imports nothing in-repo); and topology-controller recomputes only the
// three-part fold, so any deterministic config_hash is accepted as long as the
// fold matches.
func pyRemediationContractYAML(t *testing.T, service, contractPath, scriptPath string) string {
	t.Helper()

	contract, err := fs.ReadFile(pyRemediationRepo, path.Join(pyRemediationRepoRoot, contractPath))
	require.NoError(t, err, "read fixture contract %s", contractPath)
	script, err := fs.ReadFile(pyRemediationRepo, path.Join(pyRemediationRepoRoot, scriptPath))
	require.NoError(t, err, "read fixture script %s", scriptPath)

	var doc struct {
		Nodes []map[string]any `yaml:"nodes"`
	}
	require.NoError(t, yaml.Unmarshal(contract, &doc), "parse fixture contract %s", contractPath)
	require.NotEmpty(t, doc.Nodes, "fixture contract %s declares no nodes", contractPath)

	for _, node := range doc.Nodes {
		entryJSON, err := json.Marshal(node)
		require.NoError(t, err)
		sourceHash := sha256Hex(string(script))
		configHash := sha256Hex(string(entryJSON))
		node["source_hash"] = sourceHash
		node["shared_code_hash"] = ""
		node["config_hash"] = configHash
		node["content_hash"] = "sha256:" + sha256Hex(sourceHash+"|"+""+"|"+configHash)
	}

	merged, err := yaml.Marshal(map[string]any{
		"contract_version": 1,
		"service":          service,
		"nodes":            doc.Nodes,
	})
	require.NoError(t, err, "marshal merged contract")
	return string(merged)
}

// ensureBindingRelation creates the relation a repaired contract reads from
// and returns a cleanup that drops it. It lives outside the node registry, so
// the candidate-schema rewriter passes it through verbatim and the validation
// Job's bind check resolves it against the real warehouse — which is exactly
// why it has to exist for a verification run to pass.
//
// The cleanup is returned rather than registered with t.Cleanup because it
// needs the warehouse pool, and t.Cleanup functions run after every deferred
// call — including the one that closes every pool. A caller that registers it
// with defer after `defer clients.close(ctx)` gets it run first, while the
// pool is still open.
func ensureBindingRelation(t *testing.T, ctx context.Context, clients *testClients) func() {
	t.Helper()
	// The relation name is interpolated because a table name cannot be bound as
	// a query parameter. pyBindingRelation is a constant in this file and the
	// same one the assertions read, so the table this creates and the table the
	// repaired contract is checked against can never name different relations.
	_, err := clients.dbtDB.ExecContext(ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id integer)`, pyBindingRelation))
	require.NoError(t, err, "create %s in the warehouse", pyBindingRelation)
	return func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := clients.dbtDB.ExecContext(cleanupCtx,
			fmt.Sprintf(`DROP TABLE IF EXISTS %s`, pyBindingRelation)); err != nil {
			t.Errorf("cleanup: drop %s: %v", pyBindingRelation, err)
		}
	}
}

// clearPythonServiceProd removes the production pointers of both remediation
// fixture services. Neither is ever meant to reach production in these tests,
// so a row for either is stale state from an earlier run and would be read
// back during manifest assembly.
func clearPythonServiceProd(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	for _, svc := range []string{pyBadReadService, pyLoopService} {
		if _, err := clients.releaseDB.ExecContext(ctx,
			`DELETE FROM service_prod WHERE service_name = $1`, svc); err != nil {
			t.Errorf("clear service_prod row for %s: %v", svc, err)
		}
	}
}

// pyProposalRow captures the verification-related fields of a proposal row.
type pyProposalRow struct {
	Status            string `db:"status"`
	Attempt           int    `db:"attempt"`
	VerificationRunID string `db:"verification_run_id"`
	VerifyError       string `db:"verify_error"`
	FilePath          string `db:"file_path"`
	FileEdits         []byte `db:"file_edits"`
	Verifications     []byte `db:"verifications"`
}

// waitForProposal polls the agent-remediation's proposal table until the named
// attempt for a node reaches want, and returns the row. Every status change it
// observes is logged, so a run that stalls in 'generating' or 'verifying' is
// distinguishable from one that never produced an attempt at all — the timeout
// message itself cannot carry that, since pollUntil evaluates it before any
// polling happens.
func waitForProposal(
	t *testing.T, ctx context.Context, clients *testClients,
	releaseID, nodeID string, attempt int, want string, timeout time.Duration,
) pyProposalRow {
	t.Helper()
	var row pyProposalRow
	var last string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row, `
			SELECT status, attempt, verification_run_id, verify_error, file_path, file_edits, verifications
			  FROM proposal
			 WHERE release_id = $1 AND node_id = $2 AND attempt = $3`,
			releaseID, nodeID, attempt)
		if err != nil {
			return false, nil
		}
		if row.Status != last {
			t.Logf("proposal %s/%s attempt %d: status=%s", releaseID, nodeID, attempt, row.Status)
			last = row.Status
		}
		return row.Status == want, nil
	}, fmt.Sprintf("timeout waiting for proposal %s/%s attempt %d to reach %q (see the logged status changes above)",
		releaseID, nodeID, attempt, want))
	return row
}

// pyFileEdit mirrors one element of the proposal's file_edits column.
// TargetNodeID names the node whose source the edit changes, which for a
// batched attempt is the only thing that says which file repairs which
// failure — the representative node_id column cannot stand for all of them.
type pyFileEdit struct {
	Path         string `json:"path"`
	ContentURI   string `json:"content_uri"`
	DiffURI      string `json:"diff_uri"`
	TargetNodeID string `json:"target_node_id"`
}

// decodeFileEdits reads the proposal row's file_edits column.
func decodeFileEdits(t *testing.T, raw []byte) []pyFileEdit {
	t.Helper()
	var edits []pyFileEdit
	require.NoError(t, json.Unmarshal(raw, &edits), "decode proposal file_edits: %s", raw)
	return edits
}

// pyVerification mirrors one element of the proposal's verifications column:
// the service whose edits a verification run judged, the manifest kind it was
// submitted under, that run's id, and its last-read phase. An attempt posts
// one per edited service, so the list is how a reader tells which verdict
// covered what.
type pyVerification struct {
	Service string `json:"service"`
	Kind    string `json:"kind"`
	RunID   string `json:"run_id"`
	Phase   string `json:"phase"`
}

// decodeVerifications reads the proposal row's verifications column.
func decodeVerifications(t *testing.T, raw []byte) []pyVerification {
	t.Helper()
	var verifications []pyVerification
	require.NoError(t, json.Unmarshal(raw, &verifications), "decode proposal verifications: %s", raw)
	return verifications
}

// waitForVerificationStatus polls GET /verification-runs/{id} until the run
// reaches want ("passed" or "failed"); either is the expected result of one
// test or the other, so no status is treated as a failure on the way.
func waitForVerificationStatus(
	t *testing.T, ctx context.Context, clients *testClients,
	runID, want string, timeout time.Duration,
) {
	t.Helper()
	var last string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		resp, err := http.Get(fmt.Sprintf("%s/verification-runs/%s", clients.releaseBase, runID))
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		var r struct {
			Status     string `json:"status"`
			FailReason string `json:"fail_reason"`
		}
		if json.NewDecoder(resp.Body).Decode(&r) != nil {
			return false, nil
		}
		if r.Status != last {
			t.Logf("verification run %s: status=%s fail_reason=%q", runID, r.Status, r.FailReason)
			last = r.Status
		}
		return r.Status == want, nil
	}, fmt.Sprintf("timeout waiting for verification run %s to reach %q", runID, want))
}

// assertNotListedAsRelease asserts a verification run has no release
// identity: GET /releases/{id} answers 404 and the release list omits it.
func assertNotListedAsRelease(t *testing.T, ctx context.Context, clients *testClients, runID string) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/releases/%s", clients.releaseBase, runID))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode, "a verification run must not be readable as a release")

	resp, err = http.Get(clients.releaseBase + "/releases?limit=100")
	require.NoError(t, err)
	defer resp.Body.Close()
	var body struct {
		Releases []struct {
			ReleaseID string `json:"release_id"`
		} `json:"releases"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	for _, rel := range body.Releases {
		require.NotEqual(t, runID, rel.ReleaseID, "a verification run must not appear in the release list")
	}
}

// assertPipelineNamedVerification waits until GET /pipeline names runID as
// the active run — the read the Releases tab's in-flight strip uses — or
// until the run is already terminal, which a fast run may be before the
// first poll lands.
func assertPipelineNamedVerification(t *testing.T, ctx context.Context, clients *testClients, runID string, timeout time.Duration) {
	t.Helper()
	pollUntil(t, ctx, timeout, time.Second, func() (bool, error) {
		resp, err := http.Get(clients.releaseBase + "/pipeline")
		if err != nil {
			return false, nil
		}
		var p struct {
			Active *struct {
				RunID   string `json:"run_id"`
				RunKind string `json:"run_kind"`
			} `json:"active"`
		}
		err = json.NewDecoder(resp.Body).Decode(&p)
		resp.Body.Close()
		if err != nil {
			return false, nil
		}
		if p.Active != nil && p.Active.RunID == runID {
			require.Equal(t, "verification", p.Active.RunKind)
			return true, nil
		}
		resp, err = http.Get(fmt.Sprintf("%s/verification-runs/%s", clients.releaseBase, runID))
		if err != nil {
			return false, nil
		}
		var r struct {
			Status string `json:"status"`
		}
		err = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		return err == nil && (r.Status == "passed" || r.Status == "failed"), nil
	}, fmt.Sprintf("timeout waiting for the pipeline to name verification run %s", runID))
}

// waitForRemediationProposed polls remediation.proposed:v1 for the fix a
// verified attempt announces. The reconciler writes the row and enqueues this
// event in one transaction, so its arrival is what proves the announcement was
// not lost between the two.
func waitForRemediationProposed(
	t *testing.T, ctx context.Context, clients *testClients, releaseID, nodeID string, timeout time.Duration,
) {
	t.Helper()
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.RemediationProposedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			raw, _ := msg.Values["payload"].(string)
			if raw == "" {
				continue
			}
			var p remediationProposedPayload
			if json.Unmarshal([]byte(raw), &p) != nil {
				continue
			}
			if p.ReleaseID == releaseID && p.NodeID == nodeID {
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for %s for release %s node %s", streams.RemediationProposedV1, releaseID, nodeID))
}

// assertNoRemediationTriggerFor fails if any remediation trigger names the
// given run. Called with a verification run id: a trigger for one would mean
// a failed fix attempt had been handed back to the fixer as a fresh failure to
// heal, which is a loop with no exit.
func assertNoRemediationTriggerFor(t *testing.T, ctx context.Context, clients *testClients, runID string) {
	t.Helper()
	msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV2, "-", "+").Result()
	require.NoError(t, err)
	for _, msg := range msgs {
		raw, _ := msg.Values["payload"].(string)
		if raw == "" {
			continue
		}
		var p remediationRequestedPayload
		if json.Unmarshal([]byte(raw), &p) != nil {
			continue
		}
		require.NotEqual(t, runID, p.ReleaseID,
			"verification run %s must produce no remediation trigger, found one carrying %d node(s)",
			runID, len(p.Nodes))
	}
}

// assertNoNodeVersionsFor fails if the graph recorded any promoted code
// version for the given run. A verification run never promotes, so a version
// stamped with its id would mean an unapproved contract entered the
// production history the graph is the record of.
func assertNoNodeVersionsFor(t *testing.T, ctx context.Context, clients *testClients, runID string) {
	t.Helper()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)
	res, err := session.Run(ctx,
		`MATCH (v:NodeVersion {release_id: $rid}) RETURN count(v) AS n`,
		map[string]any{"rid": runID})
	require.NoError(t, err, "count :NodeVersion rows for %s", runID)
	require.True(t, res.Next(ctx), "count query returned no row")
	n, _ := res.Record().Get("n")
	count, _ := n.(int64)
	require.Zero(t, count,
		"verification run %s must write no :NodeVersion — it never promoted", runID)
}
