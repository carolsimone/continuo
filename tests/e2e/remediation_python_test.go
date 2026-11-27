package e2e

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// The remediation fixture repository: the working tree stub-github serves from
// GET /repos/{owner}/{repo}/tarball/{ref}, and the source of the ODCS contract
// each of these tests posts a release for. Both readers take the same files,
// so the contract a release is rejected on and the contract the python-validation
// type finds in the checkout cannot describe different nodes.
//
// Each scenario has a project of its own, so no two share a contract
// directory. The packaging step packs a whole directory into one bundle, so a
// second, still-broken node beside the node under repair would sink the
// release check that is verifying the repair.
//
//go:embed fixtures/py-remediation-repo
var pyRemediationRepo embed.FS

const (
	// pyRemediationRepoRoot is the embedded fixture's prefix. The paths below
	// are relative to it and are also the paths inside the served tarball,
	// which is what makes the assertions on a proposal's edited file readable.
	pyRemediationRepoRoot = "fixtures/py-remediation-repo"

	// The single-node project whose declared read cannot bind, and the file
	// that declares it. One fix attempt repairs it.
	pyBadReadProject  = "svc-py-e2e"
	pyBadReadUniqueID = "e2e_schema.py_bad_read"
	pyBadReadContract = "services/svc-py-e2e/contracts/py_bad_read.odcs.yaml"

	// The second project, whose first repair is also wrong: it takes two
	// attempts, and the second only succeeds because it is shown the first
	// one's release-check error.
	pyLoopProject  = "svc-py-e2e-loop"
	pyLoopUniqueID = "e2e_schema.py_loop_read"
	pyLoopContract = "services/svc-py-e2e-loop/contracts/py_loop_read.odcs.yaml"

	// The third project, whose declared read names its relation without a
	// schema, so the release's SQL parser rejects the node before any Job
	// runs. Its repair is made in the same contract file and proven by a
	// release check whose own parse leg is the judge.
	pyParseProject  = "svc-py-e2e-parse"
	pyParseUniqueID = "e2e_schema.py_unqualified_read"
	pyParseContract = "services/svc-py-e2e-parse/contracts/py_unqualified_read.odcs.yaml"

	// pyBindingRelation is the relation a repaired contract must read from. It
	// exists in the warehouse only because these tests create it, which is
	// what makes a release check's verdict a real measurement rather than a
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
	// pyVerifyVerdictBudget covers the same shape for a release check
	// submitted later in the same test, against a stack that is warm by
	// then — including the poll that confirms the release queue names it
	// while it runs, which resolves in the first cycle or two once the check
	// is active and does not meaningfully add to this budget.
	pyVerifyVerdictBudget = 5 * time.Minute
	// pyAttemptStartBudget covers trigger to recorded attempt: classification,
	// the agent consuming it, the repository fetch, the model call, packaging
	// via the runtime CLI, the artifact upload, and the release-check
	// submission.
	pyAttemptStartBudget = 3 * time.Minute
	// pyAttemptFinalizeBudget covers a release check's terminal verdict reaching
	// the proposal row: one tick of the runner's gate poller, 5s in this compose
	// stack.
	pyAttemptFinalizeBudget = 2 * time.Minute
	// pyRecordVisibleBudget covers a write becoming readable through another
	// component's API or stream: a submitted release appearing in the release
	// list, an emitted event reaching its stream, a classification decision
	// landing in Postgres.
	pyRecordVisibleBudget = 1 * time.Minute
)

// TestE2E_PythonValidationFailure_VerifiedFix drives the runner's
// python-validation type end to end on a single-try repair:
//
//	POST /releases (kind=python, one node whose declared read cannot bind)
//	→ the validation Job's bind check fails → release rejected
//	→ classifier emits heal.triggered:v1 (one node, node_type=python-node)
//	→ agent-remediation's runner matches it to the python-validation type: its
//	  contract_file step fetches the repository tarball at the failing commit
//	  from stub-github and finds the .odcs.yaml file declaring the node, its
//	  prompt has the model correct it, its contract_packages check packs the
//	  directory with continuo-runtime, and its release_check gate uploads
//	  the bundle and POSTs it back as a release check
//	→ the proposal parks in 'verifying' naming that release check
//	→ the release check runs the real parse, candidate-schema and validation
//	  legs and stops at 'passed' — it never promotes and is never readable as a release
//	→ the runner's gate poller finalizes the proposal to 'proposed' and emits
//	  remediation.proposed:v1
//
// It is the feature's definition of done: every stage above has to work for it
// to pass. It also asserts the two things that must NOT happen — the
// release check produced no remediation trigger of its own, and wrote no
// promoted code version — because a release check that fed the classifier
// would heal its own failed fix forever, and one that reached the graph would
// put an unapproved contract into production history.
func TestE2E_PythonValidationFailure_VerifiedFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// 22 minutes: strictly greater than the 17 this test's stage budgets sum to
	// (release 6 + attempt 3 + verify 5 + finalize 2 + event 1).
	// assertNotListedAsRelease is a plain read, not a poll, so it adds no
	// budget of its own; the release-queue check shares the release check
	// stage's budget rather than stacking one on top of it.
	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// The relation a correct fix reads from has to exist before the
	// release check bind-checks it. Both cleanups are deferred, so
	// they run when the test function returns, while the pools are open.
	dropRelation := ensureBindingRelation(t, ctx, clients)
	defer dropRelation()

	// Release ids are kept short on purpose: the candidate schema of a
	// release check is "_candidate_" plus its id, and a release check id embeds
	// the failing release id, the node id, and the attempt number.
	releaseID := "pyfix-" + uuid.NewString()[:8]
	t.Logf("release_id=%s project=%s node=%s", releaseID, pyBadReadProject, pyBadReadUniqueID)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	proposedTap := startStreamTap(t, ctx, clients.redisClient, streams.RemediationProposedV1)
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.HealTriggeredV1)

	clearProd := rejectPythonFixtureRelease(t, ctx, clients, pyBadReadProject, releaseID)
	defer clearProd()

	// 1. The runner's try produced an attempt and parked it awaiting its
	//    release check. The check id is read off the row rather than
	//    re-derived here, so the naming rule is asserted as the system's, not
	//    the test's.
	verifying := waitForProposal(t, ctx, clients, releaseID, pyBadReadUniqueID, 1, "verifying", pyAttemptStartBudget)
	checkID := verifying.ReleaseCheckID
	require.NotEmpty(t, checkID, "a verifying proposal must name the release check that is judging it")
	require.True(t, strings.HasPrefix(checkID, "verify-"),
		"a release check id must be recognisable as one in every log line and UI row; got %q", checkID)
	require.Contains(t, checkID, releaseID,
		"a release check id must name the release it is repairing; got %q", checkID)
	require.True(t, strings.HasSuffix(checkID, "-a1"),
		"the first attempt's release check id must name attempt 1; got %q", checkID)
	t.Logf("proposal attempt 1 is verifying under release check %s", checkID)

	// 2. That release check is not a release: it 404s at GET /releases/{id}
	//    and is absent from GET /releases. The release queue names it while it
	//    is active.
	assertNotListedAsRelease(t, ctx, clients, checkID)
	assertReleaseQueueNamedFix(t, ctx, clients, checkID, pyVerifyVerdictBudget)

	// 3. It runs the real legs and stops at passed.
	waitForReleaseCheckStatus(t, ctx, clients, checkID, "passed", pyVerifyVerdictBudget)

	// 4. The gate poller turns the verified attempt into a reviewable fix.
	proposed := waitForProposal(t, ctx, clients, releaseID, pyBadReadUniqueID, 1, "proposed", pyAttemptFinalizeBudget)
	require.Empty(t, proposed.VerifyError, "a verified fix records no release check error")
	require.Equal(t, checkID, proposed.ReleaseCheckID,
		"finalizing an attempt must not change which release check verified it")

	releaseChecks := decodeReleaseChecks(t, proposed.ReleaseChecks)
	require.Len(t, releaseChecks, 1,
		"the fix edits one project, so exactly one release check judged it")
	require.Equal(t, pyBadReadProject, releaseChecks[0].Project,
		"the release check must name the project whose files the attempt edited")
	require.Equal(t, checkID, releaseChecks[0].CheckID,
		"the release check must name the same check the row parked on")

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
	require.Contains(t, content, "id: "+pyBadReadUniqueID,
		"the fix must keep declaring the same node, not replace it with a different one")

	// 5. The fix is announced for human review.
	waitForRemediationProposed(t, ctx, proposedTap, releaseID, pyBadReadUniqueID, pyRecordVisibleBudget)

	// 6. Nothing from the release check reached production. The proof that
	//    the no-promote gate held is the check reaching "passed" above rather
	//    than "promoted" — a status only a candidate release can reach; these
	//    two checks confirm the consequences of that, and would also catch a
	//    promotion that took some other route.
	var prodRows int
	require.NoError(t, clients.releaseDB.GetContext(ctx, &prodRows,
		`SELECT count(*) FROM service_prod WHERE project_name = $1`, pyBadReadProject))
	require.Zero(t, prodRows, "a release check must never write the project's production pointer")
	assertNoNodeVersionsFor(t, ctx, clients, checkID)

	// 7. No remediation trigger names the release check. This scan cannot
	//    fail here — a passed check emits no rejection, so no classifier path
	//    could produce one — and it is kept as a cheap guard against a future
	//    trigger source, not as evidence. The anti-loop rule is proved where it
	//    can actually be violated: the sibling test below, whose first
	//    attempt's release check genuinely fails.
	assertNoRemediationTriggerFor(t, requestedTap, checkID)

	t.Log("✅ a rejected python node was repaired, verified by a real release check, and proposed for review")
}

// TestE2E_PythonValidationFailure_ReleaseCheckErrorFeedsNextAttempt drives the
// same type when the first repair is wrong, which is the property that makes
// the python-validation type a loop rather than a single shot: the next try is
// shown, through the previous_tries step, what the earlier try changed and the
// error its release check reported back.
//
// The canned model answers the first attempt with a read that still cannot
// bind, and answers a retry with the binding one — recognising a retry by the
// prompt's earlier-attempts section BOTH existing AND naming the relation the
// rejected attempt declared, read from the part of the prompt that is not the
// contract file itself. That relation name reaches the prompt only through the
// first attempt's recorded release check error or the diff it applied. Attempt 2
// reaching 'proposed' is therefore proof that the failed attempt's evidence was
// assembled AND shown, not merely that an attempt row existed; a retry whose
// prompt lost that evidence gets the answer that already failed, and this test
// never goes green.
//
// It also proves the loop cannot eat itself: a failed release check
// reaches no classifier at all — no decision row, no remediation trigger —
// which is what stops a failed fix from remediating itself.
func TestE2E_PythonValidationFailure_ReleaseCheckErrorFeedsNextAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// 34 minutes: strictly greater than the 26 this test's stage budgets sum to
	// (release 6 + attempt 3 + verify 5 + finalize 2, then attempt 3 + verify 5
	// + finalize 2). assertNotListedAsRelease and the classifier reads below
	// are plain queries, not polls, so neither adds a budget of its own; the
	// release-queue check shares the release check stage's budget rather than
	// stacking one on top of it.
	ctx, cancel := context.WithTimeout(context.Background(), 34*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	dropRelation := ensureBindingRelation(t, ctx, clients)
	defer dropRelation()

	releaseID := "pyloop-" + uuid.NewString()[:8]
	t.Logf("release_id=%s project=%s node=%s", releaseID, pyLoopProject, pyLoopUniqueID)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.HealTriggeredV1)

	clearProd := rejectPythonFixtureRelease(t, ctx, clients, pyLoopProject, releaseID)
	defer clearProd()

	// 1. Attempt 1's release check fails the fix, and its error is
	//    recorded on the attempt as the evidence attempt 2 will be shown.
	firstVerifying := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 1, "verifying", pyAttemptStartBudget)
	firstCheckID := firstVerifying.ReleaseCheckID
	require.NotEmpty(t, firstCheckID)
	require.True(t, strings.HasSuffix(firstCheckID, "-a1"),
		"the first attempt's release check id must name attempt 1; got %q", firstCheckID)
	assertNotListedAsRelease(t, ctx, clients, firstCheckID)
	assertReleaseQueueNamedFix(t, ctx, clients, firstCheckID, pyVerifyVerdictBudget)
	waitForReleaseCheckStatus(t, ctx, clients, firstCheckID, "failed", pyVerifyVerdictBudget)

	firstFailed := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 1, "failed", pyAttemptFinalizeBudget)
	require.NotEmpty(t, firstFailed.VerifyError,
		"a failed attempt must record why, or the next attempt has nothing new to learn from")
	require.NotContains(t, firstFailed.VerifyError, "timed out",
		"the recorded reason must be the release check's own verdict, not a wait that expired")
	require.Contains(t, firstFailed.VerifyError, "still_wrong_name",
		"the recorded reason must be the error the release check reported for this node; got %q",
		firstFailed.VerifyError)
	t.Logf("attempt 1 failed its release check: %s", firstFailed.VerifyError)

	// 2. Attempt 2 runs against a second release check and this one passes.
	secondVerifying := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 2, "verifying", pyAttemptStartBudget)
	secondCheckID := secondVerifying.ReleaseCheckID
	require.NotEmpty(t, secondCheckID)
	require.NotEqual(t, firstCheckID, secondCheckID,
		"each attempt must be verified by its own release check, or one verdict would answer for two fixes")
	require.True(t, strings.HasSuffix(secondCheckID, "-a2"),
		"the second attempt's release check id must name attempt 2; got %q", secondCheckID)
	assertNotListedAsRelease(t, ctx, clients, secondCheckID)
	waitForReleaseCheckStatus(t, ctx, clients, secondCheckID, "passed", pyVerifyVerdictBudget)

	secondProposed := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 2, "proposed", pyAttemptFinalizeBudget)
	edits := decodeFileEdits(t, secondProposed.FileEdits)
	require.Len(t, edits, 1)
	require.Equal(t, pyLoopContract, edits[0].Path)

	content := string(getS3ObjectByKey(t, ctx, clients, stripS3Prefix(edits[0].ContentURI)))
	require.Contains(t, content, "select id from "+pyBindingRelation,
		"the second attempt must declare a read against a relation that exists")
	require.NotContains(t, content, "select id from "+pyStillBrokenRelation,
		"the second attempt must not repeat the read the first attempt failed for")
	require.Contains(t, content, "id: "+pyLoopUniqueID,
		"the fix must keep declaring the same node, not replace it with a different one")

	// 3. The failed release check reached no classifier: no decision row
	//    was written for it and no remediation trigger names it, which is
	//    what stops a failed fix from remediating itself.
	var n int
	require.NoError(t, clients.remediationDB.GetContext(ctx, &n,
		`SELECT count(*) FROM classification_decision WHERE release_id = $1`, firstCheckID))
	require.Zero(t, n, "a release check's failure must never reach the classifier")
	msgs := requestedTap.Entries()
	for _, m := range msgs {
		require.NotContains(t, m.Values["payload"], firstCheckID, "no remediation trigger may name a release check")
	}
	assertNoNodeVersionsFor(t, ctx, clients, firstCheckID)
	assertNoNodeVersionsFor(t, ctx, clients, secondCheckID)

	t.Log("✅ a rejected fix's release check error fed the next attempt, which was verified and proposed")
}

// rejectPythonFixtureRelease posts one of the remediation fixture repository's
// projects as a Python release and waits for it to be rejected. The
// bundle it uploads is built from the same files the served repository
// checkout holds, so the node the release fails on is the node the
// python-validation type will later find in the checkout.
//
// It clears both python fixture projects' production pointers before posting
// and returns a cleanup that clears them again — a leftover pointer would drag
// this fixture's contract into every later test's assembled manifest set. The
// cleanup is returned rather than registered with t.Cleanup because it needs
// the release-controller pool, and t.Cleanup functions run after every
// deferred call, including the one that closes every pool.
func rejectPythonFixtureRelease(
	t *testing.T, ctx context.Context, clients *testClients,
	project, releaseID string,
) func() {
	t.Helper()

	// Baseline: every dbt project keeps its live pointer; the python fixture
	// project is unknown to production, so its single node is a changed node.
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	var prodNodes []map[string]string
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "project": svc, "content_hash": n.contentHash,
			})
		}
	}

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allProjects)
	clearPythonServiceProd(t, ctx, clients)
	cleanup := func() { clearPythonServiceProd(t, context.Background(), clients) }

	putODCSBundle(t, ctx, clients, project, releaseID, pyRemediationBundle(t, ctx, project))
	postPythonRelease(t, clients, project, releaseID, pyFixtureImage, false)

	waitForReleaseRejected(t, ctx, clients, releaseID, pyReleaseVerdictBudget)
	return cleanup
}

// pyRemediationBundle builds the odcs-bundle.json a python project's CI
// uploads before POST /releases, from the contracts and scripts of the project's
// directory in the fixture repository, the repository root its contracts name
// their scripts from, and the package list of the fixture image the release
// names.
//
// The bundle is built by buildODCSBundle rather than by `continuo-runtime
// pack`, because the parse scenario's contract declares a read the runtime
// refuses (a table without its schema): that read must reach the release for
// the release to reject it.
func pyRemediationBundle(t *testing.T, ctx context.Context, project string) []byte {
	t.Helper()
	root := path.Join(pyRemediationRepoRoot, "services", project)
	return buildODCSBundle(t, pyRemediationRepo, root, project, imagePackages(t, ctx, pyFixtureImage),
		odcsContracts(t, pyRemediationRepo, root))
}

// ensureBindingRelation creates the relation a repaired contract reads from
// and returns a cleanup that drops it. It lives outside the node registry, so
// the candidate-schema rewriter passes it through verbatim and the validation
// Job's bind check resolves it against the real warehouse — which is exactly
// why it has to exist for a release check to pass.
//
// The cleanup is returned for the caller to defer, so it runs when the test
// function returns, while the warehouse pool is open: the cleanup setupClients
// registered closes the pools only after every deferred call.
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
// fixture projects. Neither is ever meant to reach production in these tests,
// so a row for either is stale state from an earlier run and would be read
// back during manifest assembly.
func clearPythonServiceProd(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	for _, svc := range []string{pyBadReadProject, pyLoopProject} {
		if _, err := clients.releaseDB.ExecContext(ctx,
			`DELETE FROM service_prod WHERE project_name = $1`, svc); err != nil {
			t.Errorf("clear service_prod row for %s: %v", svc, err)
		}
	}
}

// pyProposalRow captures the release-check-related fields of a proposal row.
type pyProposalRow struct {
	Status         string `db:"status"`
	Attempt        int    `db:"attempt"`
	ReleaseCheckID string `db:"release_check_id"`
	VerifyError    string `db:"verify_error"`
	FilePath       string `db:"file_path"`
	FileEdits      []byte `db:"file_edits"`
	ReleaseChecks  []byte `db:"release_checks"`
}

// waitForProposal polls the agent-remediation's proposal table until the named
// attempt for a node reaches want, and returns the row. Every status change it
// observes is logged, so an attempt that stalls in 'generating' or 'verifying' is
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
			SELECT status, attempt, release_check_id, verify_error, file_path, file_edits, release_checks
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

// pyReleaseCheck mirrors one element of the proposal's release_checks column:
// the project whose edits a release check judged, the manifest kind it was
// submitted under, that check's id, and its last-read phase. An attempt posts
// one per edited project, so the list is how a reader tells which verdict
// covered what.
type pyReleaseCheck struct {
	Project string `json:"project"`
	Kind    string `json:"kind"`
	CheckID string `json:"check_id"`
	Phase   string `json:"phase"`
}

// decodeReleaseChecks reads the proposal row's release_checks column.
func decodeReleaseChecks(t *testing.T, raw []byte) []pyReleaseCheck {
	t.Helper()
	var releaseChecks []pyReleaseCheck
	require.NoError(t, json.Unmarshal(raw, &releaseChecks), "decode proposal release_checks: %s", raw)
	return releaseChecks
}

// waitForReleaseCheckStatus polls GET /release-checks/{id} until the
// check reaches want ("passed" or "failed"); either is the expected result of one
// test or the other, so no status is treated as a failure on the way.
func waitForReleaseCheckStatus(
	t *testing.T, ctx context.Context, clients *testClients,
	checkID, want string, timeout time.Duration,
) {
	t.Helper()
	var last string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		resp, err := http.Get(fmt.Sprintf("%s/release-checks/%s", clients.releaseBase, checkID))
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
			t.Logf("release check %s: status=%s fail_reason=%q", checkID, r.Status, r.FailReason)
			last = r.Status
		}
		return r.Status == want, nil
	}, fmt.Sprintf("timeout waiting for release check %s to reach %q", checkID, want))
}

// assertNotListedAsRelease asserts a release check has no release
// identity: GET /releases/{id} answers 404 and the release list omits it.
func assertNotListedAsRelease(t *testing.T, ctx context.Context, clients *testClients, checkID string) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/releases/%s", clients.releaseBase, checkID))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode, "a release check must not be readable as a release")

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
		require.NotEqual(t, checkID, rel.ReleaseID, "a release check must not appear in the release list")
	}
}

// assertReleaseQueueNamedFix waits until GET /release-queue names checkID as
// the active release check, of kind fix — the read the Releases tab's
// in-flight strip uses — or until the check has already ended, which a fast
// check may do before the first poll lands.
func assertReleaseQueueNamedFix(t *testing.T, ctx context.Context, clients *testClients, checkID string, timeout time.Duration) {
	t.Helper()
	pollUntil(t, ctx, timeout, time.Second, func() (bool, error) {
		resp, err := http.Get(clients.releaseBase + "/release-queue")
		if err != nil {
			return false, nil
		}
		var q struct {
			Active *struct {
				CheckID   string `json:"check_id"`
				CheckKind string `json:"check_kind"`
			} `json:"active"`
		}
		err = json.NewDecoder(resp.Body).Decode(&q)
		resp.Body.Close()
		if err != nil {
			return false, nil
		}
		if q.Active != nil && q.Active.CheckID == checkID {
			require.Equal(t, "fix", q.Active.CheckKind)
			return true, nil
		}
		resp, err = http.Get(fmt.Sprintf("%s/release-checks/%s", clients.releaseBase, checkID))
		if err != nil {
			return false, nil
		}
		var r struct {
			Status string `json:"status"`
		}
		err = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		return err == nil && (r.Status == "passed" || r.Status == "failed"), nil
	}, fmt.Sprintf("timeout waiting for the release queue to name release check %s", checkID))
}

// waitForRemediationProposed polls remediation.proposed:v1 for the fix a
// verified attempt announces. The runner's gate poller writes the row and
// enqueues this event in one transaction, so its arrival is what proves the announcement was
// not lost between the two.
func waitForRemediationProposed(
	t *testing.T, ctx context.Context, proposedTap *streamTap, releaseID, nodeID string, timeout time.Duration,
) {
	t.Helper()
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		msgs := proposedTap.Entries()
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
// given release check: a trigger for one would mean
// a failed fix attempt had been handed back to the runner as a fresh failure to
// heal, which is a loop with no exit.
func assertNoRemediationTriggerFor(t *testing.T, requestedTap *streamTap, checkID string) {
	t.Helper()
	for _, p := range requestedTap.Triggers(t) {
		require.NotEqual(t, checkID, p.ReleaseID,
			"release check %s must produce no remediation trigger, found one carrying %d node(s)",
			checkID, len(p.Nodes))
	}
}

// assertNoNodeVersionsFor fails if the graph recorded any promoted code
// version for the given release check. A release check never promotes, so a version
// stamped with its id would mean an unapproved contract entered the
// production history the graph is the record of.
func assertNoNodeVersionsFor(t *testing.T, ctx context.Context, clients *testClients, checkID string) {
	t.Helper()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)
	res, err := session.Run(ctx,
		`MATCH (v:NodeVersion {tenant_id: 'default', release_id: $rid}) RETURN count(v) AS n`,
		map[string]any{"rid": checkID})
	require.NoError(t, err, "count :NodeVersion rows for %s", checkID)
	require.True(t, res.Next(ctx), "count query returned no row")
	n, _ := res.Record().Get("n")
	count, _ := n.(int64)
	require.Zero(t, count,
		"release check %s must write no :NodeVersion — it never promoted", checkID)
}

// TestE2E_PythonParseFailure_VerifiedFix drives the runner's python-parse type
// from the parse stage, the leg that runs before any Job:
//
//	POST /releases (kind=python, one node whose declared read is not schema-qualified)
//	→ topology-controller's SQL parser rejects the node (unqualified_reference)
//	→ release-controller: Fail(unqualified_reference), RecordStageResults("parse")
//	→ release.rejected:v1 {stage: parse, per_node: [{kind, detail, file_path,
//	  project, node_type=python-node}]} — the parser's own text is inline;
//	  there is no dbt log because nothing ran
//	→ classifier emits heal.triggered:v1 (source=parse, node_type=python-node)
//	→ agent-remediation's runner matches it to the python-parse type: the
//	  trigger's file_path names the node's contract file by its path in the
//	  project's bundle, not in the repository, so the contract_file step finds
//	  the .odcs.yaml file from the node id alone, the prompt has the model
//	  qualify the read's relation, and the contract_packages check and the
//	  release_check gate pack the directory and POST it as a fix's release
//	  check
//	→ the release check's own parse leg accepts the repaired SQL, its validation leg binds
//	  the now-qualified read, and it stops at 'passed'
//	→ the runner's gate poller finalizes the proposal to 'proposed'
//
// The validation-stage sibling above proves the python-validation type; this
// test proves the parse stage reaches the python-parse type for a python node,
// with a rejection that carries no log and no code bundle.
func TestE2E_PythonParseFailure_VerifiedFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// 22 minutes: strictly greater than the 17 this test's stage budgets sum to
	// (release 6 + attempt 3 + verify 5 + finalize 2 + event 1).
	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// The qualified read the fix declares must bind in the release check's
	// validation leg, so the relation has to exist first.
	dropRelation := ensureBindingRelation(t, ctx, clients)
	defer dropRelation()

	releaseID := "pyparse-" + uuid.NewString()[:8]
	t.Logf("release_id=%s project=%s node=%s", releaseID, pyParseProject, pyParseUniqueID)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	rejectedTap := startStreamTap(t, ctx, clients.redisClient, streams.ReleaseRejectedV1)
	proposedTap := startStreamTap(t, ctx, clients.redisClient, streams.RemediationProposedV1)

	clearProd := rejectPythonFixtureRelease(t, ctx, clients, pyParseProject, releaseID)
	defer clearProd()

	// 1. The rejection is a parse-stage rejection naming the python node, with
	//    the parser's detail inline and the node's kind on the entry — which is
	//    what matches it to the python-parse type before any read.
	assertRejectedPythonParseStage(t, ctx, rejectedTap, releaseID)

	// 2. The runner's try produced an attempt and parked it on a release check.
	verifying := waitForProposal(t, ctx, clients, releaseID, pyParseUniqueID, 1, "verifying", pyAttemptStartBudget)
	checkID := verifying.ReleaseCheckID
	require.NotEmpty(t, checkID, "a verifying proposal must name the release check that is judging it")
	require.True(t, strings.HasSuffix(checkID, "-a1"),
		"the first attempt's release check id must name attempt 1; got %q", checkID)
	t.Logf("proposal attempt 1 is verifying under release check %s", checkID)

	// 3. The release check's own parse leg is the judge of a parse fix: it re-parses the
	//    packed contract, then validates, and stops at passed.
	assertNotListedAsRelease(t, ctx, clients, checkID)
	waitForReleaseCheckStatus(t, ctx, clients, checkID, "passed", pyVerifyVerdictBudget)

	// 4. The gate poller turns the verified attempt into a reviewable fix.
	proposed := waitForProposal(t, ctx, clients, releaseID, pyParseUniqueID, 1, "proposed", pyAttemptFinalizeBudget)
	require.Empty(t, proposed.VerifyError, "a verified fix records no release check error")

	releaseChecks := decodeReleaseChecks(t, proposed.ReleaseChecks)
	require.Len(t, releaseChecks, 1, "the fix edits one project, so exactly one release check judged it")
	require.Equal(t, pyParseProject, releaseChecks[0].Project)

	edits := decodeFileEdits(t, proposed.FileEdits)
	require.Len(t, edits, 1, "the fix changes exactly the one file that declares the node")
	require.Equal(t, pyParseContract, edits[0].Path,
		"the edit must land in the declaring contract file, the one the node's id is declared in")

	content := string(getS3ObjectByKey(t, ctx, clients, stripS3Prefix(edits[0].ContentURI)))
	require.Contains(t, content, "select id from "+pyBindingRelation,
		"the proposed contract must declare the read with its relation schema-qualified")
	require.NotContains(t, content, "select id from right_name\n",
		"the unqualified read must no longer be declared")
	require.Contains(t, content, "id: "+pyParseUniqueID,
		"the fix must keep declaring the same node, not replace it with a different one")

	// 5. The fix is announced for human review, and nothing reached production.
	waitForRemediationProposed(t, ctx, proposedTap, releaseID, pyParseUniqueID, pyRecordVisibleBudget)
	var prodRows int
	require.NoError(t, clients.releaseDB.GetContext(ctx, &prodRows,
		`SELECT count(*) FROM service_prod WHERE project_name = $1`, pyParseProject))
	require.Zero(t, prodRows, "a release check must never write the project's production pointer")

	t.Log("✅ a python node rejected at the parse stage was repaired in its contract, verified by a real release check, and proposed for review")
}

// assertRejectedPythonParseStage reads the release's rejection off
// release.rejected:v1 and pins the shape the python-parse type depends on:
// stage parse, reason unqualified_reference, one per-node entry carrying the
// parser's detail inline, the contract file as file_path (its path in the
// project's bundle, where the rejected SQL is declared), the project, and
// node_type=python-node.
func assertRejectedPythonParseStage(t *testing.T, ctx context.Context, rejectedTap *streamTap, releaseID string) {
	t.Helper()
	pollUntil(t, ctx, pyRecordVisibleBudget, 2*time.Second, func() (bool, error) {
		msgs := rejectedTap.Entries()
		for _, msg := range msgs {
			raw, _ := msg.Values["payload"].(string)
			var p struct {
				ReleaseID     string `json:"release_id"`
				Stage         string `json:"stage"`
				Reason        string `json:"reason"`
				Repo          string `json:"repo"`
				CommitSHA     string `json:"commit_sha"`
				CodeBundleURI string `json:"code_bundle_uri"`
				PerNode       []struct {
					NodeID   string `json:"node_id"`
					Kind     string `json:"kind"`
					Detail   string `json:"detail"`
					FilePath string `json:"file_path"`
					Project  string `json:"project"`
					NodeType string `json:"node_type"`
				} `json:"per_node"`
			}
			if raw == "" || json.Unmarshal([]byte(raw), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			require.Equal(t, "parse", p.Stage)
			require.Equal(t, "unqualified_reference", p.Reason)
			require.NotEmpty(t, p.Repo)
			require.Equal(t, releaseID, p.CommitSHA, "the python fixture posts its release id as the commit sha")
			require.Empty(t, p.CodeBundleURI,
				"a parse rejection precedes the parse that produces the code bundle, so the python-parse type must work without one")
			require.Len(t, p.PerNode, 1)
			require.Equal(t, pyParseUniqueID, p.PerNode[0].NodeID)
			require.Equal(t, "unqualified_reference", p.PerNode[0].Kind)
			require.Contains(t, p.PerNode[0].Detail, "right_name")
			require.Equal(t, path.Join("contracts", path.Base(pyParseContract)), p.PerNode[0].FilePath,
				"a Python node's parse failure names its contract file, by its path in the project's bundle")
			require.Equal(t, pyParseProject, p.PerNode[0].Project)
			require.Equal(t, "python-node", p.PerNode[0].NodeType)
			return true, nil
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for release.rejected:v1 for release %s", releaseID))
}
