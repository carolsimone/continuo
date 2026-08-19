package e2e

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
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
// sink the shadow release that is verifying the repair.
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
	// one's shadow-release error.
	pyLoopService  = "svc-py-e2e-loop"
	pyLoopUniqueID = "e2e_schema.py_loop_read"
	pyLoopContract = "services/svc-py-e2e-loop/contracts/py_loop_read.yml"
	pyLoopScript   = "services/svc-py-e2e-loop/scripts/py_loop_read.py"

	// pyBindingRelation is the relation a repaired contract must read from. It
	// exists in the warehouse only because these tests create it, which is
	// what makes a shadow release's verdict a real measurement rather than a
	// foregone conclusion.
	pyBindingRelation = "public.right_name"

	// pyBrokenRelation and pyLoopBrokenRelation are what the two fixtures
	// declare before repair; pyStillBrokenRelation is the canned model's first,
	// still-wrong answer for the loop fixture. None of the three exists.
	pyBrokenRelation      = "public.wrong_name"
	pyLoopBrokenRelation  = "public.loop_wrong_name"
	pyStillBrokenRelation = "public.still_wrong_name"
)

// TestE2E_PythonValidationFailure_ShadowVerifiedFix drives the whole python
// remediation lane on a single-attempt repair:
//
//	POST /releases (kind=python, one node whose declared read cannot bind)
//	→ the validation Job's bind check fails → release rejected
//	→ classifier emits remediation.requested:v1 (node_type=python-model)
//	→ remediation-agent routes it to the python contract fixer: fetches the
//	  repository tarball at the failing commit from stub-github, finds the yaml
//	  declaring the node, has the model correct it, packages the directory with
//	  continuo-runtime, uploads it and POSTs it back as a shadow release
//	→ the proposal parks in 'verifying' naming that shadow release
//	→ the shadow release runs the real parse + candidate-schema + validation
//	  pipeline and stops at 'validated' — it never promotes
//	→ the shadow-verify reconciler finalizes the proposal to 'proposed' and
//	  emits remediation.proposed:v1
//
// It is the feature's definition of done: every stage above has to work for it
// to pass. It also asserts the two things that must NOT happen — the shadow
// release produced no remediation trigger of its own, and wrote no promoted
// code version — because a shadow release that fed the classifier would heal
// its own failed fix forever, and one that reached the graph would put an
// unapproved contract into production history.
func TestE2E_PythonValidationFailure_ShadowVerifiedFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// The relation a correct fix reads from has to exist before the shadow
	// release bind-checks it.
	ensureBindingRelation(t, ctx, clients)

	// Release ids are kept short on purpose: the candidate schema of a shadow
	// release is "_candidate_" plus its id, and a shadow id embeds the failing
	// release id, the node id, and the attempt number.
	releaseID := "pyfix-" + uuid.NewString()[:8]
	t.Logf("release_id=%s service=%s node=%s", releaseID, pyBadReadService, pyBadReadUniqueID)

	rejectPythonFixtureRelease(t, ctx, clients, pyBadReadService, pyBadReadContract, pyBadReadScript, releaseID)

	// 1. The fixer produced an attempt and parked it awaiting its shadow
	//    release. The shadow id is read off the row rather than re-derived
	//    here, so the naming rule is asserted as the system's, not the test's.
	verifying := waitForProposal(t, ctx, clients, releaseID, pyBadReadUniqueID, 1, "verifying", 6*time.Minute)
	shadowID := verifying.ShadowReleaseID
	require.NotEmpty(t, shadowID, "a verifying proposal must name the release that is judging it")
	require.True(t, strings.HasPrefix(shadowID, "shadow-"),
		"a shadow release id must be recognisable as one in every log line and UI row; got %q", shadowID)
	require.Contains(t, shadowID, releaseID,
		"a shadow release id must name the release it is repairing; got %q", shadowID)
	require.True(t, strings.HasSuffix(shadowID, "-a1"),
		"the first attempt's shadow release id must name attempt 1; got %q", shadowID)
	t.Logf("proposal attempt 1 is verifying under shadow release %s", shadowID)

	// 2. That release is visible to an operator, flagged as a shadow.
	assertReleaseListedAsShadow(t, ctx, clients, shadowID)

	// 3. It runs the real pipeline and stops at validated.
	waitForReleaseStatus(t, ctx, clients, shadowID, "validated", 12*time.Minute)

	// 4. The reconciler turns the verified attempt into a reviewable fix.
	proposed := waitForProposal(t, ctx, clients, releaseID, pyBadReadUniqueID, 1, "proposed", 3*time.Minute)
	require.Empty(t, proposed.VerifyError, "a verified fix records no verification error")
	require.Equal(t, shadowID, proposed.ShadowReleaseID,
		"finalizing an attempt must not change which release verified it")

	edits := decodeFileEdits(t, proposed.FileEdits)
	require.Len(t, edits, 1, "the fix changes exactly the one file that declares the node")
	require.Equal(t, pyBadReadContract, edits[0].Path,
		"the edit must name the declaring contract file as the repository holds it")

	content := string(getS3ObjectByKey(t, ctx, clients, stripS3Prefix(edits[0].ContentURI)))
	require.Contains(t, content, pyBindingRelation,
		"the proposed contract must read from a relation that exists")
	require.NotContains(t, content, pyBrokenRelation,
		"the proposed contract must no longer read from the relation that failed to bind")
	require.Contains(t, content, "table: py_bad_read",
		"the fix must keep declaring the same node, not replace it with a different one")

	// 5. The fix is announced for human review.
	waitForRemediationProposed(t, ctx, clients, releaseID, pyBadReadUniqueID, 2*time.Minute)

	// 6. Nothing shadow reached production. The pointer for the service is
	//    still absent (the failing release did not promote and neither did the
	//    shadow), and no promoted code version was ever recorded for it.
	var prodRows int
	require.NoError(t, clients.releaseDB.GetContext(ctx, &prodRows,
		`SELECT count(*) FROM service_prod WHERE service_name = $1`, pyBadReadService))
	require.Zero(t, prodRows, "a shadow release must never write the service's production pointer")
	assertNoNodeVersionsFor(t, ctx, clients, shadowID)

	// 7. No remediation trigger was ever produced for the shadow release. This
	//    is checked after the proposal is terminal, so the classifier has long
	//    since seen everything this release produced.
	assertNoRemediationTriggerFor(t, ctx, clients, shadowID)

	t.Log("✅ a rejected python node was repaired, verified by a real shadow release, and proposed for review")
}

// TestE2E_PythonValidationFailure_ShadowErrorFeedsNextAttempt drives the same
// lane when the first repair is wrong, which is the property that makes the
// python lane a loop rather than a single shot: attempt n+1 is shown what
// attempt n changed and the error its shadow release reported back.
//
// The canned model answers the first attempt with a read that still cannot
// bind, and answers a retry — recognised solely by the prior-attempts section
// of the prompt — with the binding one. So attempt 2 reaching 'proposed' is
// itself proof that the failed attempt's evidence was assembled and shown: a
// retry whose prompt lost that section gets the answer that already failed,
// and this test never goes green.
//
// It also proves the loop cannot eat itself: the rejected shadow release is
// recorded as a dropped classification and produces no remediation trigger.
func TestE2E_PythonValidationFailure_ShadowErrorFeedsNextAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	ensureBindingRelation(t, ctx, clients)

	releaseID := "pyloop-" + uuid.NewString()[:8]
	t.Logf("release_id=%s service=%s node=%s", releaseID, pyLoopService, pyLoopUniqueID)

	rejectPythonFixtureRelease(t, ctx, clients, pyLoopService, pyLoopContract, pyLoopScript, releaseID)

	// 1. Attempt 1's shadow release rejects the fix, and its error is recorded
	//    on the attempt as the evidence attempt 2 will be shown.
	firstVerifying := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 1, "verifying", 6*time.Minute)
	firstShadowID := firstVerifying.ShadowReleaseID
	require.NotEmpty(t, firstShadowID)
	require.True(t, strings.HasSuffix(firstShadowID, "-a1"),
		"the first attempt's shadow release id must name attempt 1; got %q", firstShadowID)
	assertReleaseListedAsShadow(t, ctx, clients, firstShadowID)
	waitForReleaseStatus(t, ctx, clients, firstShadowID, "rejected", 12*time.Minute)

	firstFailed := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 1, "failed", 3*time.Minute)
	require.NotEmpty(t, firstFailed.VerifyError,
		"a rejected attempt must record why, or the next attempt has nothing new to learn from")
	require.NotContains(t, firstFailed.VerifyError, "timed out",
		"the recorded reason must be the shadow release's own verdict, not a wait that expired")
	require.Contains(t, firstFailed.VerifyError, "still_wrong_name",
		"the recorded reason must be the error the shadow release reported for this node; got %q",
		firstFailed.VerifyError)
	t.Logf("attempt 1 failed verification: %s", firstFailed.VerifyError)

	// 2. Attempt 2 runs against a second shadow release and this one validates.
	secondVerifying := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 2, "verifying", 6*time.Minute)
	secondShadowID := secondVerifying.ShadowReleaseID
	require.NotEmpty(t, secondShadowID)
	require.NotEqual(t, firstShadowID, secondShadowID,
		"each attempt must be verified by its own release, or one verdict would answer for two fixes")
	require.True(t, strings.HasSuffix(secondShadowID, "-a2"),
		"the second attempt's shadow release id must name attempt 2; got %q", secondShadowID)
	assertReleaseListedAsShadow(t, ctx, clients, secondShadowID)
	waitForReleaseStatus(t, ctx, clients, secondShadowID, "validated", 12*time.Minute)

	secondProposed := waitForProposal(t, ctx, clients, releaseID, pyLoopUniqueID, 2, "proposed", 3*time.Minute)
	edits := decodeFileEdits(t, secondProposed.FileEdits)
	require.Len(t, edits, 1)
	require.Equal(t, pyLoopContract, edits[0].Path)

	content := string(getS3ObjectByKey(t, ctx, clients, stripS3Prefix(edits[0].ContentURI)))
	require.Contains(t, content, pyBindingRelation,
		"the second attempt must read from a relation that exists")
	require.NotContains(t, content, pyStillBrokenRelation,
		"the second attempt must not repeat the read the first attempt was rejected for")

	// 3. The rejected shadow release fed the classifier nothing. Its decision
	//    is recorded (no drop is invisible) but nothing was emitted, which is
	//    what stops a failed fix from remediating itself.
	var decision classificationDecisionRow
	pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		err := clients.remediationDB.GetContext(ctx, &decision,
			`SELECT source, release_id, node_id, category, decision
			   FROM classification_decision
			  WHERE source = 'validation' AND release_id = $1 AND node_id = $2`,
			firstShadowID, pyLoopUniqueID)
		return err == nil, nil
	}, fmt.Sprintf("timeout waiting for the classification decision recorded for shadow release %s", firstShadowID))
	require.Equal(t, "drop", decision.Decision,
		"a shadow rejection must be dropped, not healed")
	assertNoRemediationTriggerFor(t, ctx, clients, firstShadowID)
	assertNoNodeVersionsFor(t, ctx, clients, firstShadowID)
	assertNoNodeVersionsFor(t, ctx, clients, secondShadowID)

	t.Log("✅ a rejected fix's shadow error fed the next attempt, which was verified and proposed")
}

// rejectPythonFixtureRelease posts one of the remediation fixture repository's
// services as a python release and waits for validation to reject it. The
// contract it uploads is built from the same file the served repository
// checkout holds, so the node the release fails on is the node the fixer will
// later find in the checkout.
//
// It clears both python services' production pointers before posting and
// registers a cleanup that clears them again: a leftover pointer would drag
// this fixture's contract into every later test's assembled manifest set. The
// cleanup is registered with t.Cleanup rather than returned because nothing in
// these tests promotes, so no pointer is expected to exist by the end — the
// cleanup is a backstop, not part of the flow.
func rejectPythonFixtureRelease(
	t *testing.T, ctx context.Context, clients *testClients,
	service, contractPath, scriptPath, releaseID string,
) {
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
	t.Cleanup(func() { clearPythonServiceProd(t, context.Background(), clients) })

	contractKey := fmt.Sprintf("%s/%s/contract.yaml", service, releaseID)
	putS3Object(t, ctx, clients, contractKey,
		[]byte(pyRemediationContractYAML(t, service, contractPath, scriptPath)))
	postPythonRelease(t, clients, service, releaseID, pyFixtureImage)

	waitForReleaseRejected(t, ctx, clients, releaseID, 12*time.Minute)
}

// pyRemediationContractYAML renders the merged contract_version-1 wire artifact
// a python service's CI uploads before POST /releases, from the fixture
// repository's own authored contract file and script.
//
// It applies the same per-node hash recipe as the python release test's
// pythonContractYAML: source_hash is the real sha256 of the script, so editing
// the script genuinely re-fingerprints the node; shared_code_hash is empty (the
// script imports nothing in-repo); and manifest-controller recomputes only the
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

// ensureBindingRelation creates the relation a repaired contract reads from.
// It lives outside the node registry, so the candidate-schema rewriter passes
// it through verbatim and the validation Job's bind check resolves it against
// the real warehouse — which is exactly why it has to exist for a shadow
// release to validate.
func ensureBindingRelation(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	_, err := clients.dbtDB.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS public.right_name (id integer)`)
	require.NoError(t, err, "create %s in the warehouse", pyBindingRelation)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := clients.dbtDB.ExecContext(cleanupCtx, `DROP TABLE IF EXISTS public.right_name`); err != nil {
			t.Logf("cleanup: drop %s: %v", pyBindingRelation, err)
		}
	})
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
	Status          string `db:"status"`
	Attempt         int    `db:"attempt"`
	ShadowReleaseID string `db:"shadow_release_id"`
	VerifyError     string `db:"verify_error"`
	FilePath        string `db:"file_path"`
	FileEdits       []byte `db:"file_edits"`
}

// waitForProposal polls the remediation-agent's proposal table until the named
// attempt for a node reaches want, and returns the row. It reports the last
// status it saw on timeout, so a run that stalls in 'generating' or 'verifying'
// is distinguishable from one that never produced an attempt at all.
func waitForProposal(
	t *testing.T, ctx context.Context, clients *testClients,
	releaseID, nodeID string, attempt int, want string, timeout time.Duration,
) pyProposalRow {
	t.Helper()
	var row pyProposalRow
	var last string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		err := clients.remediationAgentDB.GetContext(ctx, &row, `
			SELECT status, attempt, shadow_release_id, verify_error, file_path, file_edits
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
	}, fmt.Sprintf("timeout waiting for proposal %s/%s attempt %d to reach %q (last status %q)",
		releaseID, nodeID, attempt, want, last))
	return row
}

// pyFileEdit mirrors one element of the proposal's file_edits column.
type pyFileEdit struct {
	Path       string `json:"path"`
	ContentURI string `json:"content_uri"`
	DiffURI    string `json:"diff_uri"`
}

// decodeFileEdits reads the proposal row's file_edits column.
func decodeFileEdits(t *testing.T, raw []byte) []pyFileEdit {
	t.Helper()
	var edits []pyFileEdit
	require.NoError(t, json.Unmarshal(raw, &edits), "decode proposal file_edits: %s", raw)
	return edits
}

// assertReleaseListedAsShadow polls the release list until it carries the given
// release, and asserts it is flagged as a shadow — the flag the operator UI
// labels a fix-verification run by. Polled because the list is read through the
// same repository the submission writes, and the submission is accepted before
// the row is visible to a concurrent reader.
func assertReleaseListedAsShadow(t *testing.T, ctx context.Context, clients *testClients, releaseID string) {
	t.Helper()
	var found bool
	pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		resp, err := http.Get(clients.releaseBase + "/releases?limit=50")
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		var body struct {
			Releases []struct {
				ReleaseID string `json:"release_id"`
				Shadow    bool   `json:"shadow"`
			} `json:"releases"`
		}
		if json.NewDecoder(resp.Body).Decode(&body) != nil {
			return false, nil
		}
		for _, rel := range body.Releases {
			if rel.ReleaseID == releaseID {
				require.True(t, rel.Shadow,
					"release %s must be listed as a shadow so the UI can label it a fix verification", releaseID)
				found = true
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for shadow release %s to appear in GET /releases", releaseID))
	require.True(t, found)
	t.Logf("shadow release %s is listed and flagged", releaseID)
}

// waitForReleaseStatus polls GET /releases/{id} until the release reaches want.
// Unlike waitForReleasePromoted it treats no status as a failure, because both
// terminal outcomes a shadow release can reach — validated and rejected — are
// the expected result of one test or the other.
func waitForReleaseStatus(
	t *testing.T, ctx context.Context, clients *testClients,
	releaseID, want string, timeout time.Duration,
) {
	t.Helper()
	var last string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		resp, err := http.Get(fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID))
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		body, _ := io.ReadAll(resp.Body)
		var r struct {
			Status       string `json:"status"`
			Shadow       bool   `json:"shadow"`
			RejectReason string `json:"reject_reason"`
		}
		if json.Unmarshal(body, &r) != nil {
			return false, nil
		}
		if r.Status != last {
			t.Logf("release %s: status=%s reject_reason=%q", releaseID, r.Status, r.RejectReason)
			last = r.Status
		}
		return r.Status == want, nil
	}, fmt.Sprintf("timeout waiting for release %s to reach %q (last status %q)", releaseID, want, last))
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
	}, fmt.Sprintf("timeout waiting for remediation.proposed:v1 for release %s node %s", releaseID, nodeID))
}

// assertNoRemediationTriggerFor fails if any remediation trigger names the
// given release. Called with a shadow release id: a trigger for one would mean
// a failed fix attempt had been handed back to the fixer as a fresh failure to
// heal, which is a loop with no exit.
func assertNoRemediationTriggerFor(t *testing.T, ctx context.Context, clients *testClients, releaseID string) {
	t.Helper()
	msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV1, "-", "+").Result()
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
		require.NotEqual(t, releaseID, p.ReleaseID,
			"shadow release %s must produce no remediation trigger, found one for node %s", releaseID, p.NodeID)
	}
}

// assertNoNodeVersionsFor fails if the graph recorded any promoted code version
// for the given release. A shadow release never promotes, so a version stamped
// with its id would mean an unapproved contract entered the production history
// the graph is the record of.
func assertNoNodeVersionsFor(t *testing.T, ctx context.Context, clients *testClients, releaseID string) {
	t.Helper()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)
	res, err := session.Run(ctx,
		`MATCH (v:NodeVersion {release_id: $rid}) RETURN count(v) AS n`,
		map[string]any{"rid": releaseID})
	require.NoError(t, err, "count :NodeVersion rows for %s", releaseID)
	require.True(t, res.Next(ctx), "count query returned no row")
	n, _ := res.Record().Get("n")
	count, _ := n.(int64)
	require.Zero(t, count,
		"shadow release %s must write no :NodeVersion — it never promoted", releaseID)
}
