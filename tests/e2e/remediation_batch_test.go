package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// The service-2 fixture models these tests drive. ftable_k is broken the same
// way ftable_e is (it joins public.wrong_name, a relation that does not exist)
// but shares no changed ancestor with it, so the two are independent failures.
// ftable_u is a well-formed model whose body drops the amount column; ftable_v
// and ftable_w both read u.amount, so a change to u breaks both of them
// identically and is their single shared cause.
const (
	ftableKUniqueID = "e2e_schema.ftable_k"
	ftableUUniqueID = "e2e_schema.ftable_u"
	ftableVUniqueID = "e2e_schema.ftable_v"
	ftableWUniqueID = "e2e_schema.ftable_w"
)

// Budgets shared by both batched-remediation tests. Each drives two full
// release pipelines in kind — the rejected release, then the shadow release
// that verifies the proposed fix — with one or two model calls in between, so
// the ceilings are sized for a cold-ish stack rather than for the ~3 minutes a
// warm one takes.
const (
	batchCtxBudget      = 35 * time.Minute
	batchRejectBudget   = 10 * time.Minute
	batchTriggerBudget  = 3 * time.Minute
	batchProposalBudget = 20 * time.Minute
	batchEventBudget    = 3 * time.Minute
	batchShadowBudget   = 2 * time.Minute
)

// TestE2E_BatchedRemediation_TwoIndependentFailuresOnePullRequest proves that a
// release is remediated as one unit even when its failures have nothing in
// common. ftable_e and ftable_k both reference a relation that does not exist,
// and neither descends from a node this release changed, so the driver forms
// two independent clusters — yet everything downstream of the grouping stays
// singular:
//
//	POST /releases (service-2; ftable_e and ftable_k are both new)
//	→ validation fails on both nodes → release "rejected"
//	→ ONE remediation.requested:v2 carrying both nodes, each with no changed ancestor
//	→ ONE proposal row for the release (attempt 1) holding TWO file edits,
//	  each naming the node whose source it repairs
//	→ ONE dbt shadow release (both edits belong to service-2) that validates
//	→ the reconciler flips the attempt verifying → proposed
//	→ ONE remediation.proposed:v1
//	→ ONE pull request, titled for the two nodes it fixes
//
// The release, not the node, is the unit of remediation: two failures must
// never cost the reviewer two pull requests.
func TestE2E_BatchedRemediation_TwoIndependentFailuresOnePullRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), batchCtxBudget)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-batch-" + uuid.NewString()[:8]
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s changed_nodes=%s,%s",
		releaseID, changedService, ftableEUniqueID, ftableKUniqueID)

	// 1. Build the prod snapshot from the baseline manifests, holding back both
	//    broken models so the derived changed set is exactly {ftable_e, ftable_k}.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	held := map[string]bool{ftableEUniqueID: false, ftableKUniqueID: false}
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			if _, excluded := held[n.uniqueID]; excluded {
				held[n.uniqueID] = true
				continue // excluded from prod → a changed node of this release
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	for nodeID, found := range held {
		require.True(t, found,
			"%s not found in any baseline manifest — is the model in service-2 and the image rebuilt?", nodeID)
	}
	t.Logf("seeded prod snapshot with %d nodes (%s and %s excluded)",
		len(prodNodes), ftableEUniqueID, ftableKUniqueID)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// 2. Place both broken models in the graph. The independent fixers read the
	//    failing node's location off the trigger, but the agent also asks the
	//    graph for each node's upstream evidence, so a node the promoted graph
	//    cannot place degrades the prompt silently.
	seedFTableETopologyNode(t, ctx, clients)
	seedModelTopologyNodes(t, ctx, clients, topologyModel{
		uniqueID: ftableKUniqueID, schema: "e2e_schema", table: "ftable_k",
		service: changedService, filePath: "models/ftable_k.sql",
	})

	// 3. Drive the release to rejection. Both held-back models reference
	//    public.wrong_name, so both fail validation.
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.Subset(t, failing, []string{ftableEUniqueID, ftableKUniqueID},
		"both deliberately-broken models must be reported as failing; got %v", failing)
	t.Logf("release %s failing_nodes=%v", releaseID, failing)

	// 4. ONE trigger for the release, carrying BOTH failing nodes. This is the
	//    batching contract at its source: the classifier emits per release, not
	//    per node, so a second message here would mean two fix attempts, two
	//    proposals and two pull requests downstream.
	trigger := waitForBatchedTrigger(t, ctx, clients, releaseID,
		[]string{ftableEUniqueID, ftableKUniqueID}, batchTriggerBudget)
	require.Len(t, triggersForRelease(t, ctx, clients, releaseID), 1,
		"a rejected release must produce exactly one remediation.requested:v2")
	require.Len(t, trigger.Nodes, 2,
		"the trigger must carry exactly the two failing models; got %v", triggerNodeIDs(trigger))
	require.Equal(t, "validation", trigger.Source, "trigger source")

	for _, nodeID := range []string{ftableEUniqueID, ftableKUniqueID} {
		node, ok := trigger.findNode(nodeID)
		require.True(t, ok, "trigger must carry an entry for %s", nodeID)
		require.Empty(t, node.ChangedAncestors,
			"%s fails on its own body, not below a changed ancestor, so it must name none; got %v",
			nodeID, node.ChangedAncestors)
		require.NotEmpty(t, node.ErrorSignature, "%s must carry an error_signature", nodeID)
	}
	t.Logf("✅ one remediation.requested:v2 carries both independent failures")

	// 5. ONE proposal row for the whole release, reaching 'proposed' only after
	//    its shadow release has judged the fix. The representative node_id is the
	//    lowest resolved id, so the row is addressed by ftable_e.
	row := waitForBatchProposal(t, ctx, clients, releaseID, 1, "proposed", batchProposalBudget)
	require.Equal(t, 1, countProposalsForRelease(t, ctx, clients, releaseID),
		"one release yields one fix attempt, not one per failing node")

	resolved := decodeNodeIDs(t, row.ResolvedNodeIDs)
	require.Equal(t, []string{ftableEUniqueID, ftableKUniqueID}, resolved,
		"the attempt must address the release's whole failing set")

	outcomes := decodeNodeOutcomes(t, row.NodeOutcomes)
	for _, nodeID := range resolved {
		require.Equal(t, "proposed", outcomes[nodeID].Status,
			"every node the verified attempt resolved must end 'proposed'; node_outcomes=%s", row.NodeOutcomes)
	}

	// 6. Two edits, one per broken model, each naming the node it repairs. The
	//    target is what tells a reviewer which file fixes which failure — the
	//    row's single node_id column cannot stand for both.
	edits := decodeFileEdits(t, row.FileEdits)
	require.Len(t, edits, 2, "two independent failures are repaired in two files; got %+v", edits)
	targetsByPath := map[string]string{}
	for _, e := range edits {
		require.NotEmpty(t, e.ContentURI, "edit %s must point at its proposed content", e.Path)
		targetsByPath[e.Path] = e.TargetNodeID
	}
	require.Equal(t, map[string]string{
		"services/service-2/models/ftable_e.sql": ftableEUniqueID,
		"services/service-2/models/ftable_k.sql": ftableKUniqueID,
	}, targetsByPath, "each edit must name the node whose source it changes")

	// 7. Both edits belong to service-2, so ONE shadow release verified the whole
	//    attempt — and it really ran: it reached 'validated' and is flagged as a
	//    shadow in the release list the operator UI reads.
	verifications := decodeVerifications(t, row.Verifications)
	require.Len(t, verifications, 1,
		"both files belong to service-2, so one shadow release judged the attempt; got %+v", verifications)
	require.Equal(t, "dbt", verifications[0].Kind, "a dbt model's fix is verified under a dbt manifest")
	require.Equal(t, changedService, verifications[0].Service, "verification service")
	require.NotEmpty(t, verifications[0].ShadowReleaseID, "a verification must name the release that judged it")
	waitForReleaseStatus(t, ctx, clients, verifications[0].ShadowReleaseID, "validated", batchShadowBudget)
	assertReleaseListedAsShadow(t, ctx, clients, verifications[0].ShadowReleaseID)
	t.Logf("✅ one shadow release %s validated both edits", verifications[0].ShadowReleaseID)

	// 8. ONE announcement, carrying the same batched view as the row.
	proposed := waitForBatchProposedEvent(t, ctx, clients, releaseID, batchEventBudget)
	require.Len(t, proposedEventsForRelease(t, ctx, clients, releaseID), 1,
		"one verified attempt announces itself once")
	require.Equal(t, []string{ftableEUniqueID, ftableKUniqueID}, proposed.ResolvedNodeIDs,
		"remediation.proposed:v1 must announce the whole resolved set")
	require.Len(t, proposed.Edits, 2, "the announcement must carry both file edits; got %+v", proposed.Edits)
	require.Equal(t, ftableEUniqueID, proposed.NodeID,
		"the representative node is the lowest resolved id")

	// 9. ONE pull request for the release, named for the set it fixes.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID)
	pr := fetchStubPullRequest(t, ctx, row.Repo, prNumber)
	require.Equal(t, fmt.Sprintf("[remediation] fix 2 nodes (release %s)", releaseID), pr.Title,
		"a batched pull request must name how many nodes it fixes")
	t.Logf("✅ one pull request #%d: %q", pr.Number, pr.Title)

	// 10. Opening the pull request must not have minted a second attempt: the
	//     release is still one fix, reviewed once.
	require.Equal(t, 1, countProposalsForRelease(t, ctx, clients, releaseID),
		"no second proposal row may appear for a release whose single attempt was accepted")
}

// TestE2E_BatchedRemediation_SharedUpstreamFixedOnce proves that failures with
// one cause are repaired once, in the cause. ftable_u is seeded as the release's
// only changed node (its production content_hash is replaced with a stale
// value), and its new body drops the amount column that ftable_v and ftable_w
// both read. Neither v nor w changed, so neither is at fault:
//
//	POST /releases (service-2; ftable_u is the only changed node)
//	→ validation fails on ftable_v and ftable_w, identically
//	→ ONE remediation.requested:v2 whose two entries share one error signature
//	  and both name ftable_u as their changed ancestor
//	→ the driver forms ONE shared-upstream cluster targeting ftable_u
//	→ ONE proposal with ONE edit — to ftable_u's source, a node that never failed
//	→ ONE dbt shadow release that validates the repaired ancestor and its
//	  descendants
//	→ ONE pull request fixing two nodes by changing one file
//
// The signature equality is asserted before anything else and on its own: if
// the classifier's key error line differed per node, grouping could not happen
// at all and every later assertion would fail for a reason that is not the one
// under test.
func TestE2E_BatchedRemediation_SharedUpstreamFixedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), batchCtxBudget)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-up-" + uuid.NewString()[:8]
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s changed_node=%s expected_failures=%s,%s",
		releaseID, changedService, ftableUUniqueID, ftableVUniqueID, ftableWUniqueID)

	// 1. Seed prod with EVERY baseline node, but give ftable_u a stale
	//    content_hash. It is then the release's only changed node, while its two
	//    descendants keep their real hashes and are pulled into validation only
	//    because they descend from it — which is precisely the shape a
	//    shared-upstream cause has in production.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	seen := map[string]bool{}
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			seen[n.uniqueID] = true
			hash := n.contentHash
			if n.uniqueID == ftableUUniqueID {
				// Any value the candidate hash cannot equal makes this node, and
				// only this node, read as changed against production.
				hash = "stale-" + hash
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": hash,
			})
		}
	}
	for _, nodeID := range []string{ftableUUniqueID, ftableVUniqueID, ftableWUniqueID} {
		require.True(t, seen[nodeID],
			"%s not found in any baseline manifest — is the model in service-2 and the image rebuilt?", nodeID)
	}
	t.Logf("seeded prod snapshot with %d nodes (%s hash staled)", len(prodNodes), ftableUUniqueID)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// 2. Place the changed ancestor in the graph. The upstream fixer edits a node
	//    that never failed; its location now travels on the trigger (each failing
	//    node's changed_ancestors carry the path THIS candidate declares), and the
	//    promoted graph is only the fallback for a rejection that carries none.
	//    The node is still seeded so the graph the fixer's other reads consult
	//    knows the ancestor at all.
	seedModelTopologyNodes(t, ctx, clients, topologyModel{
		uniqueID: ftableUUniqueID, schema: "e2e_schema", table: "ftable_u",
		service: changedService, filePath: "models/ftable_u.sql",
	})

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	// 3. The descendants failed; the ancestor that caused it did not.
	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.Equal(t, []string{ftableVUniqueID, ftableWUniqueID}, failing,
		"only the two descendants that read the dropped column may fail; got %v", failing)
	t.Logf("release %s failing_nodes=%v", releaseID, failing)

	trigger := waitForBatchedTrigger(t, ctx, clients, releaseID,
		[]string{ftableVUniqueID, ftableWUniqueID}, batchTriggerBudget)
	vNode, ok := trigger.findNode(ftableVUniqueID)
	require.True(t, ok, "trigger must carry an entry for %s", ftableVUniqueID)
	wNode, ok := trigger.findNode(ftableWUniqueID)
	require.True(t, ok, "trigger must carry an entry for %s", ftableWUniqueID)

	// 4. FIRST, and on its own: the two nodes must fail the SAME way. Grouping is
	//    keyed on the error signature, so a mismatch here is not a test detail —
	//    it means the classifier's key error line varies per node and no
	//    shared-upstream cluster can ever form, for any release.
	require.Equal(t, vNode.ErrorSignature, wNode.ErrorSignature,
		"ftable_v and ftable_w have identical bodies and fail on the same dropped column, so the "+
			"classifier must fold them onto ONE error signature — otherwise SharedUpstreamCause "+
			"never groups them and no upstream fix is possible.\n"+
			"  %s: signature=%s error=%q\n"+
			"  %s: signature=%s error=%q",
		ftableVUniqueID, vNode.ErrorSignature, vNode.ErrorExcerpt,
		ftableWUniqueID, wNode.ErrorSignature, wNode.ErrorExcerpt)
	t.Logf("✅ both descendants fold onto one error signature: %s (%q)",
		vNode.ErrorSignature, vNode.ErrorExcerpt)

	// 5. And both must name the changed ancestor as their cause — the other half
	//    of what the clustering needs.
	require.Len(t, trigger.Nodes, 2,
		"the trigger must carry exactly the two failing descendants; got %v", triggerNodeIDs(trigger))
	for _, n := range []remediationNodeEntry{vNode, wNode} {
		require.Equal(t, []string{ftableUUniqueID}, n.ancestorIDs(),
			"%s failed below the one node this release changed, so the trigger must say so", n.NodeID)
		require.Equal(t, "models/ftable_u.sql", n.ChangedAncestors[0].FilePath,
			"the ancestor must carry the path THIS candidate declares — the file the upstream fix edits")
		require.Equal(t, changedService, n.ChangedAncestors[0].Service, "the ancestor's owning service")
	}
	t.Logf("✅ one remediation.requested:v2 names %s as the shared cause of both failures", ftableUUniqueID)

	// 6. ONE proposal, and — the point of the whole scenario — ONE edit, to a
	//    node that never appeared in the failing set.
	row := waitForBatchProposal(t, ctx, clients, releaseID, 1, "proposed", batchProposalBudget)
	require.Equal(t, 1, countProposalsForRelease(t, ctx, clients, releaseID),
		"one release yields one fix attempt")

	resolved := decodeNodeIDs(t, row.ResolvedNodeIDs)
	require.Equal(t, []string{ftableVUniqueID, ftableWUniqueID}, resolved,
		"the attempt resolves the failing descendants, not the ancestor it edits")

	outcomes := decodeNodeOutcomes(t, row.NodeOutcomes)
	for _, nodeID := range resolved {
		require.Equal(t, "proposed", outcomes[nodeID].Status,
			"both descendants must end 'proposed' off the single upstream fix; node_outcomes=%s", row.NodeOutcomes)
	}

	edits := decodeFileEdits(t, row.FileEdits)
	require.Len(t, edits, 1,
		"two failures with one cause are repaired by one edit, not two; got %+v", edits)
	require.Equal(t, "services/service-2/models/ftable_u.sql", edits[0].Path,
		"the edit must change the changed ancestor's source")
	require.Equal(t, ftableUUniqueID, edits[0].TargetNodeID,
		"the edit must name the ancestor it repairs, which is not one of the failing nodes")

	// 7. The single edit was verified by a real shadow release.
	verifications := decodeVerifications(t, row.Verifications)
	require.Len(t, verifications, 1,
		"one edited service, so one shadow release; got %+v", verifications)
	require.Equal(t, "dbt", verifications[0].Kind, "a dbt model's fix is verified under a dbt manifest")
	require.Equal(t, changedService, verifications[0].Service, "verification service")
	waitForReleaseStatus(t, ctx, clients, verifications[0].ShadowReleaseID, "validated", batchShadowBudget)
	assertReleaseListedAsShadow(t, ctx, clients, verifications[0].ShadowReleaseID)
	t.Logf("✅ shadow release %s validated the upstream fix", verifications[0].ShadowReleaseID)

	// 8. The proposed source really restores the column both descendants read —
	//    the shadow could not have validated otherwise, but asserting it names
	//    what the fix was, not just that one existed.
	contentKey := stripS3Prefix(edits[0].ContentURI)
	require.NotEmpty(t, contentKey, "could not parse key from content_uri=%s", edits[0].ContentURI)
	body := string(getS3ObjectByKey(t, ctx, clients, contentKey))
	require.Contains(t, body, "0 AS amount",
		"the repaired ancestor must give back the amount column its change dropped; got %q", strings.TrimSpace(body))
	t.Logf("proposed upstream source at %s: %q", edits[0].ContentURI, strings.TrimSpace(body))

	// 9. ONE pull request, naming the two failures the one edit resolves.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID)
	pr := fetchStubPullRequest(t, ctx, row.Repo, prNumber)
	require.Equal(t, fmt.Sprintf("[remediation] fix 2 nodes (release %s)", releaseID), pr.Title,
		"the title names the failing nodes resolved, not the single file changed")
	t.Logf("✅ one pull request #%d fixes two nodes with one edit: %q", pr.Number, pr.Title)
}

// batchProposalRow is the batched view of a proposal row: the set the attempt
// addressed, each node's outcome, every file it changed, and every shadow
// release that judged it. The single-node projection is proposalRow in
// remediation_agent_test.go; a batched attempt cannot be read through that one,
// because its scalar node_id and file_path columns hold only a representative.
type batchProposalRow struct {
	ID              string `db:"id"`
	NodeID          string `db:"node_id"`
	Status          string `db:"status"`
	Attempt         int    `db:"attempt"`
	Repo            string `db:"repo"`
	ResolvedNodeIDs []byte `db:"resolved_node_ids"`
	NodeOutcomes    []byte `db:"node_outcomes"`
	FileEdits       []byte `db:"file_edits"`
	Verifications   []byte `db:"verifications"`
}

// waitForBatchProposal polls the agent-remediation's proposal table until the
// release's given attempt reaches want, and returns the row. It is addressed by
// (release, attempt) rather than by node, because one attempt now covers a whole
// failing set and no single node identifies it. Every status change is logged,
// so an attempt that stalls in 'generating' or 'verifying' is distinguishable
// from one that was never created — pollUntil's message is built before any
// polling and cannot carry that.
func waitForBatchProposal(
	t *testing.T, ctx context.Context, clients *testClients,
	releaseID string, attempt int, want string, timeout time.Duration,
) batchProposalRow {
	t.Helper()
	var row batchProposalRow
	var last string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row, `
			SELECT id, node_id, status, attempt, repo,
			       resolved_node_ids, node_outcomes, file_edits, verifications
			  FROM proposal
			 WHERE release_id = $1 AND attempt = $2`,
			releaseID, attempt)
		if err != nil {
			return false, nil
		}
		if row.Status != last {
			t.Logf("proposal %s attempt %d: status=%s node_id=%s", releaseID, attempt, row.Status, row.NodeID)
			last = row.Status
		}
		return row.Status == want, nil
	}, fmt.Sprintf("timeout waiting for release %s attempt %d to reach %q (see the logged status changes above)",
		releaseID, attempt, want))
	return row
}

// countProposalsForRelease returns how many proposal rows exist for a release.
// A batched release is one attempt: this is what proves the failing set was not
// silently fanned back out into one proposal — and one pull request — per node.
func countProposalsForRelease(t *testing.T, ctx context.Context, clients *testClients, releaseID string) int {
	t.Helper()
	var n int
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &n,
		`SELECT count(*) FROM proposal WHERE release_id = $1`, releaseID))
	return n
}

// nodeOutcomeEntry mirrors one value of the proposal's node_outcomes column:
// how the attempt ended for one of the nodes it addressed.
type nodeOutcomeEntry struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// decodeNodeOutcomes reads the node_outcomes column, keyed by node id.
func decodeNodeOutcomes(t *testing.T, raw []byte) map[string]nodeOutcomeEntry {
	t.Helper()
	outcomes := map[string]nodeOutcomeEntry{}
	require.NoError(t, json.Unmarshal(raw, &outcomes), "decode proposal node_outcomes: %s", raw)
	return outcomes
}

// decodeNodeIDs reads a JSONB string array column (resolved_node_ids).
func decodeNodeIDs(t *testing.T, raw []byte) []string {
	t.Helper()
	var ids []string
	require.NoError(t, json.Unmarshal(raw, &ids), "decode node id array: %s", raw)
	return ids
}

// waitForBatchedTrigger polls remediation.requested:v2 until the release's
// trigger carries an entry for every named node, and returns it. Waiting on the
// whole set rather than on any one node is what makes the batching assertions
// meaningful: a trigger observed mid-write would otherwise satisfy a
// single-node wait and then fail a count.
func waitForBatchedTrigger(
	t *testing.T, ctx context.Context, clients *testClients,
	releaseID string, nodeIDs []string, timeout time.Duration,
) remediationRequestedPayload {
	t.Helper()
	var found remediationRequestedPayload
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		for _, p := range triggersForRelease(t, ctx, clients, releaseID) {
			complete := true
			for _, id := range nodeIDs {
				if _, ok := p.findNode(id); !ok {
					complete = false
					break
				}
			}
			if complete {
				found = p
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for one remediation.requested:v2 for release %s carrying %v",
		releaseID, nodeIDs))
	return found
}

// triggersForRelease returns every remediation.requested:v2 message naming the
// release. A release is remediated once, so the length of this is itself an
// assertion subject.
func triggersForRelease(t *testing.T, ctx context.Context, clients *testClients, releaseID string) []remediationRequestedPayload {
	t.Helper()
	msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV2, "-", "+").Result()
	require.NoError(t, err, "read %s", streams.RemediationRequestedV2)
	var out []remediationRequestedPayload
	for _, msg := range msgs {
		raw, _ := msg.Values["payload"].(string)
		if raw == "" {
			continue
		}
		var p remediationRequestedPayload
		if json.Unmarshal([]byte(raw), &p) != nil || p.ReleaseID != releaseID {
			continue
		}
		out = append(out, p)
	}
	return out
}

// triggerNodeIDs lists the node ids a trigger carries, for failure messages.
func triggerNodeIDs(p remediationRequestedPayload) []string {
	ids := make([]string, 0, len(p.Nodes))
	for _, n := range p.Nodes {
		ids = append(ids, n.NodeID)
	}
	return ids
}

// proposedEdit mirrors one element of remediation.proposed:v1's edits array.
type proposedEdit struct {
	Path         string `json:"path"`
	ContentURI   string `json:"content_uri"`
	DiffURI      string `json:"diff_uri"`
	TargetNodeID string `json:"target_node_id"`
}

// batchProposedPayload is the batched projection of remediation.proposed:v1:
// the whole set the attempt resolved and every file it changed. The single-node
// projection of the same event is remediationProposedPayload in
// remediation_agent_test.go, which does not decode the edits array.
type batchProposedPayload struct {
	ReleaseID       string         `json:"release_id"`
	NodeID          string         `json:"node_id"`
	ResolvedNodeIDs []string       `json:"resolved_node_ids"`
	Attempt         int            `json:"attempt"`
	Edits           []proposedEdit `json:"edits"`
}

// waitForBatchProposedEvent polls remediation.proposed:v1 for the release's
// announcement and returns it.
func waitForBatchProposedEvent(
	t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration,
) batchProposedPayload {
	t.Helper()
	var found batchProposedPayload
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		events := proposedEventsForRelease(t, ctx, clients, releaseID)
		if len(events) == 0 {
			return false, nil
		}
		found = events[0]
		return true, nil
	}, fmt.Sprintf("timeout waiting for remediation.proposed:v1 for release %s", releaseID))
	return found
}

// proposedEventsForRelease returns every remediation.proposed:v1 message naming
// the release.
func proposedEventsForRelease(t *testing.T, ctx context.Context, clients *testClients, releaseID string) []batchProposedPayload {
	t.Helper()
	msgs, err := clients.redisClient.XRange(ctx, streams.RemediationProposedV1, "-", "+").Result()
	require.NoError(t, err, "read %s", streams.RemediationProposedV1)
	var out []batchProposedPayload
	for _, msg := range msgs {
		raw, _ := msg.Values["payload"].(string)
		if raw == "" {
			continue
		}
		var p batchProposedPayload
		if json.Unmarshal([]byte(raw), &p) != nil || p.ReleaseID != releaseID {
			continue
		}
		out = append(out, p)
	}
	return out
}

// releaseFailingNodes reads the failing node ids the release-controller reports
// for a release, sorted so the set can be compared directly.
func releaseFailingNodes(t *testing.T, ctx context.Context, clients *testClients, releaseID string) []string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID), nil)
	require.NoError(t, err, "build GET release request")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	require.NoError(t, err, "GET release %s", releaseID)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET release %s", releaseID)

	var body struct {
		FailingNodes []string `json:"failing_nodes"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body), "decode release %s", releaseID)
	sort.Strings(body.FailingNodes)
	return body.FailingNodes
}

// openRemediationPR opens the proposal's pull request through the UI route and
// returns its number, asserting the route is idempotent: a second POST must
// return 409 with the same URL rather than opening a second pull request, and
// the row must settle on pr_state='open'.
func openRemediationPR(
	t *testing.T, ctx context.Context, clients *testClients, proposalID, releaseID string,
) int {
	t.Helper()
	created := callCreatePREndpoint(t, ctx, clients.uiBase, proposalID, http.StatusOK)
	require.NotEmpty(t, created.PRUrl, "pr_url must be non-empty on first create")
	require.Greater(t, created.PRNumber, 0, "pr_number must be a positive stub-github PR number")

	dup := callCreatePREndpoint409(t, ctx, clients.uiBase, proposalID)
	require.Equal(t, created.PRUrl, dup.PRUrl,
		"a second POST must return the SAME pull request, not open another one for release %s", releaseID)

	var row prRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row,
			`SELECT pr_state, pr_url, pr_number FROM proposal WHERE id = $1`, proposalID)
		if err != nil {
			return false, nil
		}
		return row.PRState == "open", nil
	}, fmt.Sprintf("timeout waiting for proposal %s to reach pr_state='open'", proposalID))
	require.Equal(t, created.PRNumber, row.PRNumber, "the row must record the number the create call returned")
	return created.PRNumber
}

// stubPullRequest is the subset of stub-github's pulls JSON these tests assert.
type stubPullRequest struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	State  string `json:"state"`
}

// fetchStubPullRequest reads one pull request back from stub-github. The title
// is only observable here — the create-PR route returns just a URL and a number
// — so this is what proves the pull request a reviewer receives is named for
// every node it fixes. The e2e suite runs inside the orchestrator container, so
// stub-github is reached by its compose service name, the same base URL
// agent-remediation and ui use.
func fetchStubPullRequest(t *testing.T, ctx context.Context, repo string, number int) stubPullRequest {
	t.Helper()
	require.NotEmpty(t, repo, "proposal row must carry the repo its pull request was opened on")
	url := fmt.Sprintf("http://stub-github:9200/repos/%s/pulls/%d", repo, number)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err, "build GET pull request")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	require.NoError(t, err, "GET %s", url)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", url)

	var pr stubPullRequest
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&pr), "decode pull request from %s", url)
	require.Equal(t, number, pr.Number, "stub-github returned a different pull request")
	return pr
}

// topologyModel is one dbt model to place in Neo4j as a :Table node.
type topologyModel struct {
	uniqueID string
	schema   string
	table    string
	service  string
	filePath string
}

// seedModelTopologyNodes MERGEs a minimal :Table node per model so the
// agent-remediation's Locator (the GetNodeLocation gRPC call) can resolve each
// node's file path and service name. In a cold e2e no release has been
// promoted, so Neo4j is empty; a node missing here is reported NOT_FOUND, which
// makes an upstream cluster skip its target and degrades a node's source
// resolution silently.
//
// The unique_id uses the same key format the release-promotion handler writes
// ("{schema_name}.{table_name}"), and original_file_path matches the dbt
// manifest value, which the agent joins with the service_repos.yaml prefix to
// form the full repository path.
//
// The nodes are removed via t.Cleanup so they do not leak into other tests.
// seedFTableETopologyNode in remediation_agent_test.go owns ftable_e and its
// private ancestor; this seeds the nodes no existing helper places.
func seedModelTopologyNodes(t *testing.T, ctx context.Context, clients *testClients, models ...topologyModel) {
	t.Helper()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer session.Close(ctx)

	ids := make([]string, 0, len(models))
	for _, m := range models {
		_, err := session.Run(ctx, `
			MERGE (t:Table {unique_id: $uid})
			SET t.schema_name        = $schema_name,
			    t.table_name         = $table_name,
			    t.service_name       = $service_name,
			    t.original_file_path = $file_path,
			    t.node_type          = 'model',
			    t.active             = true
		`, map[string]interface{}{
			"uid":          m.uniqueID,
			"schema_name":  m.schema,
			"table_name":   m.table,
			"service_name": m.service,
			"file_path":    m.filePath,
		})
		require.NoError(t, err, "seed %s topology node in Neo4j", m.uniqueID)
		ids = append(ids, m.uniqueID)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupSession := clients.neo4jDriver.NewSession(cleanupCtx, neo4jdriver.SessionConfig{
			AccessMode: neo4jdriver.AccessModeWrite,
		})
		defer cleanupSession.Close(cleanupCtx)
		_, _ = cleanupSession.Run(cleanupCtx,
			`MATCH (t:Table) WHERE t.unique_id IN $ids DETACH DELETE t`,
			map[string]interface{}{"ids": ids})
	})
	t.Logf("seeded topology nodes in Neo4j: %v", ids)
}
