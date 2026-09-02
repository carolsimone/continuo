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

	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
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

// ftableGUniqueID is service-3's fixture model for the cross-service batching
// test: it joins public.wrong_name_2, a relation that does not exist, so it
// fails validation independently of any service-2 failure. It is otherwise
// reserved by build_operation_test.go / failure_test.go / rebase_test.go for a
// scheduled-execution-failure fixture (schedule "e2e-schedule-failure"); that
// use is unaffected here because a remediation PR's merge never writes back
// to the real dbt/services/service-3/models/ftable_g.sql file — the whole PR
// lifecycle, including the merge, is mediated by stub-github and never
// touches the checked-out fixture.
const ftableGUniqueID = "e2e_schema.ftable_g"

// Budgets shared by both batched-remediation tests. Each drives two full
// pipeline runs in kind — the rejected candidate release, then the
// verification run that verifies the proposed fix — with one or two model
// calls in between, so the ceilings are sized for a cold-ish stack rather
// than for the ~3 minutes a warm one takes.
const (
	batchCtxBudget      = 35 * time.Minute
	batchRejectBudget   = 10 * time.Minute
	batchTriggerBudget  = 3 * time.Minute
	batchProposalBudget = 20 * time.Minute
	batchEventBudget    = 3 * time.Minute
	batchVerifyBudget   = 2 * time.Minute
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
//	→ ONE dbt verification run (both edits belong to service-2) that passes
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
		"a rejected release must produce exactly one %s", streams.RemediationRequestedV2)
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
	t.Logf("✅ one %s carries both independent failures", streams.RemediationRequestedV2)

	// 5. ONE proposal row for the whole release, reaching 'proposed' only after
	//    its verification run has judged the fix. The representative node_id is
	//    the lowest resolved id, so the row is addressed by ftable_e.
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

	// 7. Both edits belong to service-2, so ONE verification run verified the
	//    whole attempt — and it really ran: the pipeline names it while it is
	//    active, it reaches 'passed', and it is never readable as a release.
	verifications := decodeVerifications(t, row.Verifications)
	require.Len(t, verifications, 1,
		"both files belong to service-2, so one verification run judged the attempt; got %+v", verifications)
	require.Equal(t, "dbt", verifications[0].Kind, "a dbt model's fix is verified under a dbt manifest")
	require.Equal(t, changedService, verifications[0].Service, "verification service")
	require.NotEmpty(t, verifications[0].RunID, "a verification must name the run that judged it")
	assertPipelineNamedVerification(t, ctx, clients, verifications[0].RunID, batchVerifyBudget)
	waitForVerificationStatus(t, ctx, clients, verifications[0].RunID, "passed", batchVerifyBudget)
	assertNotListedAsRelease(t, ctx, clients, verifications[0].RunID)
	t.Logf("✅ one verification run %s passed for both edits", verifications[0].RunID)

	// 8. ONE announcement, carrying the same batched view as the row.
	proposed := waitForBatchProposedEvent(t, ctx, clients, releaseID, batchEventBudget)
	require.Len(t, proposedEventsForRelease(t, ctx, clients, releaseID), 1,
		"one verified attempt announces itself once")
	require.Equal(t, []string{ftableEUniqueID, ftableKUniqueID}, proposed.ResolvedNodeIDs,
		"%s must announce the whole resolved set", streams.RemediationProposedV1)
	require.Len(t, proposed.Edits, 2, "the announcement must carry both file edits; got %+v", proposed.Edits)
	require.Equal(t, ftableEUniqueID, proposed.NodeID,
		"the representative node is the lowest resolved id")

	// 9. ONE pull request for the release, named for the set it fixes. Both
	//    edits belong to service-2 and now carry cluster member ids, so
	//    PRServices attributes the single group to that real service — the
	//    title carries its " (service-2)" suffix, the same way it would for
	//    any proposal split across more than the legacy "" group.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID, changedService)
	pr := fetchStubPullRequest(t, ctx, row.Repo, prNumber)
	require.Equal(t, fmt.Sprintf("[remediation] fix 2 nodes (release %s) (%s)", releaseID, changedService), pr.Title,
		"a batched pull request must name how many nodes it fixes and the service it was opened for")
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
//	→ ONE dbt verification run that passes for the repaired ancestor and its
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
	t.Logf("✅ one %s names %s as the shared cause of both failures", streams.RemediationRequestedV2, ftableUUniqueID)

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

	// 7. The single edit was verified by a real verification run.
	verifications := decodeVerifications(t, row.Verifications)
	require.Len(t, verifications, 1,
		"one edited service, so one verification run; got %+v", verifications)
	require.Equal(t, "dbt", verifications[0].Kind, "a dbt model's fix is verified under a dbt manifest")
	require.Equal(t, changedService, verifications[0].Service, "verification service")
	assertPipelineNamedVerification(t, ctx, clients, verifications[0].RunID, batchVerifyBudget)
	waitForVerificationStatus(t, ctx, clients, verifications[0].RunID, "passed", batchVerifyBudget)
	assertNotListedAsRelease(t, ctx, clients, verifications[0].RunID)
	t.Logf("✅ verification run %s passed for the upstream fix", verifications[0].RunID)

	// 8. The proposed source really restores the column both descendants read —
	//    the verification run could not have passed otherwise, but asserting it
	//    names what the fix was, not just that one existed.
	contentKey := stripS3Prefix(edits[0].ContentURI)
	require.NotEmpty(t, contentKey, "could not parse key from content_uri=%s", edits[0].ContentURI)
	body := string(getS3ObjectByKey(t, ctx, clients, contentKey))
	require.Contains(t, body, "0 AS amount",
		"the repaired ancestor must give back the amount column its change dropped; got %q", strings.TrimSpace(body))
	t.Logf("proposed upstream source at %s: %q", edits[0].ContentURI, strings.TrimSpace(body))

	// 9. ONE pull request, naming the two failures the one edit resolves. The
	//    edit's cluster carries member node ids, so PRServices attributes it
	//    to the real owning service (service-2) rather than the legacy ""
	//    group, and the title carries that service's suffix.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID, changedService)
	pr := fetchStubPullRequest(t, ctx, row.Repo, prNumber)
	require.Equal(t, fmt.Sprintf("[remediation] fix 2 nodes (release %s) (%s)", releaseID, changedService), pr.Title,
		"the title names the failing nodes resolved and the service it was opened for, not the single file changed")
	t.Logf("✅ one pull request #%d fixes two nodes with one edit: %q", pr.Number, pr.Title)

	// 10. Merging the pull request AS PROPOSED — no further human edit — must
	//     draw the case-base provenance edges the orchestrator's pr_closed
	//     consumer maintains: one RESOLVED_BY per resolved descendant to the
	//     shared :Proposal, and one EDITED edge from that :Proposal to the
	//     changed ancestor's :Table, both carrying amended=false. The stub
	//     serves the PR's own written commit at the merge sha (nothing pushes
	//     a further commit in this test), so the merged content is
	//     byte-identical to what was proposed.
	mergeCommitSHA := mergePullRequestViaStub(t, ctx, row.Repo, prNumber)
	t.Logf("merged PR #%d via stub-github: merge_commit_sha=%s", prNumber, mergeCommitSHA)

	closedRow := pollChildPRState(t, ctx, clients, row.ID, changedService, "merged", 60*time.Second)
	require.NotNil(t, closedRow.PRClosedAt, "pr_closed_at must be set on merge")

	closedPayload := latestPRClosedPayload(t, ctx, clients, row.ID, changedService)
	require.Equal(t, "merged", closedPayload.Outcome, "pr_closed payload outcome")
	for _, e := range closedPayload.Edits {
		require.False(t, e.Amended, "an as-proposed merge must carry no amended edits; got %+v", closedPayload.Edits)
	}
	t.Logf("✅ close-loop confirmed: pr_state=merged, %d edit(s), none amended", len(closedPayload.Edits))

	// 11. The provenance graph itself: one RESOLVED_BY per resolved node (both
	//     descendants, neither of which was edited), all amended=false; one
	//     EDITED edge naming the changed ancestor — not either failing
	//     descendant — also amended=false; and the :PullRequest node
	//     reflecting the terminal 'merged' state. pollNeo4jPRState waits for
	//     the orchestrator's asynchronous consumption of
	//     remediation.pr_closed:v1 to land; RecordPullRequestOutcome draws the
	//     pr_state change and every edge below in the SAME Cypher statement, so
	//     once the poll observes 'merged' the edges are already committed and
	//     the plain reads that follow cannot race the write.
	pollNeo4jPRState(t, ctx, clients, row.ID, changedService, "merged", 60*time.Second)

	resolvedRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {release_id: $release_id})-[rb:RESOLVED_BY]->(p:Proposal {proposal_id: $proposal_id})
		RETURN count(r) AS n, collect(DISTINCT rb.amended) AS amended`,
		map[string]any{"release_id": releaseID, "proposal_id": row.ID})
	require.Len(t, resolvedRows, 1, "expected one summary row for the RESOLVED_BY query")
	require.EqualValues(t, 2, resolvedRows[0]["n"],
		"one RESOLVED_BY per resolved node (%s, %s); got %+v", ftableVUniqueID, ftableWUniqueID, resolvedRows[0])
	require.Equal(t, []any{false}, resolvedRows[0]["amended"],
		"every RESOLVED_BY drawn by an as-proposed merge must carry amended=false; got %+v", resolvedRows[0])

	editRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (p:Proposal {proposal_id: $proposal_id})-[e:EDITED]->(t:Table)
		RETURN t.unique_id AS unique_id, e.amended AS amended`,
		map[string]any{"proposal_id": row.ID})
	require.Len(t, editRows, 1, "the shared-upstream attempt drew one EDITED edge, to the ancestor it repaired; got %+v", editRows)
	require.Equal(t, ftableUUniqueID, editRows[0]["unique_id"], "EDITED must target the changed ancestor, not either failing descendant")
	require.Equal(t, false, editRows[0]["amended"], "an as-proposed merge's EDITED edge must carry amended=false")
	t.Logf("✅ provenance confirmed: 2 RESOLVED_BY edges (amended=false), EDITED -> %s (amended=false), pr_state=merged",
		ftableUUniqueID)
}

// TestE2E_BatchedRemediation_TwoServicesTwoPullRequests proves that a release
// whose failing set spans two services opens two independent pull requests —
// one per owning service — and that each is reviewed and closed on its own:
// merging one draws case-base provenance for its members while the other,
// closed without merging, draws none.
//
// ftable_e (service-2) and ftable_g (service-3) are both held back from
// current_prod, so a release posted for service-2 sees BOTH as changed — the
// same cross-service assembly TestE2E_ReleasePromote_GatedCrossServiceUpstream
// exercises with a genuine dependency edge, here with none: the two failures
// share no ancestor and no error signature (they reference different
// nonexistent relations), so the driver forms two independent clusters, one
// edit each, grouped by owning service into two pull requests:
//
//	POST /releases (service-2; ftable_e and ftable_g both held back from prod)
//	→ validation fails on both nodes → release "rejected"
//	→ ONE remediation.requested:v2 carrying both nodes
//	→ ONE proposal row (attempt 1) holding TWO file edits, one per service
//	→ TWO dbt verification runs (one per edited service) that both pass
//	→ ONE remediation.proposed:v1
//	→ POST /proposals/:id/pull-request opens TWO pull requests, distinct
//	  branches and numbers, one per service
//	→ merge the service-2 PR, close-without-merging the service-3 PR
//	→ the merged PR's member (ftable_e) gets a RESOLVED_BY edge; the rejected
//	  PR's member (ftable_g) gets none; each :PullRequest node carries its own
//	  terminal pr_state
func TestE2E_BatchedRemediation_TwoServicesTwoPullRequests(t *testing.T) {
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

	releaseID := "e2e-rem-2svc-" + uuid.NewString()[:8]
	changedService := "service-2"
	otherService := "service-3"
	t.Logf("release_id=%s changed_service=%s other_service=%s changed_nodes=%s,%s",
		releaseID, changedService, otherService, ftableEUniqueID, ftableGUniqueID)

	// 1. Hold BOTH ftable_e (service-2) and ftable_g (service-3) back from
	//    current_prod. Neither service's own manifest is what makes them
	//    "changed" — that comes purely from their absence in current_prod
	//    against the assembled candidate topology, which is why this works
	//    for ftable_g even though the release is posted for service-2, not
	//    service-3 (see TestE2E_ReleasePromote_GatedCrossServiceUpstream's
	//    identical assembly for a cross-service pair WITH a dependency edge —
	//    here there is none, so the two failures stay independent).
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	held := map[string]bool{ftableEUniqueID: false, ftableGUniqueID: false}
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			if _, excluded := held[n.uniqueID]; excluded {
				held[n.uniqueID] = true
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	for nodeID, found := range held {
		require.True(t, found, "%s not found in any baseline manifest", nodeID)
	}
	t.Logf("seeded prod snapshot with %d nodes (%s and %s excluded)",
		len(prodNodes), ftableEUniqueID, ftableGUniqueID)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// 2. Seed both broken models' :Table nodes in Neo4j so the Locator can
	//    resolve each one's file path and owning service.
	seedFTableETopologyNode(t, ctx, clients)
	seedModelTopologyNodes(t, ctx, clients, topologyModel{
		uniqueID: ftableGUniqueID, schema: "e2e_schema", table: "ftable_g",
		service: otherService, filePath: "models/ftable_g.sql",
	})

	// 3. Drive the release to rejection.
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.Subset(t, failing, []string{ftableEUniqueID, ftableGUniqueID},
		"both deliberately-broken cross-service models must be reported as failing; got %v", failing)
	t.Logf("release %s failing_nodes=%v", releaseID, failing)

	// 4. ONE trigger carrying both failing nodes, from two different services.
	trigger := waitForBatchedTrigger(t, ctx, clients, releaseID,
		[]string{ftableEUniqueID, ftableGUniqueID}, batchTriggerBudget)
	require.Len(t, trigger.Nodes, 2, "the trigger must carry exactly the two cross-service failures; got %v",
		triggerNodeIDs(trigger))
	require.Equal(t, "validation", trigger.Source, "trigger source")

	// 5. ONE proposal for the release, TWO independent edits.
	row := waitForBatchProposal(t, ctx, clients, releaseID, 1, "proposed", batchProposalBudget)
	require.Equal(t, 1, countProposalsForRelease(t, ctx, clients, releaseID),
		"one release yields one fix attempt, even split across two services")

	resolved := decodeNodeIDs(t, row.ResolvedNodeIDs)
	require.Equal(t, []string{ftableEUniqueID, ftableGUniqueID}, resolved,
		"the attempt must address the release's whole cross-service failing set")

	edits := decodeFileEdits(t, row.FileEdits)
	require.Len(t, edits, 2, "two independent cross-service failures are repaired in two files; got %+v", edits)
	targetsByPath := map[string]string{}
	for _, e := range edits {
		targetsByPath[e.Path] = e.TargetNodeID
	}
	require.Equal(t, map[string]string{
		"services/service-2/models/ftable_e.sql": ftableEUniqueID,
		"services/service-3/models/ftable_g.sql": ftableGUniqueID,
	}, targetsByPath, "each edit must name the node whose source it changes, in its own service's path")

	// pr_services is derived from the edits' cluster member ids
	// (agent-remediation/service/proposals/service.go's PRServices), so it is
	// readable straight off the GetProposal RPC once the edits above exist —
	// this is the same computation the create-PR route below drives from.
	proposalPB, err := clients.agentRemediationClient.GetProposal(ctx,
		&remediationv1.GetProposalRequest{Id: row.ID})
	require.NoError(t, err, "GetProposal %s", row.ID)
	require.ElementsMatch(t, []string{changedService, otherService}, proposalPB.GetPrServices(),
		"a proposal split across two services must report both in pr_services; got %v", proposalPB.GetPrServices())

	// 6. TWO verification runs, one per edited service.
	verifications := decodeVerifications(t, row.Verifications)
	require.Len(t, verifications, 2, "two edited services must be verified by two independent verification runs; got %+v", verifications)
	verificationsByService := map[string]pyVerification{}
	for _, v := range verifications {
		require.Equal(t, "dbt", v.Kind, "a dbt model's fix is verified under a dbt manifest")
		verificationsByService[v.Service] = v
	}
	require.Contains(t, verificationsByService, changedService)
	require.Contains(t, verificationsByService, otherService)
	for svc, v := range verificationsByService {
		require.NotEmpty(t, v.RunID, "verification for %s must name the run that judged it", svc)
		assertPipelineNamedVerification(t, ctx, clients, v.RunID, batchVerifyBudget)
		waitForVerificationStatus(t, ctx, clients, v.RunID, "passed", batchVerifyBudget)
		assertNotListedAsRelease(t, ctx, clients, v.RunID)
	}
	t.Logf("✅ two verification runs passed: %s=%s %s=%s",
		changedService, verificationsByService[changedService].RunID,
		otherService, verificationsByService[otherService].RunID)

	// 7. pr_services names both owning-service groups, and the route opens
	//    two pull requests with distinct branches and numbers.
	createPRResp := callCreatePREndpoint(t, ctx, clients.uiBase, row.ID, http.StatusOK)
	require.Empty(t, createPRResp.Errors, "no per-service group should fail to open a pull request; got %+v", createPRResp.Errors)
	require.Len(t, createPRResp.PullRequests, 2,
		"a proposal split across two services must open two pull requests; got %+v", createPRResp.PullRequests)

	byService := map[string]pullRequestResult{}
	for _, pr := range createPRResp.PullRequests {
		byService[pr.Service] = pr
	}
	require.Contains(t, byService, changedService)
	require.Contains(t, byService, otherService)
	require.NotEqual(t, byService[changedService].PRNumber, byService[otherService].PRNumber,
		"the two per-service pull requests must be distinct PRs")

	changedRow := pollChildPRState(t, ctx, clients, row.ID, changedService, "open", 30*time.Second)
	otherRow := pollChildPRState(t, ctx, clients, row.ID, otherService, "open", 30*time.Second)
	require.True(t, strings.HasSuffix(changedRow.Branch, "/"+changedService),
		"the %s pull request's branch must carry the /%s suffix; got %q", changedService, changedService, changedRow.Branch)
	require.True(t, strings.HasSuffix(otherRow.Branch, "/"+otherService),
		"the %s pull request's branch must carry the /%s suffix; got %q", otherService, otherService, otherRow.Branch)
	require.NotEqual(t, changedRow.Branch, otherRow.Branch, "the two per-service pull requests must use distinct branches")
	t.Logf("✅ two pull requests opened: %s=#%d branch=%s, %s=#%d branch=%s",
		changedService, changedRow.PRNumber, changedRow.Branch, otherService, otherRow.PRNumber, otherRow.Branch)

	// 8. Merge the service-2 PR; close the service-3 PR without merging. repo
	//    is a single column on the parent proposal row — the same monorepo
	//    both per-service PRs were opened against.
	var repoName string
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &repoName,
		`SELECT repo FROM proposal WHERE id = $1`, row.ID))

	mergedSHA := mergePullRequestViaStub(t, ctx, repoName, changedRow.PRNumber)
	t.Logf("merged %s PR #%d via stub-github: merge_commit_sha=%s", changedService, changedRow.PRNumber, mergedSHA)
	closePullRequestViaStub(t, ctx, repoName, otherRow.PRNumber)
	t.Logf("closed %s PR #%d via stub-github without merging", otherService, otherRow.PRNumber)

	pollChildPRState(t, ctx, clients, row.ID, changedService, "merged", 60*time.Second)
	pollChildPRState(t, ctx, clients, row.ID, otherService, "rejected", 60*time.Second)

	// 9. Provenance: the merged PR's member gets RESOLVED_BY; the rejected
	//    PR's member gets none; each :PullRequest carries its own terminal
	//    pr_state. pollNeo4jPRState waits out the orchestrator's asynchronous
	//    consumption of remediation.pr_closed:v1 for EACH service
	//    independently — the two PRs close through two separate events.
	pollNeo4jPRState(t, ctx, clients, row.ID, changedService, "merged", 60*time.Second)
	pollNeo4jPRState(t, ctx, clients, row.ID, otherService, "rejected", 60*time.Second)

	mergedResolved := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Proposal {proposal_id: $proposal_id})
		RETURN rb.amended AS amended`,
		map[string]any{"release_id": releaseID, "node_id": ftableEUniqueID, "proposal_id": row.ID})
	require.Len(t, mergedResolved, 1,
		"the merged PR's member %s must have exactly one RESOLVED_BY edge to the shared proposal", ftableEUniqueID)
	require.Equal(t, false, mergedResolved[0]["amended"], "an as-proposed merge's RESOLVED_BY edge must carry amended=false")

	rejectedResolved := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Proposal {proposal_id: $proposal_id})
		RETURN rb.amended AS amended`,
		map[string]any{"release_id": releaseID, "node_id": ftableGUniqueID, "proposal_id": row.ID})
	require.Empty(t, rejectedResolved,
		"the rejected PR's member %s must draw NO RESOLVED_BY edge; got %+v", ftableGUniqueID, rejectedResolved)

	t.Logf("✅ provenance confirmed: %s has RESOLVED_BY (merged), %s has none (rejected)", ftableEUniqueID, ftableGUniqueID)
}

// TestE2E_BatchedRemediation_AmendedMergeMarksProvenanceAmended proves that a
// human edit pushed to a remediation PR's branch before it merges is recorded
// as such all the way into the case-base: the agent-remediation's merge-time
// byte-compare (agent-remediation/service/proposals/amend.go) flags the edit
// amended, and the orchestrator's pr_closed provenance handler carries that
// flag onto both the RESOLVED_BY edge (from the fixed node's :Rejection) and
// the EDITED edge (from the shared :Proposal).
//
// The scenario reuses TestE2E_AgentRemediation_ProposesFixForRejection's
// single-node ftable_e setup (service-2 only) — the amend mechanic itself
// does not depend on batching or cross-service routing, so the simplest
// fixture that reaches a mergeable PR is enough to isolate it.
func TestE2E_BatchedRemediation_AmendedMergeMarksProvenanceAmended(t *testing.T) {
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

	releaseID := "e2e-rem-amend-" + uuid.NewString()[:8]
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s changed_node=%s", releaseID, changedService, ftableEUniqueID)

	// 1. ftable_e is the release's only changed node.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	var prodNodes []map[string]string
	ftableFound := false
	for _, si := range allServices {
		for _, n := range si.nodes {
			if n.uniqueID == ftableEUniqueID {
				ftableFound = true
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, ftableFound, "ftable_e not found in any manifest")
	t.Logf("seeded prod snapshot with %d nodes (ftable_e excluded)", len(prodNodes))

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)
	seedFTableETopologyNode(t, ctx, clients)

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	trigger := waitForBatchedTrigger(t, ctx, clients, releaseID, []string{ftableEUniqueID}, batchTriggerBudget)
	require.Len(t, trigger.Nodes, 1, "the trigger must carry exactly ftable_e; got %v", triggerNodeIDs(trigger))

	row := waitForBatchProposal(t, ctx, clients, releaseID, 1, "proposed", batchProposalBudget)
	resolved := decodeNodeIDs(t, row.ResolvedNodeIDs)
	require.Equal(t, []string{ftableEUniqueID}, resolved, "the attempt must resolve exactly ftable_e")

	edits := decodeFileEdits(t, row.FileEdits)
	require.Len(t, edits, 1, "one failure is repaired by one edit; got %+v", edits)
	require.Equal(t, "services/service-2/models/ftable_e.sql", edits[0].Path, "the edit's path")
	require.Equal(t, ftableEUniqueID, edits[0].TargetNodeID, "the edit's target node")

	verifications := decodeVerifications(t, row.Verifications)
	require.Len(t, verifications, 1, "one edited service, so one verification run; got %+v", verifications)
	assertPipelineNamedVerification(t, ctx, clients, verifications[0].RunID, batchVerifyBudget)
	waitForVerificationStatus(t, ctx, clients, verifications[0].RunID, "passed", batchVerifyBudget)

	// 2. Open the pull request as usual.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID, changedService)
	openRow := pollChildPRState(t, ctx, clients, row.ID, changedService, "open", 5*time.Second)
	require.NotEmpty(t, openRow.Branch, "branch must be recorded before it can be amended")

	var repoName string
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &repoName,
		`SELECT repo FROM proposal WHERE id = $1`, row.ID))

	// 3. Push a SECOND, human-authored commit to the PR branch through the
	//    stub's git-write endpoints, changing the one edited file's content —
	//    before merging, simulating a reviewer amending the PR. The stub does
	//    not inherit a tree's base_tree entries (tests/e2e/stub-github/main.go
	//    records exactly the entries a given POST git/trees call is given), so
	//    every path this commit should resolve to must be listed explicitly;
	//    here that is only the one file this PR touches.
	const amendedContent = `{{ config(materialized='table') }}
SELECT c.id, 'human-amended' AS note
FROM e2e_schema.ftable_c c`
	pushAmendedCommit(t, ctx, repoName, openRow.Branch, []amendedFile{
		{Path: "services/service-2/models/ftable_e.sql", Content: amendedContent},
	})

	// 4. Merge over the amended commit.
	mergeCommitSHA := mergePullRequestViaStub(t, ctx, repoName, prNumber)
	t.Logf("merged amended PR #%d via stub-github: merge_commit_sha=%s", prNumber, mergeCommitSHA)

	closedRow := pollChildPRState(t, ctx, clients, row.ID, changedService, "merged", 60*time.Second)
	require.NotNil(t, closedRow.PRClosedAt, "pr_closed_at must be set on merge")

	closedPayload := latestPRClosedPayload(t, ctx, clients, row.ID, changedService)
	require.Equal(t, "merged", closedPayload.Outcome, "pr_closed payload outcome")
	require.Len(t, closedPayload.Edits, 1, "one edited file; got %+v", closedPayload.Edits)
	require.True(t, closedPayload.Edits[0].Amended,
		"a merge over a human-pushed second commit must be recorded amended=true; got %+v", closedPayload.Edits[0])
	t.Logf("✅ close-loop confirmed: pr_state=merged, edit amended=%v", closedPayload.Edits[0].Amended)

	// 5. Provenance: rb.amended=true on the RESOLVED_BY edge, ed.amended=true
	//    on the EDITED edge. pollNeo4jPRState gates on the SAME Cypher
	//    statement that draws both, so the reads below cannot race the write.
	pollNeo4jPRState(t, ctx, clients, row.ID, changedService, "merged", 60*time.Second)

	resolvedRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Proposal {proposal_id: $proposal_id})
		RETURN rb.amended AS amended`,
		map[string]any{"release_id": releaseID, "node_id": ftableEUniqueID, "proposal_id": row.ID})
	require.Len(t, resolvedRows, 1, "ftable_e must have exactly one RESOLVED_BY edge to the proposal")
	require.Equal(t, true, resolvedRows[0]["amended"], "an amended merge's RESOLVED_BY edge must carry amended=true")

	editRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[e:EDITED]->(t:Table {unique_id: $node_id})
		RETURN e.amended AS amended`,
		map[string]any{"proposal_id": row.ID, "node_id": ftableEUniqueID})
	require.Len(t, editRows, 1, "ftable_e must have exactly one EDITED edge from the proposal")
	require.Equal(t, true, editRows[0]["amended"], "an amended merge's EDITED edge must carry amended=true")

	t.Logf("✅ amended-merge provenance confirmed: rb.amended=true, ed.amended=true, pr_state=merged")
}

// batchProposalRow is the batched view of a proposal row: the set the attempt
// addressed, each node's outcome, every file it changed, and every
// verification run that judged it. The single-node projection is proposalRow in
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
	}, fmt.Sprintf("timeout waiting for one %s for release %s carrying %v",
		streams.RemediationRequestedV2, releaseID, nodeIDs))
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
	}, fmt.Sprintf("timeout waiting for %s for release %s", streams.RemediationProposedV1, releaseID))
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

// openRemediationPR opens the proposal's pull request(s) through the UI route
// and returns the single entry's PR number, asserting a single-service
// proposal opens exactly ONE pull request, attributed to service, whose
// branch carries the deterministic /<service> suffix. It also asserts the
// route is idempotent: a second POST must return the SAME pull_requests set —
// not open a second pull request — since every group that already has one
// resolves to a reported success rather than a per-group error, so the whole
// route still answers 200, not 409.
func openRemediationPR(
	t *testing.T, ctx context.Context, clients *testClients, proposalID, releaseID, service string,
) int {
	t.Helper()
	created := callCreatePREndpoint(t, ctx, clients.uiBase, proposalID, http.StatusOK)
	require.Empty(t, created.Errors, "no per-service group should fail to open a pull request; got %+v", created.Errors)
	require.Len(t, created.PullRequests, 1,
		"a single-service proposal must open exactly one pull request; got %+v", created.PullRequests)
	pr := created.PullRequests[0]
	require.Equal(t, service, pr.Service, "the pull request must be attributed to the proposal's owning service")
	require.NotEmpty(t, pr.PRUrl, "pr_url must be non-empty on first create")
	require.Greater(t, pr.PRNumber, 0, "pr_number must be a positive stub-github PR number")

	dup := callCreatePREndpoint(t, ctx, clients.uiBase, proposalID, http.StatusOK)
	require.Empty(t, dup.Errors, "a duplicate POST must not report any per-service failure; got %+v", dup.Errors)
	require.Equal(t, created.PullRequests, dup.PullRequests,
		"a second POST must return the SAME pull request set, not open another one for release %s", releaseID)

	row := pollChildPRState(t, ctx, clients, proposalID, service, "open", 30*time.Second)
	require.Equal(t, pr.PRNumber, row.PRNumber, "the row must record the number the create call returned")
	require.True(t, strings.HasSuffix(row.Branch, "/"+service),
		"a per-service pull request's branch must carry the /<service> suffix; got %q", row.Branch)
	return pr.PRNumber
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
