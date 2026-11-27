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
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
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

// ftableGUniqueID is service-3's fixture model for the cross-project batching
// test: it joins public.wrong_name_2, a relation that does not exist, so it
// fails validation independently of any service-2 failure. It is otherwise
// reserved by build_operation_test.go / failure_test.go / rebase_test.go for a
// scheduled-execution-failure fixture (schedule "e2e-schedule-failure"); that
// use is unaffected here because a remediation PR's merge never writes back
// to the real dbt/projects/service-3/models/ftable_g.sql file — the whole PR
// lifecycle, including the merge, is mediated by stub-github and never
// touches the checked-out fixture.
const ftableGUniqueID = "e2e_schema.ftable_g"

// Budgets shared by both batched-remediation tests. Each drives two full
// release checks in kind — the rejected candidate release, then the
// release check that verifies the proposed fix — with one or two model
// calls in between, so the ceilings are sized for a cold-ish stack rather
// than for the ~3 minutes a warm one takes.
const (
	batchCtxBudget     = 35 * time.Minute
	batchRejectBudget  = 10 * time.Minute
	batchTriggerBudget = 3 * time.Minute
	batchFixBudget     = 20 * time.Minute
	batchEventBudget   = 3 * time.Minute
	batchVerifyBudget  = 2 * time.Minute
)

// TestE2E_BatchedRemediation_TwoIndependentFailuresOnePullRequest proves that a
// release is remediated as one unit even when its failures have nothing in
// common. ftable_e and ftable_k both reference a relation that does not exist,
// and neither descends from a node this release changed, so the runner forms
// two independent clusters, each worked by the dbt-validation type — yet
// everything downstream of the grouping stays singular:
//
//	POST /releases (service-2; ftable_e and ftable_k are both new)
//	→ validation fails on both nodes → release "rejected"
//	→ ONE heal.triggered:v1 carrying both nodes, each with no changed ancestor
//	→ ONE fix row for the release (attempt 1) holding TWO file edits,
//	  each naming the node whose source it repairs
//	→ ONE dbt release check (both edits belong to service-2) that passes
//	→ the runner's gate poller reads the verdict and the attempt moves
//	  verifying → proposed
//	→ ONE fix.proposed:v1
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

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-batch-" + uuid.NewString()[:8]
	changedProject := "service-2"
	t.Logf("release_id=%s changed_project=%s changed_nodes=%s,%s",
		releaseID, changedProject, ftableEUniqueID, ftableKUniqueID)

	// 1. Build the live release from the baseline manifests, holding back both
	//    broken models so the derived changed set is exactly {ftable_e, ftable_k}.
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allProjects[changedProject].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed the production pointers", changedProject)

	held := map[string]bool{ftableEUniqueID: false, ftableKUniqueID: false}
	var prodNodes []map[string]string
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			if _, excluded := held[n.uniqueID]; excluded {
				held[n.uniqueID] = true
				continue // left out of the live release → a changed node of this release
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"project":      svc,
				"content_hash": n.contentHash,
			})
		}
	}
	for nodeID, found := range held {
		require.True(t, found,
			"%s not found in any baseline manifest — is the model in service-2 and the image rebuilt?", nodeID)
	}
	t.Logf("seeded the live release with %d nodes (%s and %s excluded)",
		len(prodNodes), ftableEUniqueID, ftableKUniqueID)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.HealTriggeredV1)
	proposedTap := startStreamTap(t, ctx, clients.redisClient, streams.FixProposedV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedLiveRelease(t, ctx, clients, prodNodes)
	seedProductionPointers(t, ctx, clients, allProjects)

	// 2. Place both broken models in the graph. The clusters' steps read the
	//    failing node's location off the trigger, but the runner's
	//    upstream_changes and failure_precedents steps also ask the graph for
	//    each node's evidence, so a node the promoted graph cannot place
	//    degrades the prompt silently.
	seedFTableETopologyNode(t, ctx, clients)
	seedModelTopologyNodes(t, ctx, clients, topologyModel{
		uniqueID: ftableKUniqueID, schema: "e2e_schema", table: "ftable_k",
		project: changedProject, filePath: "models/ftable_k.sql",
	})

	// 3. Drive the release to rejection. Both held-back models reference
	//    public.wrong_name, so both fail validation.
	postRelease(t, clients, changedProject, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.Subset(t, failing, []string{ftableEUniqueID, ftableKUniqueID},
		"both deliberately-broken models must be reported as failing; got %v", failing)
	t.Logf("release %s failing_nodes=%v", releaseID, failing)

	// 4. ONE trigger for the release, carrying BOTH failing nodes. This is the
	//    batching contract at its source: the classifier emits per release, not
	//    per node, so a second message here would mean two attempts, two
	//    fixes and two pull requests downstream.
	trigger := waitForBatchedTrigger(t, ctx, requestedTap, releaseID,
		[]string{ftableEUniqueID, ftableKUniqueID}, batchTriggerBudget)
	require.Len(t, triggersForRelease(t, requestedTap, releaseID), 1,
		"a rejected release must produce exactly one %s", streams.HealTriggeredV1)
	require.Len(t, trigger.Nodes, 2,
		"the trigger must carry exactly the two failing models; got %v", triggerNodeIDs(trigger))
	require.Equal(t, string(model.RemediationSourceValidation), trigger.Source, "trigger source")

	for _, nodeID := range []string{ftableEUniqueID, ftableKUniqueID} {
		node, ok := trigger.findNode(nodeID)
		require.True(t, ok, "trigger must carry an entry for %s", nodeID)
		require.Nil(t, node.Unfixable, "%s fails on a missing relation, which a fix can reach", nodeID)
		require.Empty(t, node.ChangedAncestors,
			"%s fails on its own body, not below a changed ancestor, so it must name none; got %v",
			nodeID, node.ChangedAncestors)
		require.NotEmpty(t, node.ErrorSignature, "%s must carry an error_signature", nodeID)
	}
	t.Logf("✅ one %s carries both independent failures", streams.HealTriggeredV1)

	// 5. ONE fix row for the whole release, reaching 'proposed' only after
	//    its release check has judged the fix. The representative node_id is
	//    the lowest resolved id, so the row is addressed by ftable_e.
	row := waitForBatchFix(t, ctx, clients, releaseID, 1, "proposed", batchFixBudget)
	require.Equal(t, 1, countFixesForRelease(t, ctx, clients, releaseID),
		"one release yields one attempt, not one per failing node")

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

	// 7. Both edits belong to service-2, so ONE release check verified the
	//    whole attempt — and it really ran: the release queue names it while it is
	//    active, it reaches 'passed', and it is never readable as a release.
	releaseChecks := decodeReleaseChecks(t, row.ReleaseChecks)
	require.Len(t, releaseChecks, 1,
		"both files belong to service-2, so one release check judged the attempt; got %+v", releaseChecks)
	require.Equal(t, "dbt", releaseChecks[0].Kind, "a dbt model's fix is verified under a dbt manifest")
	require.Equal(t, changedProject, releaseChecks[0].Project, "release check project")
	require.NotEmpty(t, releaseChecks[0].CheckID, "a release check entry must name the check that judged it")
	assertReleaseQueueNamedFix(t, ctx, clients, releaseChecks[0].CheckID, batchVerifyBudget)
	waitForReleaseCheckStatus(t, ctx, clients, releaseChecks[0].CheckID, "passed", batchVerifyBudget)
	assertNotListedAsRelease(t, ctx, clients, releaseChecks[0].CheckID)
	t.Logf("✅ one release check %s passed for both edits", releaseChecks[0].CheckID)

	// 8. ONE announcement, carrying the same batched view as the row.
	proposed := waitForBatchProposedEvent(t, ctx, proposedTap, releaseID, batchEventBudget)
	require.Len(t, proposedEventsForRelease(t, proposedTap, releaseID), 1,
		"one verified attempt announces itself once")
	require.Equal(t, []string{ftableEUniqueID, ftableKUniqueID}, proposed.ResolvedNodeIDs,
		"%s must announce the whole resolved set", streams.FixProposedV1)
	require.Len(t, proposed.Edits, 2, "the announcement must carry both file edits; got %+v", proposed.Edits)
	require.Equal(t, ftableEUniqueID, proposed.NodeID,
		"the representative node is the lowest resolved id")

	// 9. ONE pull request for the release, named for the set it fixes. Both
	//    edits belong to service-2 and now carry cluster member ids, so
	//    PRProjects attributes the single group to that real project — the
	//    title carries its " (service-2)" suffix, the same way it would for
	//    any fix split across more than the legacy "" group.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID, changedProject)
	pr := fetchStubPullRequest(t, ctx, row.Repo, prNumber)
	require.Equal(t, fmt.Sprintf("[remediation] fix 2 nodes (release %s) (%s)", releaseID, changedProject), pr.Title,
		"a batched pull request must name how many nodes it fixes and the project it was opened for")
	t.Logf("✅ one pull request #%d: %q", pr.Number, pr.Title)

	// 10. Opening the pull request must not have minted a second attempt: the
	//     release is still one fix, reviewed once.
	require.Equal(t, 1, countFixesForRelease(t, ctx, clients, releaseID),
		"no second fix row may appear for a release whose single attempt was accepted")
}

// TestE2E_BatchedRemediation_SharedUpstreamFixedOnce proves that failures with
// one cause are repaired once, in the cause. ftable_u is seeded as the release's
// only changed node (its production content_hash is replaced with a stale
// value), and its new body drops the amount column that ftable_v and ftable_w
// both read. Neither v nor w changed, so neither is at fault:
//
//	POST /releases (service-2; ftable_u is the only changed node)
//	→ validation fails on ftable_v and ftable_w, identically
//	→ ONE heal.triggered:v1 whose two entries share one error signature
//	  and both name ftable_u as their changed ancestor
//	→ the runner groups them into ONE shared-upstream cluster targeting ftable_u
//	→ ONE fix with ONE edit — to ftable_u's source, a node that never failed
//	→ ONE dbt release check that passes for the repaired ancestor and its
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

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-up-" + uuid.NewString()[:8]
	changedProject := "service-2"
	t.Logf("release_id=%s changed_project=%s changed_node=%s expected_failures=%s,%s",
		releaseID, changedProject, ftableUUniqueID, ftableVUniqueID, ftableWUniqueID)

	// 1. Seed the live release with EVERY baseline node, but give ftable_u a stale
	//    content_hash. It is then the release's only changed node, while its two
	//    descendants keep their real hashes and are pulled into validation only
	//    because they descend from it — which is precisely the shape a
	//    shared-upstream cause has in production.
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allProjects[changedProject].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed the production pointers", changedProject)

	seen := map[string]bool{}
	var prodNodes []map[string]string
	for svc, si := range allProjects {
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
				"project":      svc,
				"content_hash": hash,
			})
		}
	}
	for _, nodeID := range []string{ftableUUniqueID, ftableVUniqueID, ftableWUniqueID} {
		require.True(t, seen[nodeID],
			"%s not found in any baseline manifest — is the model in service-2 and the image rebuilt?", nodeID)
	}
	t.Logf("seeded the live release with %d nodes (%s hash staled)", len(prodNodes), ftableUUniqueID)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.HealTriggeredV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedLiveRelease(t, ctx, clients, prodNodes)
	seedProductionPointers(t, ctx, clients, allProjects)

	// 2. Place the changed ancestor in the graph. The shared-upstream type edits
	//    a node that never failed; its location travels on the trigger (each
	//    failing node's changed_ancestors carry the path THIS candidate declares),
	//    and the promoted graph is only the fallback for a rejection that carries
	//    none. The node is still seeded so the graph reads of the type's other
	//    steps know the ancestor at all.
	seedModelTopologyNodes(t, ctx, clients, topologyModel{
		uniqueID: ftableUUniqueID, schema: "e2e_schema", table: "ftable_u",
		project: changedProject, filePath: "models/ftable_u.sql",
	})

	postRelease(t, clients, changedProject, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	// 3. The descendants failed; the ancestor that caused it did not.
	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.Equal(t, []string{ftableVUniqueID, ftableWUniqueID}, failing,
		"only the two descendants that read the dropped column may fail; got %v", failing)
	t.Logf("release %s failing_nodes=%v", releaseID, failing)

	trigger := waitForBatchedTrigger(t, ctx, requestedTap, releaseID,
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
	for _, n := range []pkgevents.HealTriggeredNode{vNode, wNode} {
		require.Nil(t, n.Unfixable, "%s fails on a dropped column, which a fix can reach", n.NodeID)
		require.Equal(t, []string{ftableUUniqueID}, ancestorIDs(n),
			"%s failed below the one node this release changed, so the trigger must say so", n.NodeID)
		require.Equal(t, "models/ftable_u.sql", n.ChangedAncestors[0].FilePath,
			"the ancestor must carry the path THIS candidate declares — the file the upstream fix edits")
		require.Equal(t, changedProject, n.ChangedAncestors[0].Project, "the ancestor's owning project")
		require.Equal(t, 1, n.ChangedAncestors[0].Depth, "ftable_u is a direct upstream of %s", n.NodeID)
	}
	t.Logf("✅ one %s names %s as the shared cause of both failures", streams.HealTriggeredV1, ftableUUniqueID)

	// 6. ONE fix, and — the point of the whole scenario — ONE edit, to a
	//    node that never appeared in the failing set.
	row := waitForBatchFix(t, ctx, clients, releaseID, 1, "proposed", batchFixBudget)
	require.Equal(t, 1, countFixesForRelease(t, ctx, clients, releaseID),
		"one release yields one attempt")

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

	// 7. The single edit was verified by a real release check.
	releaseChecks := decodeReleaseChecks(t, row.ReleaseChecks)
	require.Len(t, releaseChecks, 1,
		"one edited project, so one release check; got %+v", releaseChecks)
	require.Equal(t, "dbt", releaseChecks[0].Kind, "a dbt model's fix is verified under a dbt manifest")
	require.Equal(t, changedProject, releaseChecks[0].Project, "release check project")
	assertReleaseQueueNamedFix(t, ctx, clients, releaseChecks[0].CheckID, batchVerifyBudget)
	waitForReleaseCheckStatus(t, ctx, clients, releaseChecks[0].CheckID, "passed", batchVerifyBudget)
	assertNotListedAsRelease(t, ctx, clients, releaseChecks[0].CheckID)
	t.Logf("✅ release check %s passed for the upstream fix", releaseChecks[0].CheckID)

	// 8. The proposed source really restores the column both descendants read —
	//    the release check could not have passed otherwise, but asserting it
	//    names what the fix was, not just that one existed.
	contentKey := stripS3Prefix(edits[0].ContentURI)
	require.NotEmpty(t, contentKey, "could not parse key from content_uri=%s", edits[0].ContentURI)
	body := string(getS3ObjectByKey(t, ctx, clients, contentKey))
	require.Contains(t, body, "0 AS amount",
		"the repaired ancestor must give back the amount column its change dropped; got %q", strings.TrimSpace(body))
	t.Logf("proposed upstream source at %s: %q", edits[0].ContentURI, strings.TrimSpace(body))

	// 9. ONE pull request, naming the two failures the one edit resolves. The
	//    edit's cluster carries member node ids, so PRProjects attributes it
	//    to the real owning project (service-2) rather than the legacy ""
	//    group, and the title carries that project's suffix.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID, changedProject)
	pr := fetchStubPullRequest(t, ctx, row.Repo, prNumber)
	require.Equal(t, fmt.Sprintf("[remediation] fix 2 nodes (release %s) (%s)", releaseID, changedProject), pr.Title,
		"the title names the failing nodes resolved and the project it was opened for, not the single file changed")
	t.Logf("✅ one pull request #%d fixes two nodes with one edit: %q", pr.Number, pr.Title)

	// 10. Merging the pull request AS PROPOSED — no further human edit — must
	//     draw the case-base provenance edges catalog's pr_closed
	//     consumer maintains: one RESOLVED_BY per resolved descendant to the
	//     shared :Fix, and one EDITED edge from that :Fix to the
	//     changed ancestor's :Node, both carrying amended=false. The stub
	//     serves the PR's own written commit at the merge sha (nothing pushes
	//     a further commit in this test), so the merged content is
	//     byte-identical to what was proposed.
	mergeCommitSHA := mergePullRequestViaStub(t, ctx, row.Repo, prNumber)
	t.Logf("merged PR #%d via stub-github: merge_commit_sha=%s", prNumber, mergeCommitSHA)

	closedRow := pollChildPRState(t, ctx, clients, row.ID, changedProject, "merged", 60*time.Second)
	require.NotNil(t, closedRow.PRClosedAt, "pr_closed_at must be set on merge")

	closedPayload := latestPRClosedPayload(t, ctx, clients, row.ID, changedProject)
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
	//     catalog's asynchronous consumption of
	//     fix.pr_closed:v1 to land; RecordPullRequestOutcome draws the
	//     pr_state change and every edge below in the SAME Cypher statement, so
	//     once the poll observes 'merged' the edges are already committed and
	//     the plain reads that follow cannot race the write.
	pollNeo4jPRState(t, ctx, clients, row.ID, changedProject, "merged", 60*time.Second)

	resolvedRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {tenant_id: 'default', release_id: $release_id})-[rb:RESOLVED_BY]->(p:Fix {tenant_id: 'default', fix_id: $fix_id})
		RETURN count(r) AS n, collect(DISTINCT rb.amended) AS amended`,
		map[string]any{"release_id": releaseID, "fix_id": row.ID})
	require.Len(t, resolvedRows, 1, "expected one summary row for the RESOLVED_BY query")
	require.EqualValues(t, 2, resolvedRows[0]["n"],
		"one RESOLVED_BY per resolved node (%s, %s); got %+v", ftableVUniqueID, ftableWUniqueID, resolvedRows[0])
	require.Equal(t, []any{false}, resolvedRows[0]["amended"],
		"every RESOLVED_BY drawn by an as-proposed merge must carry amended=false; got %+v", resolvedRows[0])

	editRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (p:Fix {tenant_id: 'default', fix_id: $fix_id})-[e:EDITED]->(t:Node {tenant_id: 'default'})
		RETURN t.unique_id AS unique_id, e.amended AS amended`,
		map[string]any{"fix_id": row.ID})
	require.Len(t, editRows, 1, "the shared-upstream attempt drew one EDITED edge, to the ancestor it repaired; got %+v", editRows)
	require.Equal(t, ftableUUniqueID, editRows[0]["unique_id"], "EDITED must target the changed ancestor, not either failing descendant")
	require.Equal(t, false, editRows[0]["amended"], "an as-proposed merge's EDITED edge must carry amended=false")
	t.Logf("✅ provenance confirmed: 2 RESOLVED_BY edges (amended=false), EDITED -> %s (amended=false), pr_state=merged",
		ftableUUniqueID)
}

// TestE2E_BatchedRemediation_TwoProjectsTwoPullRequests proves that a release
// whose failing set spans two projects opens two independent pull requests —
// one per owning project — and that each is reviewed and closed on its own:
// merging one draws case-base provenance for its members while the other,
// closed without merging, draws none.
//
// ftable_e (service-2) and ftable_g (service-3) are both held back from
// the live release, so a release posted for service-2 sees BOTH as changed — the
// same cross-project assembly TestE2E_ReleasePromote_GatedCrossProjectUpstream
// exercises with a genuine dependency edge, here with none: the two failures
// share no ancestor and no error signature (they reference different
// nonexistent relations), so the runner forms two independent clusters, one
// edit each, grouped by owning project into two pull requests:
//
//	POST /releases (service-2; ftable_e and ftable_g both held back from the live release)
//	→ validation fails on both nodes → release "rejected"
//	→ ONE heal.triggered:v1 carrying both nodes
//	→ ONE fix row (attempt 1) holding TWO file edits, one per project
//	→ TWO dbt release checks (one per edited project) that both pass
//	→ ONE fix.proposed:v1
//	→ POST /fixes/:id/pull-request opens TWO pull requests, distinct
//	  branches and numbers, one per project
//	→ merge the service-2 PR, close-without-merging the service-3 PR
//	→ the merged PR's member (ftable_e) gets a RESOLVED_BY edge; the rejected
//	  PR's member (ftable_g) gets none; each :PullRequest node carries its own
//	  terminal pr_state
func TestE2E_BatchedRemediation_TwoProjectsTwoPullRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), batchCtxBudget)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-2svc-" + uuid.NewString()[:8]
	changedProject := "service-2"
	otherProject := "service-3"
	t.Logf("release_id=%s changed_project=%s other_project=%s changed_nodes=%s,%s",
		releaseID, changedProject, otherProject, ftableEUniqueID, ftableGUniqueID)

	// 1. Hold BOTH ftable_e (service-2) and ftable_g (service-3) back from
	//    the live release. Neither project's own manifest is what makes them
	//    "changed" — that comes purely from their absence in the live release
	//    against the assembled candidate topology, which is why this works
	//    for ftable_g even though the release is posted for service-2, not
	//    service-3 (see TestE2E_ReleasePromote_GatedCrossProjectUpstream's
	//    identical assembly for a cross-project pair WITH a dependency edge —
	//    here there is none, so the two failures stay independent).
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allProjects[changedProject].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed the production pointers", changedProject)

	held := map[string]bool{ftableEUniqueID: false, ftableGUniqueID: false}
	var prodNodes []map[string]string
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			if _, excluded := held[n.uniqueID]; excluded {
				held[n.uniqueID] = true
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"project":      svc,
				"content_hash": n.contentHash,
			})
		}
	}
	for nodeID, found := range held {
		require.True(t, found, "%s not found in any baseline manifest", nodeID)
	}
	t.Logf("seeded the live release with %d nodes (%s and %s excluded)",
		len(prodNodes), ftableEUniqueID, ftableGUniqueID)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.HealTriggeredV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedLiveRelease(t, ctx, clients, prodNodes)
	seedProductionPointers(t, ctx, clients, allProjects)

	// The other project's release check needs the rejected project's baseline
	// pointer to cover every project live in the live release at activation.
	var baselineManifest string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT manifest_s3_key FROM production_pointers WHERE project_name = $1`, changedProject).Scan(&baselineManifest))
	require.Equal(t, "s3://continuo/service-2/e2e-baseline/manifest.json", baselineManifest)

	// 2. Seed both broken models' :Node nodes in Neo4j so the Locator can
	//    resolve each one's file path and owning project.
	seedFTableETopologyNode(t, ctx, clients)
	seedModelTopologyNodes(t, ctx, clients, topologyModel{
		uniqueID: ftableGUniqueID, schema: "e2e_schema", table: "ftable_g",
		project: otherProject, filePath: "models/ftable_g.sql",
	})

	// 3. Drive the release to rejection.
	postRelease(t, clients, changedProject, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.Subset(t, failing, []string{ftableEUniqueID, ftableGUniqueID},
		"both deliberately-broken cross-project models must be reported as failing; got %v", failing)
	t.Logf("release %s failing_nodes=%v", releaseID, failing)

	// 4. ONE trigger carrying both failing nodes, from two different projects.
	trigger := waitForBatchedTrigger(t, ctx, requestedTap, releaseID,
		[]string{ftableEUniqueID, ftableGUniqueID}, batchTriggerBudget)
	require.Len(t, trigger.Nodes, 2, "the trigger must carry exactly the two cross-project failures; got %v",
		triggerNodeIDs(trigger))
	require.Equal(t, string(model.RemediationSourceValidation), trigger.Source, "trigger source")

	// 5. ONE fix for the release, TWO independent edits.
	row := waitForBatchFix(t, ctx, clients, releaseID, 1, "proposed", batchFixBudget)
	require.Equal(t, 1, countFixesForRelease(t, ctx, clients, releaseID),
		"one release yields one attempt, even split across two projects")

	resolved := decodeNodeIDs(t, row.ResolvedNodeIDs)
	require.Equal(t, []string{ftableEUniqueID, ftableGUniqueID}, resolved,
		"the attempt must address the release's whole cross-project failing set")

	edits := decodeFileEdits(t, row.FileEdits)
	require.Len(t, edits, 2, "two independent cross-project failures are repaired in two files; got %+v", edits)
	targetsByPath := map[string]string{}
	for _, e := range edits {
		targetsByPath[e.Path] = e.TargetNodeID
	}
	require.Equal(t, map[string]string{
		"services/service-2/models/ftable_e.sql": ftableEUniqueID,
		"services/service-3/models/ftable_g.sql": ftableGUniqueID,
	}, targetsByPath, "each edit must name the node whose source it changes, in its own project's path")

	// pr_projects is derived from the edits' cluster member ids
	// (agent-remediation/service/fixes/service.go's PRProjects), so it is
	// readable straight off the GetFix RPC once the edits above exist —
	// this is the same computation the create-PR route below drives from.
	fixPB, err := clients.agentRemediationClient.GetFix(ctx,
		&remediationv1.GetFixRequest{Id: row.ID})
	require.NoError(t, err, "GetFix %s", row.ID)
	require.ElementsMatch(t, []string{changedProject, otherProject}, fixPB.GetPrProjects(),
		"a fix split across two projects must report both in pr_projects; got %v", fixPB.GetPrProjects())

	// 6. TWO release checks, one per edited project.
	releaseChecks := decodeReleaseChecks(t, row.ReleaseChecks)
	require.Len(t, releaseChecks, 2, "two edited projects must be verified by two independent release checks; got %+v", releaseChecks)
	checksByProject := map[string]pyReleaseCheck{}
	for _, v := range releaseChecks {
		require.Equal(t, "dbt", v.Kind, "a dbt model's fix is verified under a dbt manifest")
		checksByProject[v.Project] = v
	}
	require.Contains(t, checksByProject, changedProject)
	require.Contains(t, checksByProject, otherProject)
	for svc, v := range checksByProject {
		require.NotEmpty(t, v.CheckID, "the release check for %s must name the check that judged it", svc)
		assertReleaseQueueNamedFix(t, ctx, clients, v.CheckID, batchVerifyBudget)
		waitForReleaseCheckStatus(t, ctx, clients, v.CheckID, "passed", batchVerifyBudget)
		assertNotListedAsRelease(t, ctx, clients, v.CheckID)
	}
	t.Logf("✅ two release checks passed: %s=%s %s=%s",
		changedProject, checksByProject[changedProject].CheckID,
		otherProject, checksByProject[otherProject].CheckID)

	// 7. pr_projects names both owning-project groups, and the route opens
	//    two pull requests with distinct branches and numbers.
	createPRResp := callCreatePREndpoint(t, ctx, clients.uiBase, row.ID, http.StatusOK)
	require.Empty(t, createPRResp.Errors, "no per-project group should fail to open a pull request; got %+v", createPRResp.Errors)
	require.Len(t, createPRResp.PullRequests, 2,
		"a fix split across two projects must open two pull requests; got %+v", createPRResp.PullRequests)

	byProject := map[string]pullRequestResult{}
	for _, pr := range createPRResp.PullRequests {
		byProject[pr.Project] = pr
	}
	require.Contains(t, byProject, changedProject)
	require.Contains(t, byProject, otherProject)
	require.NotEqual(t, byProject[changedProject].PRNumber, byProject[otherProject].PRNumber,
		"the two per-project pull requests must be distinct PRs")

	changedRow := pollChildPRState(t, ctx, clients, row.ID, changedProject, "open", 30*time.Second)
	otherRow := pollChildPRState(t, ctx, clients, row.ID, otherProject, "open", 30*time.Second)
	require.True(t, strings.HasSuffix(changedRow.Branch, "/"+changedProject),
		"the %s pull request's branch must carry the /%s suffix; got %q", changedProject, changedProject, changedRow.Branch)
	require.True(t, strings.HasSuffix(otherRow.Branch, "/"+otherProject),
		"the %s pull request's branch must carry the /%s suffix; got %q", otherProject, otherProject, otherRow.Branch)
	require.NotEqual(t, changedRow.Branch, otherRow.Branch, "the two per-project pull requests must use distinct branches")
	t.Logf("✅ two pull requests opened: %s=#%d branch=%s, %s=#%d branch=%s",
		changedProject, changedRow.PRNumber, changedRow.Branch, otherProject, otherRow.PRNumber, otherRow.Branch)

	// 8. Merge the service-2 PR; close the service-3 PR without merging. repo
	//    is a single column on the parent fix row — the same monorepo
	//    both per-project PRs were opened against.
	var repoName string
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &repoName,
		`SELECT repo FROM fix WHERE id = $1`, row.ID))

	mergedSHA := mergePullRequestViaStub(t, ctx, repoName, changedRow.PRNumber)
	t.Logf("merged %s PR #%d via stub-github: merge_commit_sha=%s", changedProject, changedRow.PRNumber, mergedSHA)
	closePullRequestViaStub(t, ctx, repoName, otherRow.PRNumber)
	t.Logf("closed %s PR #%d via stub-github without merging", otherProject, otherRow.PRNumber)

	pollChildPRState(t, ctx, clients, row.ID, changedProject, "merged", 60*time.Second)
	pollChildPRState(t, ctx, clients, row.ID, otherProject, "rejected", 60*time.Second)

	// 9. Provenance: the merged PR's member gets RESOLVED_BY; the rejected
	//    PR's member gets none; each :PullRequest carries its own terminal
	//    pr_state. pollNeo4jPRState waits out catalog's asynchronous
	//    consumption of fix.pr_closed:v1 for EACH project
	//    independently — the two PRs close through two separate events.
	pollNeo4jPRState(t, ctx, clients, row.ID, changedProject, "merged", 60*time.Second)
	pollNeo4jPRState(t, ctx, clients, row.ID, otherProject, "rejected", 60*time.Second)

	mergedResolved := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {tenant_id: 'default', release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Fix {tenant_id: 'default', fix_id: $fix_id})
		RETURN rb.amended AS amended`,
		map[string]any{"release_id": releaseID, "node_id": ftableEUniqueID, "fix_id": row.ID})
	require.Len(t, mergedResolved, 1,
		"the merged PR's member %s must have exactly one RESOLVED_BY edge to the shared fix", ftableEUniqueID)
	require.Equal(t, false, mergedResolved[0]["amended"], "an as-proposed merge's RESOLVED_BY edge must carry amended=false")

	rejectedResolved := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {tenant_id: 'default', release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Fix {tenant_id: 'default', fix_id: $fix_id})
		RETURN rb.amended AS amended`,
		map[string]any{"release_id": releaseID, "node_id": ftableGUniqueID, "fix_id": row.ID})
	require.Empty(t, rejectedResolved,
		"the rejected PR's member %s must draw NO RESOLVED_BY edge; got %+v", ftableGUniqueID, rejectedResolved)

	t.Logf("✅ provenance confirmed: %s has RESOLVED_BY (merged), %s has none (rejected)", ftableEUniqueID, ftableGUniqueID)
}

// TestE2E_BatchedRemediation_AmendedMergeMarksProvenanceAmended proves that a
// human edit pushed to a remediation PR's branch before it merges is recorded
// as such all the way into the case-base: the agent-remediation's merge-time
// byte-compare (agent-remediation/service/fixes/amend.go) flags the edit
// amended, and catalog's pr_closed provenance handler carries that
// flag onto both the RESOLVED_BY edge (from the fixed node's :Rejection) and
// the EDITED edge (from the shared :Fix).
//
// The scenario reuses TestE2E_AgentRemediation_ProposesFixForRejection's
// single-node ftable_e setup (service-2 only) — the amend mechanic itself
// does not depend on batching or cross-project routing, so the simplest
// fixture that reaches a mergeable PR is enough to isolate it.
func TestE2E_BatchedRemediation_AmendedMergeMarksProvenanceAmended(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), batchCtxBudget)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-amend-" + uuid.NewString()[:8]
	changedProject := "service-2"
	t.Logf("release_id=%s changed_project=%s changed_node=%s", releaseID, changedProject, ftableEUniqueID)

	// 1. ftable_e is the release's only changed node.
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allProjects[changedProject].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed the production pointers", changedProject)

	var prodNodes []map[string]string
	ftableFound := false
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			if n.uniqueID == ftableEUniqueID {
				ftableFound = true
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"project":      svc,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, ftableFound, "ftable_e not found in any manifest")
	t.Logf("seeded the live release with %d nodes (ftable_e excluded)", len(prodNodes))

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.HealTriggeredV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedLiveRelease(t, ctx, clients, prodNodes)
	seedProductionPointers(t, ctx, clients, allProjects)
	seedFTableETopologyNode(t, ctx, clients)

	postRelease(t, clients, changedProject, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	trigger := waitForBatchedTrigger(t, ctx, requestedTap, releaseID, []string{ftableEUniqueID}, batchTriggerBudget)
	require.Len(t, trigger.Nodes, 1, "the trigger must carry exactly ftable_e; got %v", triggerNodeIDs(trigger))

	row := waitForBatchFix(t, ctx, clients, releaseID, 1, "proposed", batchFixBudget)
	resolved := decodeNodeIDs(t, row.ResolvedNodeIDs)
	require.Equal(t, []string{ftableEUniqueID}, resolved, "the attempt must resolve exactly ftable_e")

	edits := decodeFileEdits(t, row.FileEdits)
	require.Len(t, edits, 1, "one failure is repaired by one edit; got %+v", edits)
	require.Equal(t, "services/service-2/models/ftable_e.sql", edits[0].Path, "the edit's path")
	require.Equal(t, ftableEUniqueID, edits[0].TargetNodeID, "the edit's target node")

	releaseChecks := decodeReleaseChecks(t, row.ReleaseChecks)
	require.Len(t, releaseChecks, 1, "one edited project, so one release check; got %+v", releaseChecks)
	assertReleaseQueueNamedFix(t, ctx, clients, releaseChecks[0].CheckID, batchVerifyBudget)
	waitForReleaseCheckStatus(t, ctx, clients, releaseChecks[0].CheckID, "passed", batchVerifyBudget)

	// 2. Open the pull request as usual.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID, changedProject)
	openRow := pollChildPRState(t, ctx, clients, row.ID, changedProject, "open", 5*time.Second)
	require.NotEmpty(t, openRow.Branch, "branch must be recorded before it can be amended")

	var repoName string
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &repoName,
		`SELECT repo FROM fix WHERE id = $1`, row.ID))

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

	closedRow := pollChildPRState(t, ctx, clients, row.ID, changedProject, "merged", 60*time.Second)
	require.NotNil(t, closedRow.PRClosedAt, "pr_closed_at must be set on merge")

	closedPayload := latestPRClosedPayload(t, ctx, clients, row.ID, changedProject)
	require.Equal(t, "merged", closedPayload.Outcome, "pr_closed payload outcome")
	require.Len(t, closedPayload.Edits, 1, "one edited file; got %+v", closedPayload.Edits)
	require.True(t, closedPayload.Edits[0].Amended,
		"a merge over a human-pushed second commit must be recorded amended=true; got %+v", closedPayload.Edits[0])
	t.Logf("✅ close-loop confirmed: pr_state=merged, edit amended=%v", closedPayload.Edits[0].Amended)

	// 5. Provenance: rb.amended=true on the RESOLVED_BY edge, ed.amended=true
	//    on the EDITED edge. pollNeo4jPRState gates on the SAME Cypher
	//    statement that draws both, so the reads below cannot race the write.
	pollNeo4jPRState(t, ctx, clients, row.ID, changedProject, "merged", 60*time.Second)

	resolvedRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {tenant_id: 'default', release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Fix {tenant_id: 'default', fix_id: $fix_id})
		RETURN rb.amended AS amended`,
		map[string]any{"release_id": releaseID, "node_id": ftableEUniqueID, "fix_id": row.ID})
	require.Len(t, resolvedRows, 1, "ftable_e must have exactly one RESOLVED_BY edge to the fix")
	require.Equal(t, true, resolvedRows[0]["amended"], "an amended merge's RESOLVED_BY edge must carry amended=true")

	editRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (:Fix {tenant_id: 'default', fix_id: $fix_id})-[e:EDITED]->(t:Node {tenant_id: 'default', unique_id: $node_id})
		RETURN e.amended AS amended`,
		map[string]any{"fix_id": row.ID, "node_id": ftableEUniqueID})
	require.Len(t, editRows, 1, "ftable_e must have exactly one EDITED edge from the fix")
	require.Equal(t, true, editRows[0]["amended"], "an amended merge's EDITED edge must carry amended=true")

	t.Logf("✅ amended-merge provenance confirmed: rb.amended=true, ed.amended=true, pr_state=merged")
}

// batchFixRow is the batched view of a fix row: the set the attempt
// addressed, each node's outcome, every file it changed, and every
// release check that judged it. The single-node projection is fixRow;
// a batched attempt cannot be read through that one, because its scalar
// node_id and file_path columns hold only a representative.
type batchFixRow struct {
	ID              string `db:"id"`
	NodeID          string `db:"node_id"`
	Status          string `db:"status"`
	Attempt         int    `db:"attempt"`
	Repo            string `db:"repo"`
	VerifyError     string `db:"verify_error"`
	ResolvedNodeIDs []byte `db:"resolved_node_ids"`
	NodeOutcomes    []byte `db:"node_outcomes"`
	FileEdits       []byte `db:"file_edits"`
	ReleaseChecks   []byte `db:"release_checks"`
}

// waitForBatchFix polls the agent-remediation's fix table until the
// release's given attempt reaches want, and returns the row. It is addressed by
// (release, attempt) rather than by node, because one attempt now covers a whole
// failing set and no single node identifies it. Every status change is logged,
// so an attempt that stalls in 'generating' or 'verifying' is distinguishable
// from one that was never created — pollUntil's message is built before any
// polling and cannot carry that.
func waitForBatchFix(
	t *testing.T, ctx context.Context, clients *testClients,
	releaseID string, attempt int, want string, timeout time.Duration,
) batchFixRow {
	t.Helper()
	var row batchFixRow
	var last string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row, `
			SELECT id, node_id, status, attempt, repo, verify_error,
			       resolved_node_ids, node_outcomes, file_edits, release_checks
			  FROM fix
			 WHERE release_id = $1 AND attempt = $2`,
			releaseID, attempt)
		if err != nil {
			return false, nil
		}
		if row.Status != last {
			t.Logf("fix %s attempt %d: status=%s node_id=%s", releaseID, attempt, row.Status, row.NodeID)
			last = row.Status
		}
		return row.Status == want, nil
	}, fmt.Sprintf("timeout waiting for release %s attempt %d to reach %q (see the logged status changes above)",
		releaseID, attempt, want))
	return row
}

// countFixesForRelease returns how many fix rows exist for a release.
// A batched release is one attempt: this is what proves the failing set was not
// silently fanned back out into one fix — and one pull request — per node.
func countFixesForRelease(t *testing.T, ctx context.Context, clients *testClients, releaseID string) int {
	t.Helper()
	var n int
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &n,
		`SELECT count(*) FROM fix WHERE release_id = $1`, releaseID))
	return n
}

// nodeOutcomeEntry mirrors one value of the fix's node_outcomes column:
// how the attempt ended for one of the nodes it addressed.
type nodeOutcomeEntry struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// decodeNodeOutcomes reads the node_outcomes column, keyed by node id.
func decodeNodeOutcomes(t *testing.T, raw []byte) map[string]nodeOutcomeEntry {
	t.Helper()
	outcomes := map[string]nodeOutcomeEntry{}
	require.NoError(t, json.Unmarshal(raw, &outcomes), "decode fix node_outcomes: %s", raw)
	return outcomes
}

// decodeNodeIDs reads a JSONB string array column (resolved_node_ids).
func decodeNodeIDs(t *testing.T, raw []byte) []string {
	t.Helper()
	var ids []string
	require.NoError(t, json.Unmarshal(raw, &ids), "decode node id array: %s", raw)
	return ids
}

// waitForBatchedTrigger polls heal.triggered:v1 until the release's
// trigger carries an entry for every named node, and returns it. Waiting on the
// whole set rather than on any one node is what makes the batching assertions
// meaningful: a trigger observed mid-write would otherwise satisfy a
// single-node wait and then fail a count. Every entry the tap recorded is
// decoded with pkgevents.DecodeHealTriggered, so an entry that is not a
// v3 trigger fails the test.
func waitForBatchedTrigger(
	t *testing.T, ctx context.Context, requestedTap *streamTap,
	releaseID string, nodeIDs []string, timeout time.Duration,
) tappedTrigger {
	t.Helper()
	var found tappedTrigger
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		for _, p := range triggersForRelease(t, requestedTap, releaseID) {
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
		streams.HealTriggeredV1, releaseID, nodeIDs))
	return found
}

// triggersForRelease returns every heal.triggered:v1 message naming the
// release. A release is remediated once per round, so the length of this is
// itself an assertion subject.
func triggersForRelease(t *testing.T, requestedTap *streamTap, releaseID string) []tappedTrigger {
	t.Helper()
	var out []tappedTrigger
	for _, p := range requestedTap.Triggers(t) {
		if p.ReleaseID == releaseID {
			out = append(out, p)
		}
	}
	return out
}

// triggerNodeIDs lists the node ids a trigger carries, for failure messages.
func triggerNodeIDs(p tappedTrigger) []string {
	ids := make([]string, 0, len(p.Nodes))
	for _, n := range p.Nodes {
		ids = append(ids, n.NodeID)
	}
	return ids
}

// proposedEdit mirrors one element of fix.proposed:v1's edits array.
type proposedEdit struct {
	Path         string `json:"path"`
	ContentURI   string `json:"content_uri"`
	DiffURI      string `json:"diff_uri"`
	TargetNodeID string `json:"target_node_id"`
}

// batchProposedPayload is the batched projection of fix.proposed:v1:
// the whole set the attempt resolved and every file it changed. The single-node
// projection of the same event is fixProposedPayload, which does not
// decode the edits array.
type batchProposedPayload struct {
	ReleaseID       string         `json:"release_id"`
	NodeID          string         `json:"node_id"`
	ResolvedNodeIDs []string       `json:"resolved_node_ids"`
	Attempt         int            `json:"attempt"`
	Edits           []proposedEdit `json:"edits"`
}

// waitForBatchProposedEvent polls fix.proposed:v1 for the release's
// announcement and returns it.
func waitForBatchProposedEvent(
	t *testing.T, ctx context.Context, proposedTap *streamTap, releaseID string, timeout time.Duration,
) batchProposedPayload {
	t.Helper()
	var found batchProposedPayload
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		events := proposedEventsForRelease(t, proposedTap, releaseID)
		if len(events) == 0 {
			return false, nil
		}
		found = events[0]
		return true, nil
	}, fmt.Sprintf("timeout waiting for %s for release %s", streams.FixProposedV1, releaseID))
	return found
}

// proposedEventsForRelease returns every fix.proposed:v1 message naming
// the release.
func proposedEventsForRelease(t *testing.T, proposedTap *streamTap, releaseID string) []batchProposedPayload {
	t.Helper()
	msgs := proposedTap.Entries()
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

// openRemediationPR opens the fix's pull request(s) through the UI route
// and returns the single entry's PR number, asserting a single-project
// fix opens exactly ONE pull request, attributed to project, whose
// branch carries the deterministic /<project> suffix. It also asserts the
// route is idempotent: a second POST must return the SAME pull_requests set —
// not open a second pull request — since every group that already has one
// resolves to a reported success rather than a per-group error, so the whole
// route still answers 200, not 409.
func openRemediationPR(
	t *testing.T, ctx context.Context, clients *testClients, fixID, releaseID, project string,
) int {
	t.Helper()
	created := callCreatePREndpoint(t, ctx, clients.uiBase, fixID, http.StatusOK)
	require.Empty(t, created.Errors, "no per-project group should fail to open a pull request; got %+v", created.Errors)
	require.Len(t, created.PullRequests, 1,
		"a single-project fix must open exactly one pull request; got %+v", created.PullRequests)
	pr := created.PullRequests[0]
	require.Equal(t, project, pr.Project, "the pull request must be attributed to the fix's owning project")
	require.NotEmpty(t, pr.PRUrl, "pr_url must be non-empty on first create")
	require.Greater(t, pr.PRNumber, 0, "pr_number must be a positive stub-github PR number")

	dup := callCreatePREndpoint(t, ctx, clients.uiBase, fixID, http.StatusOK)
	require.Empty(t, dup.Errors, "a duplicate POST must not report any per-project failure; got %+v", dup.Errors)
	require.Equal(t, created.PullRequests, dup.PullRequests,
		"a second POST must return the SAME pull request set, not open another one for release %s", releaseID)

	row := pollChildPRState(t, ctx, clients, fixID, project, "open", 30*time.Second)
	require.Equal(t, pr.PRNumber, row.PRNumber, "the row must record the number the create call returned")
	require.True(t, strings.HasSuffix(row.Branch, "/"+project),
		"a per-project pull request's branch must carry the /<project> suffix; got %q", row.Branch)
	return pr.PRNumber
}

// stubPullRequest is the subset of stub-github's pulls JSON these tests assert.
type stubPullRequest struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	State  string `json:"state"`
	// RequestedReviewers are the users a review was requested from, as GitHub
	// lists them on a pull request.
	RequestedReviewers []struct {
		Login string `json:"login"`
	} `json:"requested_reviewers"`
	// RequestedTeams are the teams a review was requested from.
	RequestedTeams []struct {
		Slug string `json:"slug"`
	} `json:"requested_teams"`
}

// fetchStubPullRequest reads one pull request back from stub-github. The title
// is only observable here — the create-PR route returns just a URL and a number
// — so this is what proves the pull request a reviewer receives is named for
// every node it fixes. The e2e suite runs inside the catalog container, so
// stub-github is reached by its compose service name, the same base URL
// agent-remediation and ui use.
func fetchStubPullRequest(t *testing.T, ctx context.Context, repo string, number int) stubPullRequest {
	t.Helper()
	require.NotEmpty(t, repo, "fix row must carry the repo its pull request was opened on")
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

// topologyModel is one dbt model to place in Neo4j as a :Node.
type topologyModel struct {
	uniqueID string
	schema   string
	table    string
	project  string
	filePath string
}

// seedModelTopologyNodes MERGEs a minimal :Node per model, under the default
// tenant, so the agent-remediation's Locator (the GetNodeLocation gRPC call)
// can resolve each node's file path and project name. In a cold e2e no release
// has been promoted, so Neo4j is empty; a node missing here is reported
// NOT_FOUND, which makes an upstream cluster skip its target and degrades a
// node's source resolution silently.
//
// The unique_id uses the same key format the release-promotion handler writes
// ("{schema_name}.{table_name}"), and original_file_path matches the dbt
// manifest value, which the agent joins with the project_repos.yaml prefix to
// form the full repository path.
//
// The nodes are removed via t.Cleanup so they do not leak into other tests.
// seedFTableETopologyNode owns ftable_e and its private ancestor; this seeds
// the nodes no existing helper places.
func seedModelTopologyNodes(t *testing.T, ctx context.Context, clients *testClients, models ...topologyModel) {
	t.Helper()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer session.Close(ctx)

	ids := make([]string, 0, len(models))
	for _, m := range models {
		_, err := session.Run(ctx, `
			MERGE (t:Node {tenant_id: 'default', unique_id: $uid})
			SET t.schema_name        = $schema_name,
			    t.table_name         = $table_name,
			    t.project_name       = $project_name,
			    t.original_file_path = $file_path,
			    t.node_type          = 'model',
			    t.active             = true
		`, map[string]interface{}{
			"uid":          m.uniqueID,
			"schema_name":  m.schema,
			"table_name":   m.table,
			"project_name": m.project,
			"file_path":    m.filePath,
		})
		require.NoError(t, err, "seed %s topology node in Neo4j", m.uniqueID)
		ids = append(ids, m.uniqueID)
	}

	t.Cleanup(func() { deleteSeededNodes(t, clients, ids...) })
	t.Logf("seeded topology nodes in Neo4j: %v", ids)
}

// TestE2E_BatchedRemediation_CrossProjectBreakFixedAtProducer proves that a
// consumer broken by a change in ANOTHER project is repaired at the producer,
// keeping both the new and the old contract, and that nothing in the
// consumer's project is edited: a fix there could never ship, because
// releases are one project at a time and the consumer's release would be
// validated against the producer's production code.
//
//	The live release: xbreak_up (service-3) at a STALE hash, xbreak_down (service-2) real
//	POST /releases (service-3) → xbreak_up changed, validates ok; xbreak_down
//	  (its consumer, from service-2's production manifest) fails on amount_eur
//	→ ONE heal.triggered:v1 with one node whose changed_ancestors names
//	  xbreak_up@service-3 at depth 1
//	→ CrossProjectCause targets xbreak_up; the stub keeps id AND amount_eur
//	→ ONE edit to services/service-3/models/xbreak_up.sql, ONE release
//	  check for service-3 that passes (xbreak_down rebuilt as its descendant
//	  binds again), ONE pull request for service-3, nothing for service-2
//	→ merge → RESOLVED_BY for xbreak_down, EDITED to xbreak_up
func TestE2E_BatchedRemediation_CrossProjectBreakFixedAtProducer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), batchCtxBudget)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-xsvc-" + uuid.NewString()[:8]
	producerProject := "service-3"
	consumerProject := "service-2"
	t.Logf("release_id=%s producer=%s consumer=%s changed_node=%s expected_failure=%s",
		releaseID, producerProject, consumerProject, xbreakUpUniqueID, xbreakDownUniqueID)

	// 1. Production holds every baseline node; the producer at a stale hash so
	//    it is the release's only changed node, the consumer at its real hash.
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	producerImageTag := allProjects[producerProject].imageTag
	require.NotEmpty(t, producerImageTag, "image_tag missing for %s — setup.sh must seed the production pointers", producerProject)

	seen := map[string]bool{}
	var prodNodes []map[string]string
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			seen[n.uniqueID] = true
			hash := n.contentHash
			if n.uniqueID == xbreakUpUniqueID {
				hash = "stale-" + hash
			}
			prodNodes = append(prodNodes, map[string]string{"unique_id": n.uniqueID, "project": svc, "content_hash": hash})
		}
	}
	for _, id := range []string{xbreakUpUniqueID, xbreakDownUniqueID} {
		require.True(t, seen[id], "%s not found in any baseline manifest", id)
	}

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.HealTriggeredV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedLiveRelease(t, ctx, clients, prodNodes)
	seedProductionPointers(t, ctx, clients, allProjects)
	// The producer is placed in the graph so the upstream_target step's diff
	// against the version that runs now has a stored version to read; its
	// location travels on the trigger.
	seedModelTopologyNodes(t, ctx, clients, topologyModel{
		uniqueID: xbreakUpUniqueID, schema: "e2e_schema", table: "xbreak_up",
		project: producerProject, filePath: "models/xbreak_up.sql",
	})

	// 2. The release is rejected on the consumer alone.
	postRelease(t, clients, producerProject, releaseID, producerImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)
	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.Equal(t, []string{xbreakDownUniqueID}, failing,
		"only the cross-project consumer of the dropped column may fail; got %v", failing)

	// 3. The trigger names the producer, in the other project, as the cause.
	trigger := waitForBatchedTrigger(t, ctx, requestedTap, releaseID, []string{xbreakDownUniqueID}, batchTriggerBudget)
	require.Len(t, trigger.Nodes, 1, "exactly the failing consumer; got %v", triggerNodeIDs(trigger))
	down, ok := trigger.findNode(xbreakDownUniqueID)
	require.True(t, ok)
	require.Equal(t, consumerProject, down.Project, "the failing node's own project")
	require.Equal(t, []string{xbreakUpUniqueID}, ancestorIDs(down))
	require.Equal(t, producerProject, down.ChangedAncestors[0].Project, "the changed ancestor lives in the other project")
	require.Equal(t, "models/xbreak_up.sql", down.ChangedAncestors[0].FilePath)
	require.Equal(t, 1, down.ChangedAncestors[0].Depth)
	t.Logf("✅ trigger names %s@%s (depth 1) as the cause of %s@%s",
		xbreakUpUniqueID, producerProject, xbreakDownUniqueID, consumerProject)

	// 4. ONE fix, ONE edit — to the producer, in the producer's project.
	row := waitForBatchFix(t, ctx, clients, releaseID, 1, "proposed", batchFixBudget)
	require.Equal(t, 1, countFixesForRelease(t, ctx, clients, releaseID))
	require.Equal(t, []string{xbreakDownUniqueID}, decodeNodeIDs(t, row.ResolvedNodeIDs))
	outcomes := decodeNodeOutcomes(t, row.NodeOutcomes)
	require.Equal(t, "proposed", outcomes[xbreakDownUniqueID].Status, "node_outcomes=%s", row.NodeOutcomes)

	edits := decodeFileEdits(t, row.FileEdits)
	require.Len(t, edits, 1, "one cross-project failure is repaired by one edit at the producer; got %+v", edits)
	require.Equal(t, "services/service-3/models/xbreak_up.sql", edits[0].Path, "the edit lands in the producer's project")
	require.Equal(t, xbreakUpUniqueID, edits[0].TargetNodeID)
	body := string(getS3ObjectByKey(t, ctx, clients, stripS3Prefix(edits[0].ContentURI)))
	require.Contains(t, body, "amount_eur", "the producer keeps the column the consumer reads; got %q", strings.TrimSpace(body))
	require.Contains(t, body, "1 AS id", "and keeps its own change; got %q", strings.TrimSpace(body))

	// 5. ONE release check, for the producer's project, that passes.
	releaseChecks := decodeReleaseChecks(t, row.ReleaseChecks)
	require.Len(t, releaseChecks, 1, "one edited project, one release check; got %+v", releaseChecks)
	require.Equal(t, "dbt", releaseChecks[0].Kind)
	require.Equal(t, producerProject, releaseChecks[0].Project, "verified as the producer's delta")
	assertReleaseQueueNamedFix(t, ctx, clients, releaseChecks[0].CheckID, batchVerifyBudget)
	waitForReleaseCheckStatus(t, ctx, clients, releaseChecks[0].CheckID, "passed", batchVerifyBudget)
	assertNotListedAsRelease(t, ctx, clients, releaseChecks[0].CheckID)
	t.Logf("✅ release check %s passed with the consumer rebuilt against the repaired producer", releaseChecks[0].CheckID)

	// 6. ONE pull request, for the producer's project; merge it and confirm
	//    provenance: RESOLVED_BY for the consumer, EDITED to the producer.
	prNumber := openRemediationPR(t, ctx, clients, row.ID, releaseID, producerProject)
	prRow := pollChildPRState(t, ctx, clients, row.ID, producerProject, "open", 30*time.Second)
	require.True(t, strings.HasSuffix(prRow.Branch, "/"+producerProject), "branch %q must carry the producer's suffix", prRow.Branch)
	var consumerPRs int
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &consumerPRs,
		`SELECT count(*) FROM fix_pull_request WHERE fix_id = $1 AND project = $2`, row.ID, consumerProject))
	require.Zero(t, consumerPRs, "nothing is opened for the consumer's project")

	mergeCommitSHA := mergePullRequestViaStub(t, ctx, row.Repo, prNumber)
	t.Logf("merged PR #%d via stub-github: merge_commit_sha=%s", prNumber, mergeCommitSHA)
	pollChildPRState(t, ctx, clients, row.ID, producerProject, "merged", 60*time.Second)
	pollNeo4jPRState(t, ctx, clients, row.ID, producerProject, "merged", 60*time.Second)

	resolvedRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {tenant_id: 'default', release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Fix {tenant_id: 'default', fix_id: $fix_id})
		RETURN rb.amended AS amended`,
		map[string]any{"release_id": releaseID, "node_id": xbreakDownUniqueID, "fix_id": row.ID})
	require.Len(t, resolvedRows, 1, "the consumer is resolved by the producer's merged fix")
	editRows := queryNeo4jRows(t, ctx, clients, `
		MATCH (p:Fix {tenant_id: 'default', fix_id: $fix_id})-[e:EDITED]->(t:Node {tenant_id: 'default'})
		RETURN t.unique_id AS unique_id`,
		map[string]any{"fix_id": row.ID})
	require.Len(t, editRows, 1)
	require.Equal(t, xbreakUpUniqueID, editRows[0]["unique_id"], "EDITED must target the producer, not the consumer")
	t.Logf("✅ provenance confirmed: RESOLVED_BY %s, EDITED -> %s", xbreakDownUniqueID, xbreakUpUniqueID)
}
