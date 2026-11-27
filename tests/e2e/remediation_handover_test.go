package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// unfixableInfraUniqueID is service-2's fixture model whose validation fails
// with an error the remediation classifier reads as an infrastructure failure.
// It selects the quoted column "connection refused", which no relation
// provides, so the warehouse answers `column "connection refused" does not
// exist`. The classifier checks its infrastructure rules before its logic
// rules, so the phrase "connection refused" wins over "does not exist". The
// model reads no table and nothing reads it, so only a release that changes it
// validates it.
const unfixableInfraUniqueID = "e2e_schema.unfixable_infra"

// connectionRefusedRule is the classifier rule that matches the fixture's
// error: the reason a classification_decision row records and the reason of
// the trigger's node. It is the rule's id, not the message a hand-over carries:
// that is the failure's error line.
const connectionRefusedRule = "infra:connection_refused"

// connectionRefusedText is the phrase the warehouse's error for the fixture
// holds, and so does the failure's key error line. The line's exact wording
// belongs to the database driver, so the checks pin the phrase.
const connectionRefusedText = "connection refused"

// rejectedReleaseFlow is the agent-remediation flow every remediation trigger
// runs.
const rejectedReleaseFlow = "rejected-release"

// Stage budgets of the hand-over test. The rejection covers one validation
// release check in kind; every later stage is one consumer hop or one runner
// drive, with no model call and no release check.
const (
	handoverRejectBudget  = 10 * time.Minute
	handoverStageBudget   = 3 * time.Minute
	handoverDecisionWait  = 30 * time.Second
	handoverCatalogBudget = 2 * time.Minute
)

// TestE2E_Remediation_EveryNodeUnfixableIsHandedOver proves that a release
// whose only failing node the classifier marks unfixable is handed over to a
// human with its reason, without a model call, and that the operator's "Try
// again" still starts the next round:
//
//	POST /releases (service-2; unfixable_infra is the only changed node)
//	→ validation fails: column "connection refused" does not exist
//	→ release.rejected:v1 → the remediation classifier drops the node
//	  (infra_transient, rule infra:connection_refused)
//	→ ONE heal.triggered:v1 forwarding the node, marked unfixable
//	  {reason: classifier_infra, detail: the failure's key error line}
//	→ catalog records the message and no :Rejection for the node
//	→ agent-remediation's runner settles the remediation with no step but the
//	  hand-over: ONE skipped attempt whose node carries code classifier_infra,
//	  and ONE remediation.handed_over:v1 naming the node with that reason and
//	  the error line as its message
//	→ POST /releases/{id}/retry-remediation answers 202: a skipped attempt ends
//	  the round, so the retry is not refused as in progress
//	→ round 2 is classified and handed over the same way, its attempt
//	  continuing the release's attempt numbering
func TestE2E_Remediation_EveryNodeUnfixableIsHandedOver(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// 40 minutes: strictly greater than the 39 this test's stage budgets sum to
	// (the validation request 8 and the rejection 10, then six stages of 3, the
	// catalog's 2 and two decisions of half a minute).
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-handover-" + uuid.NewString()[:8]
	changedProject := "service-2"
	t.Logf("release_id=%s changed_project=%s changed_node=%s", releaseID, changedProject, unfixableInfraUniqueID)

	// 1. Production holds every baseline node but the fixture, so the derived
	//    changed set is exactly {unfixable_infra}.
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	changedImageTag := allProjects[changedProject].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed the production pointers", changedProject)

	var prodNodes []map[string]string
	fixtureFound := false
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			if n.uniqueID == unfixableInfraUniqueID {
				fixtureFound = true
				continue
			}
			prodNodes = append(prodNodes, map[string]string{"unique_id": n.uniqueID, "project": svc, "content_hash": n.contentHash})
		}
	}
	require.True(t, fixtureFound,
		"%s not found in any baseline manifest — is the model in service-2 and the image rebuilt?", unfixableInfraUniqueID)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.HealTriggeredV1)
	handedOverTap := startStreamTap(t, ctx, clients.redisClient, streams.RemediationHandedOverV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedLiveRelease(t, ctx, clients, prodNodes)
	seedProductionPointers(t, ctx, clients, allProjects)

	// 2. The release validates the fixture alone and is rejected on it.
	postRelease(t, clients, changedProject, releaseID, changedImageTag, false)
	assertValidationRequestedNodes(t, ctx, validationTap, releaseID, []string{unfixableInfraUniqueID})
	waitForReleaseRejected(t, ctx, clients, releaseID, handoverRejectBudget)
	require.Equal(t, []string{unfixableInfraUniqueID}, releaseFailingNodes(t, ctx, clients, releaseID),
		"only the fixture may fail")

	// 3. The trigger forwards the node, marked unfixable with the classifier's
	//    reason and the failure's error line.
	trigger := waitForBatchedTrigger(t, ctx, requestedTap, releaseID, []string{unfixableInfraUniqueID}, handoverStageBudget)
	require.Len(t, triggersForRelease(t, requestedTap, releaseID), 1, "one trigger per rejected release and round")
	require.Equal(t, pkgevents.DefaultTenantID, trigger.TenantID, "the trigger carries the rejection's tenant")
	require.Equal(t, 1, trigger.RemediationRound)
	require.Equal(t, string(model.RemediationSourceValidation), trigger.Source)
	require.Len(t, trigger.Nodes, 1, "the trigger carries the one failing node; got %v", triggerNodeIDs(trigger))
	node := trigger.Nodes[0]
	require.Equal(t, "infra_transient", node.Category)
	require.Contains(t, node.ErrorExcerpt, "connection refused", "the excerpt is the error the infrastructure rule matched")
	require.NotNil(t, node.Unfixable, "a node the classifier dropped is forwarded marked unfixable")
	require.Equal(t, string(model.HandoverReasonClassifierInfra), node.Unfixable.Reason)
	require.Equal(t, connectionRefusedRule, node.Reason, "the node's reason is the rule that matched")
	detail := node.Unfixable.Detail
	requireErrorLine(t, detail)
	require.Equal(t, node.ErrorExcerpt, detail, "the detail is the excerpt: the failure's key error line")
	t.Logf("✅ %s forwards %s marked unfixable: %+v", streams.HealTriggeredV1, node.NodeID, *node.Unfixable)

	// 4. The classifier recorded the drop.
	assertDecision(t, ctx, clients, releaseID, 1)

	// 5. ONE attempt, skipped, whose node carries the classifier's code.
	assertSkippedAttempt(t, ctx, clients, releaseID, 1, 1, detail)

	// 6. ONE hand-over naming the node with that reason, from a remediation
	//    that ran no step but the hand-over: no model call, no release check.
	handOver := waitForHandOver(t, ctx, handedOverTap, releaseID, 1)
	assertHandOver(t, ctx, clients, handOver, releaseID, 1, detail)

	// 7. Catalog processed the trigger and kept the node out of its case base.
	pollUntil(t, ctx, handoverCatalogBudget, 2*time.Second, func() (bool, error) {
		rows := queryNeo4jRows(t, ctx, clients, `
			MATCH (p:ProcessedMessage {tenant_id: 'default', group: $group, outbox_entry_id: $outbox_entry_id})
			RETURN count(p) AS n`,
			map[string]any{"group": streams.CatalogHealTriggeredRejections, "outbox_entry_id": trigger.OutboxEntryID})
		n, _ := rows[0]["n"].(int64)
		return n == 1, nil
	}, fmt.Sprintf("timeout waiting for catalog's %s marker of the trigger of release %s",
		streams.CatalogHealTriggeredRejections, releaseID))
	rejections := queryNeo4jRows(t, ctx, clients, `
		MATCH (r:Rejection {tenant_id: 'default', release_id: $release_id, node_id: $node_id})
		RETURN r.node_id AS node_id`,
		map[string]any{"release_id": releaseID, "node_id": unfixableInfraUniqueID})
	require.Empty(t, rejections, "an unfixable node is no precedent, so catalog records no :Rejection for it")
	t.Log("✅ catalog processed the trigger and recorded no :Rejection for the unfixable node")

	// 8. "Try again" is accepted: the skipped attempt ended round 1.
	st, body := postRetry(t, clients, releaseID)
	require.Equal(t, http.StatusAccepted, st, "body=%v", body)
	require.EqualValues(t, 2, body["remediation_round"])

	// 9. Round 2 is classified and handed over the same way, and its attempt
	//    continues the release's attempt numbering.
	var round2 tappedTrigger
	pollUntil(t, ctx, handoverStageBudget, 2*time.Second, func() (bool, error) {
		for _, p := range triggersForRelease(t, requestedTap, releaseID) {
			if p.RemediationRound == 2 {
				round2 = p
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for the round-2 %s of release %s", streams.HealTriggeredV1, releaseID))
	again, ok := round2.findNode(unfixableInfraUniqueID)
	require.True(t, ok, "the round-2 trigger carries the node; got %v", triggerNodeIDs(round2))
	require.NotNil(t, again.Unfixable, "the round-2 trigger marks the node unfixable again")
	require.Equal(t, string(model.HandoverReasonClassifierInfra), again.Unfixable.Reason)
	requireErrorLine(t, again.Unfixable.Detail)

	assertDecision(t, ctx, clients, releaseID, 2)
	assertSkippedAttempt(t, ctx, clients, releaseID, 2, 2, again.Unfixable.Detail)
	assertHandOver(t, ctx, clients, waitForHandOver(t, ctx, handedOverTap, releaseID, 2), releaseID, 2, again.Unfixable.Detail)
	require.EqualValues(t, 2, getReleaseDetail(t, clients, releaseID)["remediation_round"])
	t.Logf("✅ release %s: every round hands the unfixable node over, and Try again starts the next", releaseID)
}

// requireErrorLine checks that detail says what failed, the fixture's error
// line, and is not the id of the rule that matched it.
func requireErrorLine(t *testing.T, detail string) {
	t.Helper()
	require.Contains(t, detail, connectionRefusedText, "the detail is the failure's error line")
	require.NotEqual(t, connectionRefusedRule, detail, "the detail is not the classifier's rule id")
}

// assertDecision waits for the classification_decision row of the fixture in
// round and checks that it records the drop and the rule that made it.
func assertDecision(t *testing.T, ctx context.Context, clients *testClients, releaseID string, round int) {
	t.Helper()
	var decision struct {
		classificationDecisionRow
		Reason string `db:"reason"`
	}
	pollUntil(t, ctx, handoverDecisionWait, time.Second, func() (bool, error) {
		err := clients.remediationDB.GetContext(ctx, &decision,
			`SELECT source, release_id, node_id, category, decision, reason
			   FROM classification_decision
			  WHERE release_id = $1 AND node_id = $2 AND remediation_round = $3`,
			releaseID, unfixableInfraUniqueID, round)
		return err == nil, nil
	}, fmt.Sprintf("timeout waiting for the round-%d classification_decision of release %s", round, releaseID))
	require.Equal(t, string(model.RemediationSourceValidation), decision.Source)
	require.Equal(t, "infra_transient", decision.Category)
	require.Equal(t, "drop", decision.Decision, "an infrastructure failure is dropped, not emitted")
	require.Equal(t, connectionRefusedRule, decision.Reason)
}

// assertSkippedAttempt waits for the release's fix of round on the
// agent-remediation fixes API, the surface release-controller and the UI
// read, and checks that it is the round's one attempt, numbered attempt and
// skipped: the fixture skipped with the classifier's code and detail as its
// reason, no edit, no model and no release check. It also checks the
// release holds exactly round fixes, one per round.
func assertSkippedAttempt(t *testing.T, ctx context.Context, clients *testClients, releaseID string, round, attempt int, detail string) {
	t.Helper()
	var found *remediationv1.Fix
	var last string
	pollUntilDescribed(t, ctx, handoverStageBudget, 2*time.Second, func() (bool, error) {
		resp, err := clients.agentRemediationClient.ListFixes(ctx, &remediationv1.ListFixesRequest{ReleaseId: releaseID})
		if err != nil {
			return false, err
		}
		last = ""
		for _, p := range resp.GetFixes() {
			last += fmt.Sprintf("[round %d attempt %d %s] ", p.GetRemediationRound(), p.GetAttempt(), p.GetStatus())
			if int(p.GetRemediationRound()) == round && p.GetStatus() == "skipped" {
				found = p
			}
		}
		return found != nil, nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for the skipped round-%d attempt of release %s; fixes: %s", round, releaseID, last)
	})

	resp, err := clients.agentRemediationClient.ListFixes(ctx, &remediationv1.ListFixesRequest{ReleaseId: releaseID})
	require.NoError(t, err)
	require.Len(t, resp.GetFixes(), round, "one attempt per round; got %s", last)

	require.EqualValues(t, attempt, found.GetAttempt(), "the attempt continues the release's numbering")
	require.Equal(t, []string{unfixableInfraUniqueID}, found.GetResolvedNodeIds())
	outcome := found.GetNodeOutcomes()[unfixableInfraUniqueID]
	require.NotNil(t, outcome, "the attempt records the fixture's outcome: %v", found.GetNodeOutcomes())
	require.Equal(t, "skipped", outcome.GetStatus())
	require.Equal(t, string(model.HandoverReasonClassifierInfra), outcome.GetCode())
	require.Equal(t, detail, outcome.GetReason(), "the outcome's reason is the trigger's detail")
	requireErrorLine(t, outcome.GetReason())
	require.Empty(t, found.GetEdits(), "an unfixable node gets no edit")
	require.Empty(t, found.GetModel(), "an unfixable node costs no model call")
	require.Empty(t, found.GetReleaseCheckId(), "an unfixable node starts no release check")
	require.Empty(t, found.GetReleaseChecks(), "an unfixable node starts no release check")
	t.Logf("✅ round %d: attempt %d skipped, %s handed over with code %s", round, attempt, unfixableInfraUniqueID, outcome.GetCode())
}

// waitForHandOver waits for the remediation.handed_over:v1 entry of the
// release's round, checks it is the only one, and returns it.
func waitForHandOver(t *testing.T, ctx context.Context, tap *streamTap, releaseID string, round int) tappedHandOver {
	t.Helper()
	ofRound := func() []tappedHandOver {
		var out []tappedHandOver
		for _, h := range tap.HandOvers(t) {
			if h.ReleaseID == releaseID && h.RemediationRound == round {
				out = append(out, h)
			}
		}
		return out
	}
	pollUntil(t, ctx, handoverStageBudget, 2*time.Second, func() (bool, error) {
		return len(ofRound()) > 0, nil
	}, fmt.Sprintf("timeout waiting for the round-%d %s of release %s", round, streams.RemediationHandedOverV1, releaseID))
	all := ofRound()
	require.Len(t, all, 1, "a remediation hands its nodes over once")
	return all[0]
}

// assertHandOver checks that h names the fixture with the classifier's reason,
// the trigger's detail (the failure's error line) as its message and the
// fixture's owner (service-2's dbt project owner, read from the code bundle),
// and that the remediation it reports is done after one try that ran no step
// but the hand-over.
func assertHandOver(t *testing.T, ctx context.Context, clients *testClients, h tappedHandOver, releaseID string, round int, detail string) {
	t.Helper()
	require.Equal(t, pkgevents.DefaultTenantID, h.TenantID, "the hand-over carries the remediation's tenant")
	require.Equal(t, rejectedReleaseFlow, h.Flow)
	require.Equal(t, 1, h.Tries, "the remediation settled in its first try")
	require.Equal(t, []pkgevents.HandedOverNode{{
		NodeID:  unfixableInfraUniqueID,
		Reason:  string(model.HandoverReasonClassifierInfra),
		Message: detail,
		Owner:   testOwner,
	}}, h.Nodes)
	requireErrorLine(t, h.Nodes[0].Message)

	var rem struct {
		ID         string `db:"id"`
		Status     string `db:"status"`
		CurrentTry int    `db:"current_try"`
	}
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &rem,
		`SELECT id, status, current_try FROM remediation
		  WHERE tenant_id = $1 AND release_id = $2 AND remediation_round = $3`,
		pkgevents.DefaultTenantID, releaseID, round))
	require.Equal(t, rem.ID, h.RemediationID, "the hand-over names its remediation")
	require.Equal(t, "done", rem.Status)
	require.Equal(t, 1, rem.CurrentTry)

	var steps []string
	require.NoError(t, clients.agentRemediationDB.SelectContext(ctx, &steps,
		`SELECT step FROM remediation_step WHERE remediation_id = $1 ORDER BY id`, rem.ID))
	require.Equal(t, []string{"hand_over"}, steps,
		"no context, prompt, check or gate step runs for an unfixable node, so no model is called and no release check starts")
	t.Logf("✅ round %d: %s names %s with reason %s", round, streams.RemediationHandedOverV1, unfixableInfraUniqueID, h.Nodes[0].Reason)
}
