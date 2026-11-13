package runner_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
)

func TestRunner_AcceptTwiceCreatesOneRemediation(t *testing.T) {
	h := newHarness(t)
	trig := trigger("compile", compileNode("svc-a"))
	outboxID := uuid.New()
	first := h.accepted(trig, "1-0")
	first.OutboxEntryID = &outboxID
	require.NoError(t, h.runner.Accept(ctx, first))
	require.NoError(t, h.runner.Accept(ctx, first))
	republished := h.accepted(trig, "2-0")
	republished.OutboxEntryID = &outboxID
	require.NoError(t, h.runner.Accept(ctx, republished))

	rems := h.store.Remediations()
	require.Len(t, rems, 1)
	rem := rems[0]
	assert.NotEqual(t, uuid.Nil, rem.ID)
	assert.Equal(t, "default", rem.TenantID)
	assert.Equal(t, releaseID, rem.ReleaseID)
	assert.Equal(t, 1, rem.Round)
	assert.Equal(t, "rejected-release", rem.Flow)
	assert.Equal(t, remediation.StatusRunning, rem.Status)
	assert.JSONEq(t, string(first.Payload), string(rem.TriggerPayload))
	assert.Equal(t, "1-0", rem.TriggerMessageID)
	assert.Equal(t, map[string]string{"tenant_id": "default"}, rem.TriggerFields)
	assert.Equal(t, 0, rem.CurrentTry)

	msgs := h.store.ProcessedMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, messageprocessing.StateCompleted, msgs[0].State)
}

func TestRunner_AcceptOfASecondTriggerForTheSameRoundAddsNoRemediation(t *testing.T) {
	h := newHarness(t)
	trig := trigger("compile", compileNode("svc-a"))
	require.NoError(t, h.runner.Accept(ctx, h.accepted(trig, "1-0")))
	require.NoError(t, h.runner.Accept(ctx, h.accepted(trig, "2-0")))

	assert.Len(t, h.store.Remediations(), 1)
	msgs := h.store.ProcessedMessages()
	require.Len(t, msgs, 2)
	for _, m := range msgs {
		assert.Equal(t, messageprocessing.StateCompleted, m.State)
	}
}

func TestRunner_AcceptOfAnEmptyFailingSetWritesNothing(t *testing.T) {
	h := newHarness(t)
	require.NoError(t, h.runner.Accept(ctx, h.accepted(trigger("compile"), "1-0")))

	assert.Empty(t, h.store.Remediations())
	assert.Empty(t, h.store.ProcessedMessages())
}

func TestRunner_DriveWithoutTheLeaseWritesNothing(t *testing.T) {
	h := newHarness(t)
	require.NoError(t, h.runner.Accept(ctx, h.accepted(trigger("compile", compileNode("svc-a")), "1-0")))
	id := h.store.Remediations()[0].ID

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Empty(t, h.store.Tries(id))
	assert.Empty(t, h.proposals())
	assert.Zero(t, h.llm.calls())
}

func TestRunner_AttemptNumberContinuesTheReleaseSequence(t *testing.T) {
	h := newHarness(t)
	h.legacyAttempt(1, proposal.StatusFailed)
	h.legacyAttempt(2, proposal.StatusFailed)
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.NextTry(ctx, id))

	tries := h.store.Tries(id)
	require.Len(t, tries, 1)
	assert.Equal(t, 1, tries[0].Try)
	assert.Equal(t, 3, tries[0].Attempt)
	assert.Equal(t, remediation.TryDrafting, tries[0].Status)
	assert.Equal(t, map[string]string{
		flowFile:                            flowRevision,
		"types/dbt-compile.yaml":            "rev-dbt-compile",
		"types/dbt-validation.yaml":         "rev-dbt-validation",
		"types/shared-upstream.yaml":        "rev-shared-upstream",
		"types/cross-service-upstream.yaml": "rev-cross-service-upstream",
	}, tries[0].FileRevisions)
	assert.Equal(t, 1, h.remediation(id).CurrentTry)

	ps := h.proposals()
	require.Len(t, ps, 3)
	generating := ps[2].Proposal
	assert.Equal(t, 3, generating.Attempt)
	assert.Equal(t, proposal.StatusGenerating, generating.Status)
	assert.Equal(t, id.String(), generating.RemediationID)
	assert.Equal(t, []string{"svc-a"}, generating.ResolvedNodeIDs)
	assert.Equal(t, proposal.NodeOutcome{Status: proposal.StatusGenerating}, generating.NodeOutcomes["svc-a"])
}

func TestRunner_RestartAfterTheModelCallReusesTheAnswer(t *testing.T) {
	h := newHarness(t)
	h.llm.onCall = func(n int) {
		if n == 1 {
			h.store.FailNextCommit()
		}
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.Error(t, h.runner.Drive(ctx, id), "the prompt step's commit fails")
	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, 1, h.llm.calls(), "the second drive is answered by the cache")
	rows := h.stepRows(id, 1, 1)["dbt-compile"]
	require.Len(t, rows, 1)
	assert.Equal(t, remediation.StepDone, rows[0].Status)
	assert.True(t, rows[0].CacheHit)
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
}

func TestRunner_FallbackClustersAreAddedOnce(t *testing.T) {
	h := newHarness(t)
	h.bundle.onRead = func(n int) {
		if n == 1 {
			h.store.FailNextCommit()
		}
	}
	ancestor := remediation.ChangedAncestor{NodeID: "model.core.base", FilePath: "models/base.sql", Service: "core", Depth: 1}
	a, b := compileNode("svc-a"), compileNode("svc-b")
	a.ErrorSignature, b.ErrorSignature = "sig-shared", "sig-shared"
	a.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
	b.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
	id := h.accept(trigger("compile", a, b))

	require.Error(t, h.runner.Drive(ctx, id), "the declining step's commit fails")
	require.NoError(t, h.runner.Drive(ctx, id))

	clusters := h.store.Clusters(id)
	require.Len(t, clusters, 3, "one cluster per member is added, once")
	upstream := clusters[0]
	assert.Equal(t, workflow.ClusterSharedUpstream, upstream.Kind)
	assert.Equal(t, "model.core.base", upstream.TargetNodeID)
	assert.Equal(t, remediation.ClusterFellBack, upstream.Status)
	assert.Nil(t, upstream.Outcome)
	for i, member := range []string{"svc-a", "svc-b"} {
		c := clusters[i+1]
		assert.Equal(t, i+2, c.Cluster)
		assert.Equal(t, workflow.ClusterSingle, c.Kind)
		assert.Equal(t, member, c.TargetNodeID)
		assert.Equal(t, []string{member}, c.Members)
		assert.Equal(t, "dbt-compile", c.TypeName)
		assert.Equal(t, remediation.ClusterChecked, c.Status)
	}
	assert.Equal(t, 2, h.llm.calls(), "each member's own type called the model once")
}

func TestRunner_CrossServiceDeclineSkipsTheMembers(t *testing.T) {
	h := newHarness(t)
	ancestor := remediation.ChangedAncestor{NodeID: "model.core.base", FilePath: "models/base.sql", Service: "core", Depth: 1}
	a := compileNode("svc-a")
	a.Service = "svc-a"
	a.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
	id := h.accept(trigger("compile", a))

	require.NoError(t, h.runner.Drive(ctx, id))

	clusters := h.store.Clusters(id)
	require.Len(t, clusters, 1, "a cross-service cluster adds no member cluster")
	assert.Equal(t, workflow.ClusterCrossService, clusters[0].Kind)
	assert.Equal(t, remediation.ClusterFellBack, clusters[0].Status)
	assert.Equal(t, &proposal.NodeOutcome{
		Status: proposal.StatusSkipped,
		Reason: "the change that broke this node lives in service core; a fix in this node's own service cannot ship before it: " +
			"the changed ancestor's source is not in the release's code bundle",
	}, clusters[0].Outcome)
	assert.Zero(t, h.llm.calls())
}

func TestRunner_MaintenanceRecordsSkippedWithoutAModelCall(t *testing.T) {
	h := newHarness(t, withMaintenance())
	id := h.accept(trigger("compile", compileNode("svc-b"), compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	const reason = "not attempted: continuo was in maintenance mode"
	ps := h.proposals()
	require.Len(t, ps, 1)
	p := ps[0].Proposal
	assert.Equal(t, 1, p.Attempt)
	assert.Equal(t, proposal.StatusSkipped, p.Status)
	assert.Equal(t, reason, p.Rationale)
	assert.Equal(t, id.String(), p.RemediationID)
	assert.Equal(t, []string{"svc-a", "svc-b"}, p.ResolvedNodeIDs)
	assert.Equal(t, map[string]proposal.NodeOutcome{
		"svc-a": {Status: proposal.StatusSkipped, Reason: reason},
		"svc-b": {Status: proposal.StatusSkipped, Reason: reason},
	}, p.NodeOutcomes)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, h.store.Tries(id))
	assert.Zero(t, h.llm.calls())
}

func TestRunner_NoRepositoryRecordsFailed(t *testing.T) {
	h := newHarness(t)
	trig := trigger("compile", compileNode("svc-a"))
	trig.Repo = ""
	id := h.accept(trig)

	require.NoError(t, h.runner.Drive(ctx, id))

	const reason = "the trigger carried no repository for rel-1"
	ps := h.proposals()
	require.Len(t, ps, 1)
	p := ps[0].Proposal
	assert.Equal(t, proposal.StatusFailed, p.Status)
	assert.Equal(t, reason, p.Rationale)
	assert.Equal(t, id.String(), p.RemediationID)
	assert.Equal(t, map[string]proposal.NodeOutcome{"svc-a": {Status: proposal.StatusFailed, Reason: reason}}, p.NodeOutcomes)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Zero(t, h.llm.calls())
}

func TestRunner_DbtTestTargetIsSkipped(t *testing.T) {
	h := newHarness(t)
	test := remediation.TriggerNode{NodeID: "test.sales.not_null", ErrorSignature: "sig-t", NodeType: "dbt-test", Service: "svc-a"}
	python := remediation.TriggerNode{NodeID: "python.sales.loader", ErrorSignature: "sig-p", NodeType: "python-node", Service: "svc-a"}
	id := h.accept(trigger("validation", test, python))

	require.NoError(t, h.runner.Drive(ctx, id))

	byTarget := map[string]remediation.Cluster{}
	for _, c := range h.store.Clusters(id) {
		byTarget[c.TargetNodeID] = c
	}
	require.Len(t, byTarget, 2)
	assert.Equal(t, remediation.ClusterHandedOver, byTarget[test.NodeID].Status)
	assert.Equal(t, "", byTarget[test.NodeID].TypeName)
	assert.Equal(t, &proposal.NodeOutcome{
		Status: proposal.StatusSkipped,
		Reason: "dbt tests are not fix targets; fix the model or edit the test by hand",
	}, byTarget[test.NodeID].Outcome)
	assert.Equal(t, remediation.ClusterHandedOver, byTarget[python.NodeID].Status)
	assert.Equal(t, &proposal.NodeOutcome{
		Status: proposal.StatusSkipped,
		Reason: "no remediation type matches this failure",
	}, byTarget[python.NodeID].Outcome)
	assert.Empty(t, h.store.Steps(id))
	assert.Zero(t, h.llm.calls())
}

func TestRunner_DbtTestTargetIsSkippedEvenWhenATypeWouldSelectIt(t *testing.T) {
	h := newHarness(t)
	test := compileNode("svc-a")
	test.NodeType = "dbt-test"
	id := h.accept(trigger("compile", test))

	require.NoError(t, h.runner.Drive(ctx, id))

	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterHandedOver, c.Status)
	assert.Equal(t, "", c.TypeName, "dbt-compile selects any node type, and is not used for a test")
	assert.Equal(t, &proposal.NodeOutcome{
		Status: proposal.StatusSkipped,
		Reason: "dbt tests are not fix targets; fix the model or edit the test by hand",
	}, c.Outcome)
	assert.Empty(t, h.store.Steps(id))
	assert.Zero(t, h.llm.calls())
}

func TestRunner_ADeclineWithNoTextHandsTheClusterOver(t *testing.T) {
	h := newHarness(t)
	node := remediation.TriggerNode{NodeID: "model.sales.orders", ErrorSignature: "sig-v", NodeType: "dbt-model", Service: "svc-a"}
	id := h.accept(trigger("validation", node))

	require.NoError(t, h.runner.Drive(ctx, id))

	c := h.cluster(id, 1, 1)
	assert.Equal(t, "dbt-validation", c.TypeName)
	assert.Equal(t, remediation.ClusterHandedOver, c.Status)
	assert.Equal(t, &proposal.NodeOutcome{Status: proposal.StatusSkipped, Reason: ""}, c.Outcome)
	rows := h.stepRows(id, 1, 1)
	require.Len(t, rows, 1, "the cluster stops at the declining step")
	require.Len(t, rows["candidate_sql"], 1)
	assert.Equal(t, remediation.StepDone, rows["candidate_sql"][0].Status)
	assert.Zero(t, h.llm.calls())
}

func TestRunner_EveryStepWritesOneJournalRow(t *testing.T) {
	h := newHarness(t)
	reads := 0
	h.source.onRead = func() {
		reads++
		if reads == 1 {
			h.store.FailNextCommit()
		}
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.Error(t, h.runner.Drive(ctx, id), "the first step's commit fails")
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.runner.Drive(ctx, id), "a drive of a verifying try writes nothing")

	type key struct {
		step string
		kind string
	}
	want := []key{
		{"failing_node_source", "context"}, {"dbt_log", "context"}, {"failure_precedents", "context"},
		{"dbt-compile", "prompt"},
		{"target_file_was_shown", "check"}, {"content_changed", "check"}, {"confidence_not_low", "check"},
		{"preflight", "runner"},
	}
	var got []key
	for _, s := range h.store.Steps(id) {
		if s.Cluster != 1 {
			continue
		}
		got = append(got, key{s.Step, s.Kind})
		assert.Equal(t, 1, s.Try, s.Step)
		assert.Equal(t, 1, s.Cluster, s.Step)
		assert.Equal(t, 1, s.Round, s.Step)
		assert.Equal(t, remediation.StepDone, s.Status, s.Step)
		assert.Equal(t, "rev-dbt-compile", s.FileRevision, s.Step)
		assert.NotNil(t, s.FinishedAt, s.Step)
		assert.NotEmpty(t, s.Output, s.Step)
		if s.Kind != "runner" {
			assert.NotEmpty(t, s.InputDigest, s.Step)
		}
	}
	assert.Equal(t, want, got)

	rows := h.stepRows(id, 1, 1)
	assert.Equal(t, 1, rows["failing_node_source"][0].Failures, "the lost commit is counted on its step")
	p := rows["dbt-compile"][0]
	req := h.llm.lastRequest()
	sum := sha256.Sum256([]byte(req.System + req.ToolName))
	assert.Equal(t, hex.EncodeToString(sum[:])[:12], p.PromptVersion)
	assert.Equal(t, "claude-test", p.Model)
	assert.Equal(t, 100, p.TokensIn)
	assert.Equal(t, 20, p.TokensOut)
	assert.False(t, p.CacheHit)
}

func TestRunner_AStepErrorIsRecordedAndRunsAgain(t *testing.T) {
	h := newHarness(t)
	h.source.fail = errors.New("github is down")
	id := h.accept(trigger("compile", compileNode("svc-a")))

	err := h.runner.Drive(ctx, id)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "github is down")
	row := h.stepRows(id, 1, 1)["failing_node_source"]
	require.Len(t, row, 1)
	assert.Equal(t, remediation.StepFailed, row[0].Status)
	assert.Equal(t, 1, row[0].Failures)
	assert.Contains(t, row[0].LastError, "github is down")
	assert.Equal(t, remediation.ClusterGathering, h.cluster(id, 1, 1).Status)

	require.NoError(t, h.runner.Drive(ctx, id))
	row = h.stepRows(id, 1, 1)["failing_node_source"]
	require.Len(t, row, 1)
	assert.Equal(t, remediation.StepDone, row[0].Status)
	assert.Equal(t, 1, row[0].Failures)
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
}

func TestRunner_PassedPreflightWritesTheArtifacts(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
	content, ok := h.artifacts.get("proposed-fix/rel-1/svc-a/attempt-1.source.sql")
	require.True(t, ok)
	assert.Equal(t, "select 2", content)
	diff, ok := h.artifacts.get("proposed-fix/rel-1/svc-a/attempt-1.source.diff")
	require.True(t, ok)
	assert.Equal(t, proposal.ComputeUnifiedDiff("select 1", "select 2", "services/svc-a/models/orders.sql"), diff)

	out, err := serialization.DecodeRecord[serialization.PreflightRecord](h.stepRows(id, 1, 1)["preflight"][0].Output)
	require.NoError(t, err)
	require.Len(t, out.Draft.Edits, 1)
	e := out.Draft.Edits[0]
	assert.Equal(t, "services/svc-a/models/orders.sql", e.Path)
	assert.Equal(t, "select 1", e.Original)
	assert.Equal(t, "s3://artifacts/proposed-fix/rel-1/svc-a/attempt-1.source.sql", e.ContentURI)
	assert.Equal(t, "s3://artifacts/proposed-fix/rel-1/svc-a/attempt-1.source.diff", e.DiffURI)
}

func TestRunner_PreflightFindingHandsTheClusterOver(t *testing.T) {
	cases := []struct {
		name     string
		answer   func(r *ports.ProposeResult)
		outcome  proposal.NodeOutcome
		findings []string
	}{
		{
			name:   "a target the model was not shown is skipped",
			answer: func(r *ports.ProposeResult) { r.TargetFile = "models/elsewhere.sql" },
			outcome: proposal.NodeOutcome{Status: proposal.StatusSkipped,
				Reason: `the model named "models/elsewhere.sql" as the file to change, which is not one of the files it was shown`},
			findings: []string{"target_not_shown", "content_unchanged"},
		},
		{
			name:     "a low-confidence draft is failed",
			answer:   func(r *ports.ProposeResult) { r.Confidence = "low" },
			outcome:  proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: "the model could not determine a safe fix with confidence"},
			findings: []string{"low_confidence"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
				r := fixAnswer(req)
				tc.answer(&r)
				return r
			}
			id := h.accept(trigger("compile", compileNode("svc-a")))

			require.NoError(t, h.runner.Drive(ctx, id))

			c := h.cluster(id, 1, 1)
			assert.Equal(t, remediation.ClusterHandedOver, c.Status)
			assert.Equal(t, &tc.outcome, c.Outcome)
			var codes []string
			for _, f := range h.stepRows(id, 1, 1)["preflight"][0].Findings {
				codes = append(codes, f.Code)
			}
			assert.Equal(t, tc.findings, codes, "every finding of the pass is recorded")
		})
	}
}

func TestRunner_TriesSpentHandsOverOnce(t *testing.T) {
	h := newHarness(t)
	for attempt := 1; attempt <= 3; attempt++ {
		h.legacyAttempt(attempt, proposal.StatusFailed)
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))
	h.store.FailNextCommit()

	require.Error(t, h.runner.Drive(ctx, id), "the hand-over's commit fails")
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.runner.Drive(ctx, id))

	var escalated []proposal.Proposal
	for _, p := range h.proposals() {
		if p.Proposal.Status == proposal.StatusEscalated {
			escalated = append(escalated, p.Proposal)
		}
	}
	require.Len(t, escalated, 1)
	assert.Equal(t, 4, escalated[0].Attempt)
	assert.Equal(t, id.String(), escalated[0].RemediationID)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, h.store.Tries(id))
	assert.Zero(t, h.llm.calls())
}

func TestRunner_NextTryOpensATryOnlyWhenNoneIsOpen(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.NextTry(ctx, id))
	require.NoError(t, h.runner.NextTry(ctx, id))

	tries := h.store.Tries(id)
	require.Len(t, tries, 1)
	assert.Equal(t, remediation.TryDrafting, tries[0].Status)
	assert.Equal(t, remediation.ClusterGathering, h.cluster(id, 1, 1).Status)
	assert.Empty(t, h.store.Steps(id), "opening a try runs no step")
	assert.Zero(t, h.llm.calls())
}

func TestRunner_ARoundTwoTriggerContinuesTheAttemptsWithItsOwnTries(t *testing.T) {
	h := newHarness(t)
	for attempt := 1; attempt <= 3; attempt++ {
		h.legacyAttemptInRound(1, attempt, proposal.StatusFailed)
	}
	trig := trigger("compile", compileNode("svc-a"))
	trig.Round = 2
	id := h.accept(trig)

	require.NoError(t, h.runner.Drive(ctx, id))

	tries := h.store.Tries(id)
	require.Len(t, tries, 1, "round 1's attempts do not spend round 2's tries")
	assert.Equal(t, 4, tries[0].Attempt, "the attempt continues the release's sequence")
	for _, p := range h.proposals() {
		assert.NotEqual(t, proposal.StatusEscalated, p.Proposal.Status)
	}
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
}

func TestRunner_ThePromptIsCachedUnderTheRemediationTryClusterAndRound(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	key, err := llmcache.Key("test-model", "remediation:"+id.String()+":try:1:cluster:1:round:1", h.llm.lastRequest())
	require.NoError(t, err)
	assert.True(t, h.cache.has(key), "the answer is cached under the cluster's idempotency key")
}

func TestRunner_ALeaseLostDuringTheModelCallWritesNothing(t *testing.T) {
	h := newHarness(t)
	h.llm.onCall = func(int) {
		repo := h.store.RemediationRepository()
		id := h.store.Remediations()[0].ID
		require.NoError(t, repo.ReleaseLease(ctx, id, workerOwner))
		claimed, err := repo.Claim(ctx, id, "worker-2", h.clock.Now(), lease)
		require.NoError(t, err)
		require.True(t, claimed)
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id), "a lost lease stops the drive quietly")

	rows := h.stepRows(id, 1, 1)
	assert.Empty(t, rows["dbt-compile"], "the prompt's run is not recorded")
	assert.Len(t, rows["failure_precedents"], 1, "the steps before it stay committed")
	assert.Equal(t, remediation.ClusterProposing, h.cluster(id, 1, 1).Status)
}

func TestRunner_TheFlowContextRunsBeforeTheTypeContext(t *testing.T) {
	h := newHarness(t, withFlowContext())
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	var order, revisions []string
	for _, s := range h.store.Steps(id) {
		if s.Cluster != 1 {
			continue
		}
		order = append(order, s.Step)
		revisions = append(revisions, s.FileRevision)
	}
	assert.Equal(t, []string{"dbt_log", "failing_node_source", "failure_precedents", "dbt-compile",
		"target_file_was_shown", "content_changed", "confidence_not_low", "preflight"}, order)
	assert.Equal(t, flowRevision, revisions[0], "a flow step runs under the flow file's revision")
	assert.Equal(t, "rev-dbt-compile", revisions[1])
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
}

func TestRunner_AFailedRunRecordsNoOutput(t *testing.T) {
	h := newHarness(t)
	h.llm.onCall = func(int) { h.store.FailNextCommit() }
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.Error(t, h.runner.Drive(ctx, id), "the prompt step's commit fails")

	rows := h.stepRows(id, 1, 1)["dbt-compile"]
	require.Len(t, rows, 1)
	assert.Equal(t, remediation.StepFailed, rows[0].Status)
	assert.Equal(t, 1, rows[0].Failures)
	assert.Empty(t, rows[0].Output, "a failed run's output is not stored")
	assert.Empty(t, rows[0].Findings)
}

func TestRunner_AFailedRunKeepsNoNULInItsError(t *testing.T) {
	h := newHarness(t)
	h.source.fail = errors.New("bad\x00byte")
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.Error(t, h.runner.Drive(ctx, id))

	rows := h.stepRows(id, 1, 1)["failing_node_source"]
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0].Failures)
	assert.NotContains(t, rows[0].LastError, "\x00")
	assert.Contains(t, rows[0].LastError, "badbyte")
}

func TestRunner_AnUnknownTypeIsRecordedOnThePlanRow(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.NextTry(ctx, id))
	files := testFiles(3)
	delete(files.Types, "dbt-compile")
	r := h.runnerOver(files)

	require.Error(t, r.Drive(ctx, id))
	require.Error(t, r.Drive(ctx, id))

	rows := h.stepRows(id, 1, 1)["plan"]
	require.Len(t, rows, 1)
	assert.Equal(t, "runner", rows[0].Kind)
	assert.Equal(t, remediation.StepFailed, rows[0].Status)
	assert.Equal(t, 2, rows[0].Failures, "every drive counts the failure")
	assert.Contains(t, rows[0].LastError, `type "dbt-compile"`)
}

func TestRunner_AnUndecodableRowIsRecordedOnTheNextStep(t *testing.T) {
	cases := []struct {
		name    string
		done    []string
		corrupt string
		blamed  string
	}{
		{name: "a context step's evidence", done: []string{"failing_node_source"}, corrupt: "failing_node_source", blamed: "dbt_log"},
		{name: "the prompt's draft", done: []string{"failing_node_source", "dbt_log", "failure_precedents", "dbt-compile"}, corrupt: "dbt-compile", blamed: "preflight"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			id := h.accept(trigger("compile", compileNode("svc-a")))
			require.NoError(t, h.runner.NextTry(ctx, id))
			for _, name := range tc.done {
				out := `{"evidence":{"failure_precedents":null}}`
				switch name {
				case tc.corrupt:
					out = `{"evidence":{"source_files":"not an object"},"draft":"not an object"}`
				case "dbt-compile":
					out = `{"draft":{}}`
				}
				h.saveStep(remediation.StepRun{
					RemediationID: id, Try: 1, Cluster: 1, Step: name, Kind: "context", Round: 1,
					Status: remediation.StepDone, Output: []byte(out), StartedAt: t0,
				})
			}

			require.Error(t, h.runner.Drive(ctx, id))

			rows := h.stepRows(id, 1, 1)[tc.blamed]
			require.Len(t, rows, 1)
			assert.Equal(t, remediation.StepFailed, rows[0].Status)
			assert.Equal(t, 1, rows[0].Failures)
			assert.Contains(t, rows[0].LastError, "decode")
		})
	}
}
