package runner_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// The reasons the runner records a node with, as the tests read them.
const (
	dbtTestReason       = "dbt tests are not fix targets; fix the model or edit the test by hand"
	noTypeReason        = "no remediation type matches this failure"
	noFileReason        = "the compile error names no source file, so there is no file to fix"
	maintenanceReason   = "continuo was in maintenance mode."
	lowConfidenceReason = "the model could not determine a safe fix with confidence"
	twoEditsReason      = "two edits change the same file: services/core/models/base.sql"
)

// clusterFor returns the cluster of one try whose target is target.
func (h *harness) clusterFor(id uuid.UUID, try int, target string) remediation.Cluster {
	h.t.Helper()
	for _, c := range h.store.Clusters(id) {
		if c.Try == try && c.TargetNodeID == target {
			return c
		}
	}
	h.t.Fatalf("no cluster of try %d targets %s", try, target)
	return remediation.Cluster{}
}

// failGate makes every release check of the try fail on node and polls the
// gate, which fails the try and opens the next one.
func (h *harness) failGate(id uuid.UUID, node string) {
	h.t.Helper()
	h.checks.answer(fix.PhaseFailed, map[string]string{node: "column x does not exist"})
	require.NoError(h.t, h.poll(id))
}

// decliningPrompt stands in for a prompt step and declines every cluster
// without calling the model.
type decliningPrompt struct {
	step.Prompt
	reason string
}

func (p decliningPrompt) Propose(context.Context, step.PromptInput, step.Model) (step.PromptOutput, error) {
	return step.PromptOutput{Declined: true, Decline: p.reason}, nil
}

func TestRunner_ClassifierInfra(t *testing.T) {
	const detail = "the warehouse connection dropped; release again once it is back"
	h := newHarness(t)
	id := h.accept(trigger("compile", unfixable(compileNode("svc-a"), model.HandoverReasonClassifierInfra, detail), compileNode("svc-b")))

	require.NoError(t, h.runner.NextTry(ctx, id))

	want := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: detail, Code: model.HandoverReasonClassifierInfra}
	carried := h.clusterFor(id, 1, "svc-a")
	assert.Equal(t, workflow.ClusterSingle, carried.Kind)
	assert.Equal(t, []string{"svc-a"}, carried.Members)
	assert.Equal(t, "", carried.TypeName, "no type is matched for a node the classifier marked unfixable")
	assert.Equal(t, remediation.ClusterHandedOver, carried.Status)
	assert.Equal(t, &want, carried.Outcome)
	assert.Equal(t, "dbt-compile", h.clusterFor(id, 1, "svc-b").TypeName)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": want,
		"svc-b": {Status: fix.StatusGenerating},
	}, h.fixRow(1).Fix.NodeOutcomes, "the in-flight row names the node handed over from the start")

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Empty(t, h.stepRows(id, 1, carried.Cluster), "no step runs for the unfixable node")
	require.Equal(t, 1, h.llm.calls(), "the model is asked about svc-b alone")
	assert.Contains(t, h.llm.lastRequest().User, "services/svc-b/")
	assert.NotContains(t, h.llm.lastRequest().User, "services/svc-a/")
	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusVerifying, p.Status)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": want,
		"svc-b": {Status: fix.StatusVerifying},
	}, p.NodeOutcomes)
	runs := h.checks.requests()
	require.Len(t, runs, 1)
	assert.Equal(t, svcBCheck1, runs[0].CheckID)
}

// An unhealable parse failure keeps the classifier's outcome in a try
// that comes due in maintenance mode, where every other node is skipped with
// no code: maintenance is not a hand-over.
func TestRunner_ClassifierUnhealableParse(t *testing.T) {
	const detail = "the parser refuses a construct no edit of the source avoids"
	h := newHarness(t, withMaintenance())
	broken := remediation.TriggerNode{NodeID: "model.sales.orders", ErrorSignature: "sig-o", NodeType: "dbt-model", Project: "svc-a", FilePath: "models/orders.sql"}
	other := remediation.TriggerNode{NodeID: "model.sales.items", ErrorSignature: "sig-i", NodeType: "dbt-model", Project: "svc-a", FilePath: "models/items.sql"}
	id := h.accept(trigger("parse", unfixable(broken, model.HandoverReasonClassifierUnhealableParse, detail), other))

	require.NoError(t, h.runner.Drive(ctx, id))

	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusSkipped, p.Status)
	assert.Equal(t, map[string]fix.NodeOutcome{
		broken.NodeID: {Status: fix.StatusSkipped, Reason: detail, Code: model.HandoverReasonClassifierUnhealableParse},
		other.NodeID:  {Status: fix.StatusSkipped, Reason: maintenanceReason},
	}, p.NodeOutcomes)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Zero(t, h.llm.calls())
}

func TestRunner_NoType(t *testing.T) {
	h := newHarness(t)
	test := remediation.TriggerNode{NodeID: "test.sales.not_null", ErrorSignature: "sig-t", NodeType: "dbt-test", Project: "svc-a"}
	python := remediation.TriggerNode{NodeID: "python.sales.loader", ErrorSignature: "sig-p", NodeType: "python-node", Project: "svc-a"}
	id := h.accept(trigger("validation", test, python))

	require.NoError(t, h.runner.Drive(ctx, id))

	want := map[string]fix.NodeOutcome{
		test.NodeID:   {Status: fix.StatusSkipped, Reason: dbtTestReason, Code: model.HandoverReasonNoType},
		python.NodeID: {Status: fix.StatusSkipped, Reason: noTypeReason, Code: model.HandoverReasonNoType},
	}
	for node, o := range want {
		c := h.clusterFor(id, 1, node)
		assert.Equal(t, remediation.ClusterHandedOver, c.Status, node)
		assert.Equal(t, &o, c.Outcome, node)
	}
	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusSkipped, p.Status)
	assert.Equal(t, want, p.NodeOutcomes)
	assert.Equal(t, []string{"hand_over"}, stepNames(h.store.Steps(id)), "no step runs, and the hand-over is recorded")
	assert.Zero(t, h.llm.calls())
}

// A python-csv or python-api node whose parse failed matches no built-in type:
// no step of the dbt-parse type runs for it, and it is handed over without a
// model call.
func TestRunner_APythonParseFailureNoTypeMatchesIsHandedOverWithoutAModelCall(t *testing.T) {
	for _, nodeType := range []string{"python-csv", "python-api"} {
		t.Run(nodeType, func(t *testing.T) {
			h := newHarness(t, withBuiltinFiles())
			node := remediation.TriggerNode{
				NodeID: "python.sales.fx", ErrorSignature: "sig-p", Category: "parse", Reason: "parse_error",
				NodeType: nodeType, Project: "svc-a", FilePath: "contracts/fx.yaml",
			}
			id := h.accept(trigger("parse", node))

			require.NoError(t, h.runner.Drive(ctx, id))

			want := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: noTypeReason, Code: model.HandoverReasonNoType}
			c := h.cluster(id, 1, 1)
			assert.Equal(t, "", c.TypeName, "type %s now matches a %s node whose parse failed. This test says no built-in type does, "+
				"so a built-in type that takes the case over must also drop %s from this test's node types, "+
				"and its own fixture must test it", c.TypeName, nodeType, nodeType)
			assert.Equal(t, remediation.ClusterHandedOver, c.Status)
			assert.Equal(t, &want, c.Outcome)
			assert.Equal(t, []string{"hand_over"}, stepNames(h.store.Steps(id)), "no step runs, and the hand-over is recorded")
			assert.Zero(t, h.llm.calls())
			p := h.fixRow(1).Fix
			assert.Equal(t, fix.StatusSkipped, p.Status)
			assert.Equal(t, map[string]fix.NodeOutcome{node.NodeID: want}, p.NodeOutcomes)
		})
	}
}

func TestRunner_Declined(t *testing.T) {
	t.Run("a context step declines", func(t *testing.T) {
		h := newHarness(t)
		node := compileNode("svc-a")
		node.FilePath = ""
		id := h.accept(trigger("compile", node))

		require.NoError(t, h.runner.Drive(ctx, id))

		want := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: noFileReason, Code: model.HandoverReasonDeclined}
		c := h.cluster(id, 1, 1)
		assert.Equal(t, remediation.ClusterHandedOver, c.Status)
		assert.Equal(t, &want, c.Outcome)
		p := h.fixRow(1).Fix
		assert.Equal(t, fix.StatusSkipped, p.Status)
		assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": want}, p.NodeOutcomes)
		assert.Zero(t, h.llm.calls())
	})

	t.Run("the prompt step declines", func(t *testing.T) {
		const reason = "no edit of the release's source reaches this node"
		h := newHarness(t)
		orig, _, ok := h.registry().Lookup("dbt-compile")
		require.True(t, ok)
		h.replaceStep(decliningPrompt{Prompt: orig.(step.Prompt), reason: reason})
		id := h.accept(trigger("compile", compileNode("svc-a")))

		require.NoError(t, h.runner.Drive(ctx, id))

		want := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: reason, Code: model.HandoverReasonDeclined}
		c := h.cluster(id, 1, 1)
		assert.Equal(t, remediation.ClusterHandedOver, c.Status)
		assert.Equal(t, &want, c.Outcome)
		assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": want}, h.fixRow(1).Fix.NodeOutcomes)
	})

	t.Run("a cross-project cluster falls back", func(t *testing.T) {
		h := newHarness(t)
		id := h.accept(trigger("compile", crossProjectNode()))

		require.NoError(t, h.runner.Drive(ctx, id))

		want := crossProjectDeclined()
		c := h.cluster(id, 1, 1)
		assert.Equal(t, remediation.ClusterFellBack, c.Status)
		assert.Equal(t, &want, c.Outcome)
		assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": want}, h.fixRow(1).Fix.NodeOutcomes)
		assert.Zero(t, h.llm.calls())
	})

	t.Run("a draft with no edit passes pre-flight", func(t *testing.T) {
		h := newHarness(t, withPreflight("dbt-compile", "confidence_not_low"))
		h.llm.answer = noEditAnswer
		id := h.accept(trigger("compile", compileNode("svc-a")))

		require.NoError(t, h.runner.Drive(ctx, id))

		c := h.cluster(id, 1, 1)
		assert.Equal(t, remediation.ClusterHandedOver, c.Status)
		assert.Equal(t, &noEditDeclined, c.Outcome)
		p := h.fixRow(1).Fix
		assert.Equal(t, fix.StatusFailed, p.Status, "a node the model was asked about fails the try")
		assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": noEditDeclined}, p.NodeOutcomes)
		assert.Empty(t, h.checks.requests())
	})
}

// A step that declines with no reason is recorded as having declined the fix,
// so the fix row, the hand-over event and every reader of them carry a
// message. A reason of only white space is no reason. A step's own reason is
// kept as it is.
func TestRunner_AStepThatDeclinesWithNoReasonIsNamed(t *testing.T) {
	const missing = "step dbt-compile declined the fix"
	for name, tc := range map[string]struct {
		reason string
		want   string
	}{
		"no reason":               {reason: "", want: missing},
		"a reason of white space": {reason: " \t\n", want: missing},
		"a reason":                {reason: "no edit reaches this node", want: "no edit reaches this node"},
	} {
		t.Run("the prompt step gives "+name, func(t *testing.T) {
			h := newHarness(t)
			orig, _, ok := h.registry().Lookup("dbt-compile")
			require.True(t, ok)
			h.replaceStep(decliningPrompt{Prompt: orig.(step.Prompt), reason: tc.reason})
			id := h.accept(trigger("compile", compileNode("svc-a")))

			require.NoError(t, h.runner.Drive(ctx, id))

			want := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: tc.want, Code: model.HandoverReasonDeclined}
			assert.Equal(t, &want, h.cluster(id, 1, 1).Outcome)
			assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": want}, h.fixRow(1).Fix.NodeOutcomes)
			assert.Equal(t, handedOverOf(id, 1, 1, handedOverNode("svc-a", want)), h.handedOverEvent())
		})
	}

	t.Run("a context step gives no reason", func(t *testing.T) {
		h := newHarness(t)
		node := remediation.TriggerNode{NodeID: "model.sales.orders", ErrorSignature: "sig-v", NodeType: "dbt-model", Project: "svc-a"}
		id := h.accept(trigger("validation", node))

		require.NoError(t, h.runner.Drive(ctx, id))

		want := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: "step candidate_sql declined the fix", Code: model.HandoverReasonDeclined}
		assert.Equal(t, &want, h.cluster(id, 1, 1).Outcome)
		assert.Equal(t, handedOverOf(id, 1, 1, handedOverNode(node.NodeID, want)), h.handedOverEvent())
	})

	t.Run("a cross-project cluster whose step gives no reason", func(t *testing.T) {
		h := newHarness(t)
		orig, _, ok := h.registry().Lookup("upstream")
		require.True(t, ok)
		h.replaceStep(decliningPrompt{Prompt: orig.(step.Prompt), reason: ""})
		h.bundle.nodes = map[string]ports.CandidateSource{"model.core.base": {RawCode: "select 1", Runtime: "dbt"}}
		trig := trigger("compile", crossProjectNode())
		trig.CodeBundleURI = "s3://bundles/rel-1.tar.gz"
		id := h.accept(trig)

		require.NoError(t, h.runner.Drive(ctx, id))

		o := h.cluster(id, 1, 1).Outcome
		require.NotNil(t, o)
		assert.Equal(t, model.HandoverReasonDeclined, o.Code)
		assert.Equal(t, "the change that broke this node lives in project core; a fix in this node's own project cannot ship before it: "+
			"step upstream declined the fix", o.Reason)
	})
}

// noEditDeclined is the outcome of the members of a cluster whose draft passed
// pre-flight with no edit.
var noEditDeclined = fix.NodeOutcome{
	Status: fix.StatusFailed,
	Reason: "the fix could not be applied to version-controlled source, so there is nothing to verify",
	Code:   model.HandoverReasonDeclined,
}

// noEditAnswer names a file the model was not shown, so the compile prompt's
// draft holds no edit.
func noEditAnswer(req prompt.ProposeRequest) ports.ProposeResult {
	r := fixAnswer(req)
	r.TargetFile = "models/elsewhere.sql"
	return r
}

// A draft that passes pre-flight with no edit is a decline: the cluster is
// handed over declined, and the next try carries the node as settled and does
// not group it again. The type here checks only the draft's confidence, so a
// draft with no edit passes its pre-flight.
func TestRunner_ADraftWithNoEditIsDeclinedAndNotTriedAgain(t *testing.T) {
	h := newHarness(t, withPreflight("dbt-compile", "confidence_not_low"))
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		if strings.Contains(req.User, "services/svc-a/") {
			return noEditAnswer(req)
		}
		return fixAnswer(req)
	}
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))
	require.NoError(t, h.runner.Drive(ctx, id))

	declinedCluster := h.clusterFor(id, 1, "svc-a")
	assert.Equal(t, remediation.ClusterHandedOver, declinedCluster.Status)
	assert.Equal(t, &noEditDeclined, declinedCluster.Outcome)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": noEditDeclined,
		"svc-b": {Status: fix.StatusVerifying},
	}, h.fixRow(1).Fix.NodeOutcomes)
	require.Equal(t, 2, h.llm.calls())

	h.failGate(id, "svc-b")

	assert.Equal(t, noEditDeclined, h.fixRow(1).Fix.NodeOutcomes["svc-a"], "the failed gate leaves the declined node as it was")
	carried := h.clusterFor(id, 2, "svc-a")
	assert.Equal(t, "dbt-compile", carried.TypeName, "the carried cluster keeps the type of the cluster that settled the node")
	assert.Equal(t, remediation.ClusterHandedOver, carried.Status)
	assert.Equal(t, &noEditDeclined, carried.Outcome)
	for _, c := range h.store.Clusters(id) {
		if c.Try == 2 && c.Cluster != carried.Cluster {
			assert.Equal(t, []string{"svc-b"}, c.Members, "only svc-b is grouped again")
		}
	}

	require.NoError(t, h.drive(id))

	assert.Empty(t, h.stepRows(id, 2, carried.Cluster), "no step runs again for the declined node")
	assert.Equal(t, 3, h.llm.calls(), "the second try asks the model about svc-b alone")
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": noEditDeclined,
		"svc-b": {Status: fix.StatusVerifying},
	}, h.fixRow(2).Fix.NodeOutcomes)
}

// crossProjectNode is svc-a, broken by a change to model.core.base in core,
// whose source the release's code bundle does not hold.
func crossProjectNode() remediation.TriggerNode {
	n := compileNode("svc-a")
	n.Project = "svc-a"
	n.ChangedAncestors = []remediation.ChangedAncestor{{NodeID: "model.core.base", FilePath: "models/base.sql", Project: "core", Depth: 1}}
	return n
}

// crossProjectDeclined is the outcome of crossProjectNode once its
// cross-project cluster fell back.
func crossProjectDeclined() fix.NodeOutcome {
	return fix.NodeOutcome{
		Status: fix.StatusSkipped,
		Reason: "the change that broke this node lives in project core; a fix in this node's own project cannot ship before it: " +
			"the changed ancestor's source is not in the release's code bundle",
		Code: model.HandoverReasonDeclined,
	}
}

// A trigger with no repository fails every node a fix could reach with
// no_repository; a node the classifier marked unfixable keeps its outcome.
func TestRunner_NoRepository(t *testing.T) {
	const detail = "the warehouse connection dropped; release again once it is back"
	h := newHarness(t)
	trig := trigger("compile", unfixable(compileNode("svc-a"), model.HandoverReasonClassifierInfra, detail), compileNode("svc-b"))
	trig.Repo = ""
	id := h.accept(trig)

	require.NoError(t, h.runner.Drive(ctx, id))

	const reason = "the trigger carried no repository for rel-1"
	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusFailed, p.Status)
	assert.Equal(t, reason, p.Rationale)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": {Status: fix.StatusSkipped, Reason: detail, Code: model.HandoverReasonClassifierInfra},
		"svc-b": {Status: fix.StatusFailed, Reason: reason, Code: model.HandoverReasonNoRepository},
	}, p.NodeOutcomes)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Zero(t, h.llm.calls())
}

func TestRunner_PreflightRoundsSpent(t *testing.T) {
	h := newHarness(t)
	h.llm.answer = lowConfidence
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	want := fix.NodeOutcome{Status: fix.StatusFailed, Reason: lowConfidenceReason, Code: model.HandoverReasonPreflightRoundsSpent}
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterHandedOver, c.Status)
	assert.Equal(t, &want, c.Outcome, "the reason is the first finding alone; the code says the rounds are spent")
	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusFailed, p.Status)
	assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": want}, p.NodeOutcomes)
	assert.Equal(t, 1, h.llm.calls())
}

// A try whose drafts a check refuses, a cross-cluster check or the gate at
// submit, fails each drafted member with unverifiable_edits while a try is
// left; a cluster handed over before any draft keeps its own outcome.
func TestRunner_RefusedTryMarksUnverifiableEdits(t *testing.T) {
	for name, opts := range map[string][]option{
		"a cross-cluster check": nil,
		"the gate":              {withoutCrossClusterChecks()},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, opts...)
			h.twoEditsOfOneFile()
			trig := crossClusterTrigger()
			test := compileNode("svc-b")
			test.NodeType = "dbt-test"
			trig.Nodes = append(trig.Nodes, test)
			id := h.accept(trig)

			require.NoError(t, h.runner.Drive(ctx, id))

			refused := fix.NodeOutcome{Status: fix.StatusFailed, Reason: twoEditsReason, Code: model.HandoverReasonUnverifiableEdits}
			p := h.fixRow(1).Fix
			assert.Equal(t, fix.StatusFailed, p.Status)
			assert.Equal(t, map[string]fix.NodeOutcome{
				"svc-a": refused,
				"core":  refused,
				"svc-b": {Status: fix.StatusSkipped, Reason: dbtTestReason, Code: model.HandoverReasonNoType},
			}, p.NodeOutcomes)
			assert.GreaterOrEqual(t, len(h.store.Tries(id)), 2, "a try was left, so the next one opened")
		})
	}
}

// A node handed over as declined is settled for good: the next try carries it
// as a cluster already handed over with the same outcome, runs no step for it
// and groups only the other nodes.
func TestRunner_ADeclinedNodeIsNotTriedAgain(t *testing.T) {
	noFile := compileNode("svc-a")
	noFile.FilePath = ""
	cases := []struct {
		name     string
		node     remediation.TriggerNode
		outcome  fix.NodeOutcome
		typeName string
	}{
		{
			name:     "a context step declined",
			node:     noFile,
			outcome:  fix.NodeOutcome{Status: fix.StatusSkipped, Reason: noFileReason, Code: model.HandoverReasonDeclined},
			typeName: "dbt-compile",
		},
		{
			name:     "its cross-project cluster fell back",
			node:     crossProjectNode(),
			outcome:  crossProjectDeclined(),
			typeName: "cross-project-upstream",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			id := h.accept(trigger("compile", tc.node, compileNode("svc-b")))
			require.NoError(t, h.runner.Drive(ctx, id))
			require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
			calls := h.llm.calls()

			h.failGate(id, "svc-b")

			assert.Equal(t, map[string]fix.NodeOutcome{
				"svc-a": tc.outcome,
				"svc-b": {Status: fix.StatusFailed},
			}, h.fixRow(1).Fix.NodeOutcomes, "the failed gate fails the verifying node; the declined one keeps its outcome")
			require.Equal(t, remediation.TryDrafting, h.try(id, 2).Status)
			carried := h.clusterFor(id, 2, "svc-a")
			assert.Equal(t, workflow.ClusterSingle, carried.Kind)
			assert.Equal(t, []string{"svc-a"}, carried.Members)
			assert.Equal(t, tc.typeName, carried.TypeName, "the carried cluster keeps the type of the cluster that settled the node")
			assert.Equal(t, remediation.ClusterHandedOver, carried.Status)
			assert.Equal(t, &tc.outcome, carried.Outcome)
			for _, c := range h.store.Clusters(id) {
				if c.Try == 2 && c.Cluster != carried.Cluster {
					assert.Equal(t, []string{"svc-b"}, c.Members, "only svc-b is grouped again")
				}
			}
			assert.Equal(t, map[string]fix.NodeOutcome{
				"svc-a": tc.outcome,
				"svc-b": {Status: fix.StatusGenerating},
			}, h.fixRow(2).Fix.NodeOutcomes)

			require.NoError(t, h.drive(id))

			assert.Empty(t, h.stepRows(id, 2, carried.Cluster), "no step runs again for the declined node")
			assert.Equal(t, calls+1, h.llm.calls(), "the second try asks the model about svc-b alone")
			assert.Equal(t, remediation.TryVerifying, h.try(id, 2).Status)
			assert.Equal(t, map[string]fix.NodeOutcome{
				"svc-a": tc.outcome,
				"svc-b": {Status: fix.StatusVerifying},
			}, h.fixRow(2).Fix.NodeOutcomes)
		})
	}
}

// A node handed over because its rounds are spent is not settled: the next
// try groups it again and asks the model about it again.
func TestRunner_ARoundsSpentNodeIsTriedAgain(t *testing.T) {
	h := newHarness(t)
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		if strings.Contains(req.User, "services/svc-a/") {
			return lowConfidence(req)
		}
		return fixAnswer(req)
	}
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))
	require.NoError(t, h.runner.Drive(ctx, id))
	spent := h.clusterFor(id, 1, "svc-a")
	require.Equal(t, remediation.ClusterHandedOver, spent.Status)
	require.Equal(t, &fix.NodeOutcome{
		Status: fix.StatusFailed, Reason: lowConfidenceReason, Code: model.HandoverReasonPreflightRoundsSpent,
	}, spent.Outcome)

	h.failGate(id, "svc-b")

	again := h.clusterFor(id, 2, "svc-a")
	assert.Equal(t, "dbt-compile", again.TypeName)
	assert.Equal(t, remediation.ClusterGathering, again.Status)
	assert.Nil(t, again.Outcome)
	assert.Equal(t, fix.NodeOutcome{Status: fix.StatusGenerating}, h.fixRow(2).Fix.NodeOutcomes["svc-a"])

	require.NoError(t, h.drive(id))

	assert.Equal(t, 4, h.llm.calls(), "each try asks the model about both nodes")
	assert.Equal(t, remediation.ClusterHandedOver, h.clusterFor(id, 2, "svc-a").Status)
}

// A cross-project cluster that fell back hands its members over with the code
// of the cause: declined when a step declined it, which settles them for good,
// and preflight_rounds_spent when pre-flight refused every round, which leaves
// them to be grouped again in the next try.
func TestRunner_ACrossProjectFallbackKeepsTheCodeOfItsCause(t *testing.T) {
	const bundleURI = "s3://bundles/rel-1.tar.gz"
	upstreamSource := map[string]ports.CandidateSource{"model.core.base": {RawCode: "select 1", Runtime: "dbt"}}
	// clusterOf is the cluster of try that holds node as a member.
	clusterOf := func(h *harness, id uuid.UUID, try int, node string) remediation.Cluster {
		h.t.Helper()
		for _, c := range h.store.Clusters(id) {
			if c.Try == try && slices.Contains(c.Members, node) {
				return c
			}
		}
		h.t.Fatalf("no cluster of try %d holds %s", try, node)
		return remediation.Cluster{}
	}

	t.Run("a step declined the upstream fix", func(t *testing.T) {
		h := newHarness(t)
		trig := trigger("compile", crossProjectNode(), compileNode("svc-b"))
		trig.CodeBundleURI = bundleURI
		id := h.accept(trig)
		require.NoError(t, h.runner.Drive(ctx, id))
		fellBack := clusterOf(h, id, 1, "svc-a")
		require.Equal(t, remediation.ClusterFellBack, fellBack.Status)
		require.Equal(t, &fix.NodeOutcome{
			Status: fix.StatusSkipped,
			Reason: "the change that broke this node lives in project core; a fix in this node's own project cannot ship before it: " +
				"the changed ancestor's source is not in the release's code bundle",
			Code: model.HandoverReasonDeclined,
		}, fellBack.Outcome)

		h.failGate(id, "svc-b")

		carried := clusterOf(h, id, 2, "svc-a")
		assert.Equal(t, remediation.ClusterHandedOver, carried.Status, "the members are settled, so the next try carries them handed over")
		assert.Equal(t, "cross-project-upstream", carried.TypeName, "the carried cluster keeps the type of the cluster that settled the node")
		assert.Equal(t, fellBack.Outcome, carried.Outcome)
		assert.Equal(t, fix.NodeOutcome{Status: fix.StatusSkipped, Reason: fellBack.Outcome.Reason, Code: model.HandoverReasonDeclined},
			h.fixRow(2).Fix.NodeOutcomes["svc-a"])
	})

	t.Run("pre-flight refused every round of the upstream fix", func(t *testing.T) {
		h := newHarness(t)
		h.bundle.nodes = upstreamSource
		h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
			r := fixAnswer(req)
			if strings.Contains(req.User, "model.core.base") {
				r.ProposedSQL, r.Confidence = "select 3", "low"
			}
			return r
		}
		trig := trigger("compile", crossProjectNode(), compileNode("svc-b"))
		trig.CodeBundleURI = bundleURI
		id := h.accept(trig)
		require.NoError(t, h.runner.Drive(ctx, id))
		fellBack := clusterOf(h, id, 1, "svc-a")
		require.Equal(t, workflow.ClusterCrossProject, fellBack.Kind)
		require.Equal(t, remediation.ClusterFellBack, fellBack.Status)
		require.NotNil(t, fellBack.Outcome)
		require.Equal(t, model.HandoverReasonPreflightRoundsSpent, fellBack.Outcome.Code)
		require.Equal(t, fix.StatusSkipped, fellBack.Outcome.Status)
		require.Contains(t, fellBack.Outcome.Reason, "the change that broke this node lives in project core; "+
			"a fix in this node's own project cannot ship before it: "+lowConfidenceReason)
		assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": *fellBack.Outcome, "svc-b": {Status: fix.StatusVerifying}},
			h.fixRow(1).Fix.NodeOutcomes)
		calls := h.llm.calls()

		h.failGate(id, "svc-b")

		again := clusterOf(h, id, 2, "svc-a")
		assert.Equal(t, workflow.ClusterCrossProject, again.Kind, "the members are grouped again")
		assert.Equal(t, "cross-project-upstream", again.TypeName)
		assert.Equal(t, remediation.ClusterGathering, again.Status)
		assert.Nil(t, again.Outcome)
		assert.Equal(t, fix.NodeOutcome{Status: fix.StatusGenerating}, h.fixRow(2).Fix.NodeOutcomes["svc-a"])

		require.NoError(t, h.drive(id))

		assert.Equal(t, calls*2, h.llm.calls(), "the second try asks the model about the upstream fix again")
		assert.Equal(t, remediation.ClusterFellBack, clusterOf(h, id, 2, "svc-a").Status)
	})
}

// A trigger whose every node the classifier marked unfixable writes one
// fix row, skipped, with each node skipped with its classifier code, and
// the remediation is done without a step or a model call.
func TestRunner_AllUnfixableWritesOneSkippedTry(t *testing.T) {
	const (
		infraDetail = "the warehouse connection dropped; release again once it is back"
		parseDetail = "the parser refuses a construct no edit of the source avoids"
	)
	h := newHarness(t)
	orders := remediation.TriggerNode{NodeID: "model.sales.orders", ErrorSignature: "sig-o", NodeType: "dbt-model", Project: "svc-a", FilePath: "models/orders.sql"}
	items := remediation.TriggerNode{NodeID: "model.sales.items", ErrorSignature: "sig-i", NodeType: "dbt-model", Project: "svc-a", FilePath: "models/items.sql"}
	id := h.accept(trigger("parse",
		unfixable(orders, model.HandoverReasonClassifierInfra, infraDetail),
		unfixable(items, model.HandoverReasonClassifierUnhealableParse, parseDetail)))

	require.NoError(t, h.runner.Drive(ctx, id))

	want := map[string]fix.NodeOutcome{
		orders.NodeID: {Status: fix.StatusSkipped, Reason: infraDetail, Code: model.HandoverReasonClassifierInfra},
		items.NodeID:  {Status: fix.StatusSkipped, Reason: parseDetail, Code: model.HandoverReasonClassifierUnhealableParse},
	}
	ps := h.fixes()
	require.Len(t, ps, 1, "one fix row")
	p := ps[0].Fix
	assert.Equal(t, 1, p.Try)
	assert.Equal(t, fix.StatusSkipped, p.Status)
	assert.Equal(t, want, p.NodeOutcomes)
	assert.Equal(t, []string{items.NodeID, orders.NodeID}, p.ResolvedNodeIDs)
	for node, o := range want {
		c := h.clusterFor(id, 1, node)
		assert.Equal(t, remediation.ClusterHandedOver, c.Status, node)
		assert.Equal(t, &o, c.Outcome, node)
	}
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Len(t, h.store.Tries(id), 1)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Equal(t, []string{"hand_over"}, stepNames(h.store.Steps(id)), "no step runs, and the hand-over is recorded")
	assert.Zero(t, h.llm.calls())
	assert.Empty(t, h.checks.requests())
}
