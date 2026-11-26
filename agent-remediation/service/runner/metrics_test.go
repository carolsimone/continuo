package runner_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rametrics "github.com/carolsimone/continuo/agent-remediation/adapters/metrics"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
)

// recordedMetrics keeps every sample the runner counts, each as a
// slash-joined string of its labels, in the order it was counted.
type recordedMetrics struct {
	mu       sync.Mutex
	tries    []string
	rounds   []string
	outcomes []string
	failures []string
	tokens   []string
	stages   []string
	// stageTime is the duration of each stage sample, by the same string.
	stageTime map[string][]time.Duration
}

var _ ports.RemediationMetrics = (*recordedMetrics)(nil)

func (m *recordedMetrics) add(into *[]string, parts ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	*into = append(*into, strings.Join(parts, "/"))
}

func (m *recordedMetrics) Try(flow, typ, result string)   { m.add(&m.tries, flow, typ, result) }
func (m *recordedMetrics) Round(flow, typ, result string) { m.add(&m.rounds, flow, typ, result) }
func (m *recordedMetrics) Outcome(flow, typ, outcome, reason string) {
	m.add(&m.outcomes, flow, typ, outcome, reason)
}
func (m *recordedMetrics) SystemFailure(flow, reason string) { m.add(&m.failures, flow, reason) }
func (m *recordedMetrics) Stage(flow, typ, stage string, d time.Duration) {
	m.add(&m.stages, flow, typ, stage)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stageTime == nil {
		m.stageTime = map[string][]time.Duration{}
	}
	key := strings.Join([]string{flow, typ, stage}, "/")
	m.stageTime[key] = append(m.stageTime[key], d)
}
func (m *recordedMetrics) Tokens(flow, typ, direction string, n int) {
	m.add(&m.tokens, flow, typ, direction, fmt.Sprint(n))
}

// snapshot returns a copy of one list of samples.
func (m *recordedMetrics) snapshot(list *[]string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(*list)
}

func (m *recordedMetrics) gotTries() []string    { return m.snapshot(&m.tries) }
func (m *recordedMetrics) gotRounds() []string   { return m.snapshot(&m.rounds) }
func (m *recordedMetrics) gotOutcomes() []string { return m.snapshot(&m.outcomes) }
func (m *recordedMetrics) gotFailures() []string { return m.snapshot(&m.failures) }
func (m *recordedMetrics) gotTokens() []string   { return m.snapshot(&m.tokens) }
func (m *recordedMetrics) gotStages() []string   { return m.snapshot(&m.stages) }

// durations is the durations of the samples of one stage.
func (m *recordedMetrics) durations(key string) []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.stageTime[key])
}

const flowName = "rejected-release"

// sample is a slash-joined sample of the flow rejected-release.
func sample(parts ...string) string { return flowName + "/" + strings.Join(parts, "/") }

// Each cluster a try worked counts once, when the try ends: a try that waits
// on its verification counts nothing, and a second try counts its clusters
// again.
func TestRunner_CountsOneTryPerClusterPerTry(t *testing.T) {
	m := &recordedMetrics{}
	h := newHarness(t, withMetrics(m))
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))

	require.NoError(t, h.runner.Drive(ctx, id))
	assert.Empty(t, m.gotTries(), "a verifying try has not ended")

	h.failGate(id, "svc-a")
	assert.ElementsMatch(t, []string{
		sample("dbt-compile", "gate_failed"),
		sample("dbt-compile", "gate_failed"),
	}, m.gotTries(), "the failed try counts each of its two clusters once")

	require.NoError(t, h.drive(id))
	assert.Len(t, m.gotTries(), 2, "the second try waits on its verification")
	h.pipeline.answer(proposal.PhasePassed, nil)
	require.NoError(t, h.poll(id))

	assert.ElementsMatch(t, []string{
		sample("dbt-compile", "gate_failed"),
		sample("dbt-compile", "gate_failed"),
		sample("dbt-compile", "passed"),
		sample("dbt-compile", "passed"),
	}, m.gotTries())
}

// A cluster counts the result of the try that worked it: the verdict of the
// gate, a refusal of the drafts by a cross-cluster check or by the gate at
// submit, every pre-flight round spent, or a decline.
func TestRunner_CountsTheResultOfEachTry(t *testing.T) {
	t.Run("the gate passed", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))
		h.pipeline.answer(proposal.PhasePassed, nil)
		require.NoError(t, h.poll(id))

		assert.Equal(t, []string{sample("dbt-compile", "passed")}, m.gotTries())
	})

	t.Run("a verification run failed on the last try", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withTries(1))
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))
		h.failGate(id, "svc-a")

		assert.Equal(t, []string{sample("dbt-compile", "gate_failed")}, m.gotTries())
	})

	for name, opts := range map[string][]option{
		"a cross-cluster check refused the drafts": nil,
		"the gate refused the drafts at submit":    {withoutCrossClusterChecks()},
	} {
		t.Run(name, func(t *testing.T) {
			m := &recordedMetrics{}
			h := newHarness(t, append(opts, withMetrics(m), withTries(1))...)
			h.twoEditsOfOneFile()
			id := h.accept(crossClusterTrigger())
			require.NoError(t, h.runner.Drive(ctx, id))

			assert.ElementsMatch(t, []string{
				sample(h.clusterFor(id, 1, "model.core.base").TypeName, "checks_failed"),
				sample(h.clusterFor(id, 1, "core").TypeName, "checks_failed"),
			}, m.gotTries())
			assert.Equal(t, "cross-project-upstream", h.clusterFor(id, 1, "model.core.base").TypeName)
		})
	}

	t.Run("every pre-flight round was spent", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withTries(1))
		h.llm.answer = lowConfidence
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, []string{sample("dbt-compile", "preflight_spent")}, m.gotTries())
	})

	t.Run("a step declined", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withTries(1))
		orig, _, ok := h.registry().Lookup("dbt-compile")
		require.True(t, ok)
		h.replaceStep(decliningPrompt{Prompt: orig.(step.Prompt), reason: "nothing reaches this node"})
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, []string{sample("dbt-compile", "declined")}, m.gotTries())
	})

	t.Run("a cross-project cluster fell back because a step declined", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withTries(1))
		id := h.accept(trigger("compile", crossProjectNode()))
		require.NoError(t, h.runner.Drive(ctx, id))

		require.Equal(t, remediation.ClusterFellBack, h.cluster(id, 1, 1).Status)
		assert.Equal(t, []string{sample("cross-project-upstream", "declined")}, m.gotTries(),
			"its members get no cluster of their own, so it counts its own failure")
		assert.Equal(t, []string{sample("cross-project-upstream", "handed_over", "declined")}, m.gotOutcomes())
	})

	t.Run("a cross-project cluster fell back because its pre-flight rounds were spent", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withTries(1))
		h.bundle.nodes = map[string]ports.CandidateSource{"model.core.base": {RawCode: "select 1", Runtime: "dbt"}}
		h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
			r := fixAnswer(req)
			r.ProposedSQL, r.Confidence = "select 3", "low"
			return r
		}
		trig := trigger("compile", crossProjectNode())
		trig.CodeBundleURI = "s3://bundles/rel-1.tar.gz"
		id := h.accept(trig)
		require.NoError(t, h.runner.Drive(ctx, id))

		require.Equal(t, remediation.ClusterFellBack, h.cluster(id, 1, 1).Status)
		assert.Equal(t, []string{sample("cross-project-upstream", "preflight_spent")}, m.gotTries())
		assert.Equal(t, []string{sample("cross-project-upstream", "handed_over", "preflight_rounds_spent")}, m.gotOutcomes())
	})

	t.Run("a draft made no edit", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withTries(1), withPreflight("dbt-compile", "confidence_not_low"))
		h.llm.answer = noEditAnswer
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, []string{sample("dbt-compile", "declined")}, m.gotTries())
	})
}

// A cluster no type matches, a cluster handed over when the try opened and a
// cluster that fell back count nothing on their own: the members of a fallen
// back cluster count their own result, and the next try counts a settled node
// once, as the cluster it was grouped into when it was settled, not again.
func TestRunner_CountsNothingForAClusterNoTypeMatchedOrCarriedOver(t *testing.T) {
	t.Run("no type matches", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		test := remediation.TriggerNode{NodeID: "test.sales.not_null", ErrorSignature: "sig-t", NodeType: "dbt-test", Project: "svc-a"}
		python := remediation.TriggerNode{NodeID: "python.sales.loader", ErrorSignature: "sig-p", NodeType: "python-node", Project: "svc-a"}
		id := h.accept(trigger("validation", test, python))
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Empty(t, m.gotTries())
	})

	t.Run("a shared-upstream cluster fell back to its members", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withTries(1))
		ancestor := remediation.ChangedAncestor{NodeID: "model.core.base", FilePath: "models/base.sql", Project: "core", Depth: 1}
		a, b := compileNode("svc-a"), compileNode("svc-b")
		a.ErrorSignature, b.ErrorSignature = "sig-shared", "sig-shared"
		a.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
		b.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
		id := h.accept(trigger("compile", a, b))
		require.NoError(t, h.runner.Drive(ctx, id))
		h.failGate(id, "svc-a")

		assert.ElementsMatch(t, []string{
			sample("dbt-compile", "gate_failed"),
			sample("dbt-compile", "gate_failed"),
		}, m.gotTries(), "the members count their own result, the fallen back cluster none")
	})

	t.Run("a settled node is carried over", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withPreflight("dbt-compile", "confidence_not_low"))
		h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
			if strings.Contains(req.User, "services/svc-a/") {
				return noEditAnswer(req)
			}
			return fixAnswer(req)
		}
		id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))
		require.NoError(t, h.runner.Drive(ctx, id))
		h.failGate(id, "svc-b")
		require.NoError(t, h.drive(id))
		assert.Empty(t, m.gotOutcomes(), "the first try is not the end")
		h.pipeline.answer(proposal.PhasePassed, nil)
		require.NoError(t, h.poll(id))

		assert.ElementsMatch(t, []string{
			sample("dbt-compile", "declined"),    // svc-a, in the first try
			sample("dbt-compile", "gate_failed"), // svc-b, in the first try
			sample("dbt-compile", "passed"),      // svc-b, in the second try
		}, m.gotTries(), "the second try counts svc-b alone")
		assert.ElementsMatch(t, []string{
			sample("dbt-compile", "handed_over", "declined"),
			sample("dbt-compile", "fixed", ""),
		}, m.gotOutcomes(), "the node declined in the first try is counted, at the end of the second, under the type that declined it")
	})

	t.Run("a node the classifier marked unfixable is carried over", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		id := h.accept(trigger("compile", unfixable(compileNode("svc-a"), "classifier_infra", "the warehouse dropped"), compileNode("svc-b")))
		require.NoError(t, h.runner.Drive(ctx, id))
		h.pipeline.answer(proposal.PhasePassed, nil)
		require.NoError(t, h.poll(id))

		assert.Equal(t, []string{sample("dbt-compile", "passed")}, m.gotTries(), "the unfixable node's cluster ran no step")
		assert.ElementsMatch(t, []string{
			sample("none", "handed_over", "classifier_infra"),
			sample("dbt-compile", "fixed", ""),
		}, m.gotOutcomes(), "no type ever worked an unfixable node")
	})
}

// Every pre-flight pass counts once, with how it ended.
func TestRunner_CountsEachPreflightPass(t *testing.T) {
	t.Run("a refused draft is revised and passes", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withRounds(3))
		answersInTurn(h, lowConfidence, fixAnswer)
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, []string{sample("dbt-compile", "revised"), sample("dbt-compile", "passed")}, m.gotRounds())
	})

	t.Run("every round is refused", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withRounds(3))
		h.llm.answer = lowConfidence
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, []string{
			sample("dbt-compile", "revised"), sample("dbt-compile", "revised"), sample("dbt-compile", "spent"),
		}, m.gotRounds())
	})

	t.Run("a pass whose commit fails counts nothing until the pass commits", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		id := h.accept(trigger("compile", compileNode("svc-a")))
		// The pre-flight pass's commit is the one after the prompt's: arm its
		// failure once the prompt step has committed.
		var armed bool
		h.afterCommit = func() {
			if armed {
				return
			}
			for _, row := range h.store.Steps(id) {
				if row.Kind == "prompt" && row.Status == remediation.StepDone {
					armed = true
					h.store.FailNextCommit()
				}
			}
		}
		require.Error(t, h.runner.Drive(ctx, id), "the pre-flight pass's commit fails")
		assert.Empty(t, m.gotRounds(), "the rolled-back pass counts nothing")

		h.afterCommit = nil
		require.NoError(t, h.runner.Drive(ctx, id))
		assert.Equal(t, []string{sample("dbt-compile", "passed")}, m.gotRounds(), "the pass that committed counts once")
	})
}

// A node counts once, when its remediation ends: fixed when a verified fix was
// handed over, handed over with the code it carries otherwise, under the type
// of its cluster, or none when no type matched it.
func TestRunner_CountsEachNodeWhenItsRemediationEnds(t *testing.T) {
	t.Run("a fixed node and a node no type matches", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		test := compileNode("test.sales.not_null")
		test.NodeType = "dbt-test"
		id := h.accept(trigger("compile", compileNode("svc-a"), test))
		require.NoError(t, h.runner.Drive(ctx, id))
		assert.Empty(t, m.gotOutcomes(), "a remediation that waits on its verification has not ended")
		h.pipeline.answer(proposal.PhasePassed, nil)
		require.NoError(t, h.poll(id))

		assert.ElementsMatch(t, []string{
			sample("dbt-compile", "fixed", ""),
			sample("none", "handed_over", "no_type"),
		}, m.gotOutcomes())
	})

	t.Run("tries spent counts each node once, at the last try", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		h.pipeline.answer(proposal.PhaseFailed, map[string]string{"svc-a": "still broken"})
		id := h.accept(trigger("compile", compileNode("svc-a")))
		for try := 1; try <= 3; try++ {
			require.NoError(t, h.drive(id))
			assert.Empty(t, m.gotOutcomes(), "try %d is not the end", try)
			require.NoError(t, h.poll(id))
		}

		assert.Equal(t, []string{sample("dbt-compile", "handed_over", "tries_spent")}, m.gotOutcomes())
		require.NoError(t, h.drive(id))
		assert.Len(t, m.gotOutcomes(), 1, "a remediation that ended counts nothing more")
	})

	t.Run("a try that settles with nothing to verify", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		orig, _, ok := h.registry().Lookup("dbt-compile")
		require.True(t, ok)
		h.replaceStep(decliningPrompt{Prompt: orig.(step.Prompt), reason: "nothing reaches this node"})
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, []string{sample("dbt-compile", "handed_over", "declined")}, m.gotOutcomes())
		assert.Equal(t, []string{sample("dbt-compile", "declined")}, m.gotTries())
	})

	t.Run("a node the classifier marked unfixable and one with no repository", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		trig := trigger("compile", unfixable(compileNode("svc-a"), "classifier_infra", "the warehouse dropped"), compileNode("svc-b"))
		trig.Repo = ""
		id := h.accept(trig)
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.ElementsMatch(t, []string{
			sample("none", "handed_over", "classifier_infra"),
			sample("none", "handed_over", "no_repository"),
		}, m.gotOutcomes())
	})

	t.Run("maintenance mode counts only the nodes it hands over", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m), withMaintenance())
		id := h.accept(trigger("compile", unfixable(compileNode("svc-a"), "classifier_infra", "the warehouse dropped"), compileNode("svc-b")))
		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, []string{sample("none", "handed_over", "classifier_infra")}, m.gotOutcomes(),
			"svc-b is skipped to be retried, which is neither a fix nor a hand-over")
	})
}

// markProposedByHand moves the verifying attempt row id to proposed, as a
// repository call made outside the runner would.
func markProposedByHand(h *harness, id string) error {
	checked, err := remediation.NewChecked(remediation.Draft{}, nil)
	if err != nil {
		return err
	}
	verified, err := remediation.NewVerified(id, []remediation.Checked{checked}, nil, nil)
	if err != nil {
		return err
	}
	_, err = h.store.ProposalRepository().MarkFixVerified(ctx, verified)
	return err
}

// A remediation whose attempt row someone else moved while its try was
// verifying still ends, and counts each node from the row as it then stands:
// fixed for a node the row holds proposed, handed over for a node whose outcome
// carries a code, and nothing for the others.
func TestRunner_CountsTheNodesOfARemediationWhoseAttemptRowMoved(t *testing.T) {
	test := compileNode("test.sales.not_null")
	test.NodeType = "dbt-test"
	for name, tc := range map[string]struct {
		move  func(h *harness, proposalID string) error
		phase proposal.Phase
		want  []string
	}{
		"the gate passed, the row was proposed by hand": {
			move: func(h *harness, id string) error {
				return markProposedByHand(h, id)
			},
			phase: proposal.PhasePassed,
			want:  []string{sample("dbt-compile", "fixed", ""), sample("none", "handed_over", "no_type")},
		},
		"the gate failed, the row was proposed by hand": {
			move: func(h *harness, id string) error {
				return markProposedByHand(h, id)
			},
			phase: proposal.PhaseFailed,
			want:  []string{sample("dbt-compile", "fixed", ""), sample("none", "handed_over", "no_type")},
		},
		"the gate passed, the row was failed by hand": {
			move: func(h *harness, id string) error {
				_, err := h.store.ProposalRepository().MarkVerifyFailed(ctx, id, "failed by hand", nil)
				return err
			},
			phase: proposal.PhasePassed,
			want:  []string{sample("none", "handed_over", "no_type")},
		},
		"the gate failed, the row was failed by hand": {
			move: func(h *harness, id string) error {
				_, err := h.store.ProposalRepository().MarkVerifyFailed(ctx, id, "failed by hand", nil)
				return err
			},
			phase: proposal.PhaseFailed,
			want:  []string{sample("none", "handed_over", "no_type")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := &recordedMetrics{}
			h := newHarness(t, withMetrics(m))
			id := h.accept(trigger("compile", compileNode("svc-a"), test))
			require.NoError(t, h.runner.Drive(ctx, id))
			require.NoError(t, tc.move(h, h.attempt(1).ID))
			h.pipeline.answer(tc.phase, map[string]string{"svc-a": "broken"})

			require.NoError(t, h.poll(id))

			require.Equal(t, remediation.StatusDone, h.remediation(id).Status)
			assert.ElementsMatch(t, tc.want, m.gotOutcomes())
		})
	}
}

// The flow's own context steps run once per cluster, and count under the
// cluster's type like its type's context steps: only the cross-cluster checks,
// the gate and the outputs have no type.
func TestRunner_CountsTheFlowsContextStepsUnderTheClusterType(t *testing.T) {
	m := &recordedMetrics{}
	h := newHarness(t, withMetrics(m), withPreviousTries())
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, 4, countOf(m.gotStages(), sample("dbt-compile", "context")),
		"previous_tries, then the type's three context steps")
	assert.Zero(t, countOf(m.gotStages(), sample("", "context")))
}

// Stage times each committed step, a cluster's steps under its type and the
// steps of no cluster with no type.
func TestRunner_CountsEachCommittedStepWithItsDuration(t *testing.T) {
	m := &recordedMetrics{}
	h := newHarness(t, withMetrics(m))
	h.llm.onCall = func(int) { h.clock.set(h.clock.Now().Add(7 * time.Second)) }
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.ElementsMatch(t, []string{
		sample("dbt-compile", "context"), sample("dbt-compile", "context"), sample("dbt-compile", "context"),
		sample("dbt-compile", "propose"),
		sample("dbt-compile", "preflight"),
		sample("", "cross_cluster"), sample("", "cross_cluster"), sample("", "cross_cluster"),
	}, m.gotStages(), "the gate that waits on its runs has no time yet")
	assert.Equal(t, []time.Duration{7 * time.Second}, m.durations(sample("dbt-compile", "propose")))

	h.clock.set(h.clock.Now().Add(10 * time.Minute))
	h.pipeline.answer(proposal.PhasePassed, nil)
	require.NoError(t, h.poll(id))

	assert.Equal(t, []time.Duration{10 * time.Minute}, m.durations(sample("", "gate")), "from the submit to the verdict")
	assert.Len(t, m.durations(sample("", "output")), 1)
}

// A step whose commit rolled back counts nothing, and counts once when the
// step runs again and commits.
func TestRunner_CountsNothingForAStepWhoseCommitRolledBack(t *testing.T) {
	m := &recordedMetrics{}
	h := newHarness(t, withMetrics(m))
	h.llm.onCall = func(n int) {
		if n == 1 {
			h.store.FailNextCommit()
		}
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.Error(t, h.runner.Drive(ctx, id), "the prompt step's commit fails")
	assert.NotContains(t, m.gotStages(), sample("dbt-compile", "propose"), "the rolled-back step counts nothing")

	require.NoError(t, h.runner.Drive(ctx, id))
	assert.Equal(t, 1, countOf(m.gotStages(), sample("dbt-compile", "propose")), "the step that committed counts once")
}

func countOf(list []string, want string) int {
	n := 0
	for _, s := range list {
		if s == want {
			n++
		}
	}
	return n
}

// The model's tokens count at each answered call, and never for an answer the
// response cache replays.
func TestRunner_CountsNoTokensOnACacheHit(t *testing.T) {
	m := &recordedMetrics{}
	h := newHarness(t, withMetrics(m))
	h.llm.onCall = func(n int) {
		if n == 1 {
			h.store.FailNextCommit()
		}
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.Error(t, h.runner.Drive(ctx, id), "the prompt step's commit fails after its model call")
	assert.ElementsMatch(t, []string{
		sample("dbt-compile", "in", "100"),
		sample("dbt-compile", "out", "20"),
	}, m.gotTokens(), "the call used its tokens, whatever happened to the commit")

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, 1, h.llm.calls(), "the second drive is answered by the cache")
	assert.Len(t, m.gotTokens(), 2, "an answer the cache replays uses none")
}

// A revision round is a call of its own, with tokens of its own.
func TestRunner_CountsTheTokensOfEachRound(t *testing.T) {
	m := &recordedMetrics{}
	h := newHarness(t, withMetrics(m), withRounds(3))
	answersInTurn(h, lowConfidence, fixAnswer)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))

	assert.ElementsMatch(t, []string{
		sample("dbt-compile", "in", "100"), sample("dbt-compile", "out", "20"),
		sample("dbt-compile", "in", "100"), sample("dbt-compile", "out", "20"),
	}, m.gotTokens())
}

// A remediation the runner stops counts one system failure, by the kind of
// failure, whoever ends it: the runner before a step, the sweeper and the
// worker. It counts no try and no node, which count when the remediation ends
// for good.
func TestRunner_CountsEachSystemFailureOnce(t *testing.T) {
	t.Run("the runner ends a try past its deadline", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		id := h.accept(trigger("compile", compileNode("svc-a")))
		var once sync.Once
		h.source.onRead = func() { once.Do(func() { h.passTime(id, draftingTime+time.Second) }) }
		require.NoError(t, h.runner.Drive(ctx, id))
		h.requireEndedAsStuck(id, "try 1 was still drafting at its deadline", remediation.TryDrafting)

		assert.Equal(t, []string{sample("deadline")}, m.gotFailures())

		require.NoError(t, h.sweeper().Sweep(ctx))
		require.NoError(t, h.runner.NextTry(ctx, id))
		require.NoError(t, h.drive(id))
		assert.Equal(t, []string{sample("deadline")}, m.gotFailures(), "a remediation that stopped is not stopped again")
		assert.Empty(t, m.gotTries(), "a paused try counts nothing")
		assert.Empty(t, m.gotOutcomes(), "a paused remediation has not ended")
	})

	t.Run("the runner ends a remediation that failed five times before a step", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		id := h.accept(trigger("compile", compileNode("svc-a")))
		h.countRemediationFailures(id, 5)
		require.NoError(t, h.drive(id))

		assert.Equal(t, []string{sample("remediation_failures")}, m.gotFailures())
	})

	t.Run("the sweeper ends a step that failed five times", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		id := h.drafting()
		h.stepFailing(id, "dbt_log", remediation.StepFailed, 5)
		require.NoError(t, h.sweeper().Sweep(ctx))
		require.NoError(t, h.sweeper().Sweep(ctx))

		assert.Equal(t, []string{sample("step_failures")}, m.gotFailures())
	})

	t.Run("the worker ends a remediation whose drive failed", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		h.acceptUnclaimed(trigger("compile", compileNode("svc-a")))
		id := h.store.Remediations()[0].ID
		h.countRemediationFailures(id, 4)

		w := runner.NewWorker(h.runnerOwnedBy(h.withoutTheFlow(), "worker-a"), h.store.RemediationRepository, "worker-a", h.clock, discard)
		runWorker(t, w)
		w.Wake()
		require.Eventually(t, func() bool { return h.remediation(id).Status == remediation.StatusSystemFailure }, 10*time.Second, 5*time.Millisecond)
		w.Wake()
		time.Sleep(50 * time.Millisecond)

		assert.Equal(t, []string{sample("remediation_failures")}, m.gotFailures())
	})

	t.Run("a redriven remediation counts its end once, and its pause once", func(t *testing.T) {
		m := &recordedMetrics{}
		h := newHarness(t, withMetrics(m))
		id := h.accept(trigger("compile", compileNode("svc-a")))
		h.failGateSubmits(id, 5)
		require.NoError(t, h.drive(id), "the sixth drive ends the remediation")
		require.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
		dls := h.deadLetters()
		require.Len(t, dls, 1)
		assert.Equal(t, []string{sample("step_failures")}, m.gotFailures())
		assert.Empty(t, m.gotTries())
		assert.Empty(t, m.gotOutcomes())

		h.clock.set(t0.Add(time.Hour))
		h.redrive(dls[0], "2-0")
		require.NoError(t, h.drive(id))
		h.pipeline.answer(proposal.PhasePassed, nil)
		require.NoError(t, h.poll(id))

		assert.Equal(t, []string{sample("step_failures")}, m.gotFailures())
		assert.Equal(t, []string{sample("dbt-compile", "passed")}, m.gotTries())
		assert.Equal(t, []string{sample("dbt-compile", "fixed", "")}, m.gotOutcomes())
	})
}

// A flow and a type label come from the loaded files alone: a node no type
// matched is none, and a step of the flow itself has no type.
func TestRemediationMetrics_TypeLabelComesFromTheLoadedFiles(t *testing.T) {
	reg := pkgmetrics.New("agent-remediation")
	h := newHarness(t, withMetrics(rametrics.NewRemediationMetrics(reg)))
	test := compileNode("test.sales.not_null")
	test.NodeType = "dbt-test"
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b"), test))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.failGate(id, "svc-a")
	require.NoError(t, h.drive(id))
	h.pipeline.answer(proposal.PhasePassed, nil)
	require.NoError(t, h.poll(id))

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	allowedTypes := map[string]bool{"none": true, "": true}
	for name := range h.files.Types {
		allowedTypes[name] = true
	}
	flowLabel, typeLabel := regexp.MustCompile(`flow="([^"]*)"`), regexp.MustCompile(`type="([^"]*)"`)
	seen := map[string]bool{}
	for line := range strings.SplitSeq(body, "\n") {
		if !strings.HasPrefix(line, "continuo_remediation_") {
			continue
		}
		if m := flowLabel.FindStringSubmatch(line); m != nil {
			assert.Contains(t, h.files.Flows, m[1], line)
		}
		if m := typeLabel.FindStringSubmatch(line); m != nil {
			assert.True(t, allowedTypes[m[1]], "type %q is not the name of a loaded type: %s", m[1], line)
			seen[m[1]] = true
		}
	}
	assert.True(t, seen["dbt-compile"], "a cluster's steps are under its type")
	assert.True(t, seen["none"], "a node no type matched is none")
	assert.True(t, seen[""], "a step of the flow itself has no type")
	assert.Contains(t, body, `continuo_remediation_outcomes_total{flow="rejected-release",outcome="handed_over",reason="no_type",service="agent-remediation",type="none"} 1`)
	assert.Contains(t, body, `continuo_remediation_outcomes_total{flow="rejected-release",outcome="fixed",reason="",service="agent-remediation",type="dbt-compile"} 2`)
}
