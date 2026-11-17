package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

const (
	contractNodeID = "sales.loader"
	contractYAML   = "services/svc-a/contracts/loader.odcs.yaml"
	brokenContract = "apiVersion: v3.2.0\nkind: DataContract\nid: sales.loader\ncustomProperties:\n" +
		"  - {vendor: continuo, property: script, value: scripts/loader.py}\n" +
		"  - {vendor: continuo, property: readQueries, value: {src: select id from public.wrong_name}}\n" +
		"schema:\n  - name: loader\n    physicalType: table\n"
	fixedContract = "apiVersion: v3.2.0\nkind: DataContract\nid: sales.loader\ncustomProperties:\n" +
		"  - {vendor: continuo, property: script, value: scripts/loader.py}\n" +
		"  - {vendor: continuo, property: readQueries, value: {src: select id from public.right_name}}\n" +
		"schema:\n  - name: loader\n    physicalType: table\n"
	toolRefusal = "read src: relation public.wrong_name is not declared"
)

// checkoutOf is a repository checkout that holds files, which every fetch
// writes into a directory of its own.
type checkoutOf struct {
	dir   string
	files map[string]string
	mu    sync.Mutex
	n     int
}

var _ ports.RepoArchive = (*checkoutOf)(nil)

func (c *checkoutOf) Fetch(context.Context, string, string) (string, func(), error) {
	c.mu.Lock()
	c.n++
	root := filepath.Join(c.dir, fmt.Sprintf("checkout-%d", c.n))
	c.mu.Unlock()
	for name, body := range c.files {
		full := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			return "", nil, err
		}
		if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
			return "", nil, err
		}
	}
	return root, func() { _ = os.RemoveAll(root) }, nil
}

// refusingPackager is the packaging tool, which refuses the first contracts it
// is given with refusal and accepts every later one.
type refusingPackager struct {
	mu       sync.Mutex
	refuse   int
	refusal  string
	calls    int
	packages []map[string]string
}

var _ ports.ContractPackager = (*refusingPackager)(nil)

func (p *refusingPackager) Pack(_ context.Context, _, _, _, _ string, packages map[string]string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.packages = append(p.packages, packages)
	if p.calls <= p.refuse {
		return nil, fmt.Errorf("%w: %s", ports.ErrContractRejected, p.refusal)
	}
	return []byte(`{"bundle_version": 1}`), nil
}

// withContractType gives the harness a python-validation type of rounds
// pre-flight rounds whose one pre-flight check is the packaging tool, over a
// checkout that holds one contract yaml declaring sales.loader, and the
// packager the type's check calls.
func withContractType(rounds int, packager ports.ContractPackager) option {
	return func(h *harness) {
		h.archive = &checkoutOf{dir: h.t.TempDir(), files: map[string]string{contractYAML: brokenContract}}
		h.packager = packager
		flow := h.files.Flows["rejected-release"]
		flow.Types = append(flow.Types, "python-validation")
		h.files.Flows[flow.Name] = flow
		h.files.Types["python-validation"] = workflow.Type{
			Version: workflow.SupportedVersion,
			Name:    "python-validation",
			Match:   workflow.Match{Cluster: workflow.ClusterSingle, Source: "validation", NodeTypes: []string{"python-node"}},
			Context: refs("contract_file", "failing_siblings", "image_packages", "dbt_log", "contract_entry", "current_contract", "upstream_changes", "failure_precedents"),
			Agent: workflow.Agent{
				Prompt: workflow.StepRef{Name: "python-contract"}, Tools: []workflow.StepRef{}, Rounds: rounds,
			},
			Preflight: refs("contract_packages"),
			Revision:  "rev-python-validation",
			File:      "types/python-validation.yaml",
		}
	}
}

func contractNode() remediation.TriggerNode {
	return remediation.TriggerNode{
		NodeID:         contractNodeID,
		ErrorSignature: "sig-loader",
		Category:       "validation",
		Reason:         "relation_not_found",
		ErrorExcerpt:   "relation public.wrong_name does not exist",
		Service:        "svc-a",
		NodeType:       "python-node",
	}
}

// contractAnswer is the model's answer to a python contract request: the
// contract yaml with the read corrected.
func contractAnswer(prompt.ProposeRequest) ports.ProposeResult {
	return ports.ProposeResult{
		Files:      []ports.ProposedFile{{Path: contractYAML, Content: fixedContract}},
		Rationale:  "the read names a relation that exists",
		Confidence: "high",
		Model:      "claude-test",
	}
}

// The tool that packages a contract refuses what the model answered, and its
// complaint goes back to the model as a finding of the same try's next round.
func TestContractPackages_ARefusalGoesBackToTheModel(t *testing.T) {
	packager := &refusingPackager{refuse: 1, refusal: toolRefusal}
	h := newHarness(t, withContractType(2, packager))
	h.llm.answer = contractAnswer
	id := h.accept(trigger("validation", contractNode()))

	require.NoError(t, h.runner.Drive(ctx, id))

	passes := h.roundRows(id, 1, "preflight")
	require.Len(t, passes, 2)
	require.Len(t, passes[1].Findings, 1, "the tool refused round 1's draft")
	assert.Equal(t, "contract_rejected", passes[1].Findings[0].Code)
	assert.Contains(t, passes[1].Findings[0].Message, toolRefusal, "the finding keeps the tool's own complaint")
	assert.Empty(t, passes[2].Findings, "the tool accepted the revised draft")

	reqs := h.llm.all()
	require.Len(t, reqs, 2, "the refused draft goes back to the model once")
	want := prompt.Revise(reqs[0], step.AnswerJSON(contractAnswer(reqs[0])), step.FindingLines(passes[1].Findings))
	assert.Equal(t, want, reqs[1], "round 2 revises round 1's request with its answer and the tool's refusal")
	assert.Contains(t, reqs[1].User, toolRefusal, "round 2's request carries the tool's message")
	assert.Contains(t, reqs[1].User, "- "+step.FindingLines(passes[1].Findings)[0]+"\n", "as a finding line")

	assert.Equal(t, 2, packager.calls)
	assert.Equal(t, []map[string]string{harnessPackages, harnessPackages}, packager.packages, "each round packs with the release's package list")
	assert.Equal(t, 1, h.images.count(), "the package list is read once, by the context step, never again per round")
	require.Len(t, h.store.Tries(id), 1, "the revision stays in the same try")
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterChecked, c.Status)
	assert.Equal(t, 2, c.Round)
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status, "the revised draft goes on to the gate")
}

// With no round left, the tool's refusal ends the cluster as any other
// pre-flight finding does.
func TestContractPackages_ARefusalInTheLastRoundHandsTheClusterOver(t *testing.T) {
	packager := &refusingPackager{refuse: 100, refusal: toolRefusal}
	h := newHarness(t, withContractType(2, packager))
	h.llm.answer = contractAnswer
	id := h.accept(trigger("validation", contractNode()))

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, 2, h.llm.calls(), "one model call per round")
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterHandedOver, c.Status)
	assert.Equal(t, 2, c.Round)
	require.NotNil(t, c.Outcome)
	assert.Equal(t, proposal.StatusFailed, c.Outcome.Status)
	assert.Contains(t, c.Outcome.Reason, "pre-flight rounds spent: ")
	assert.Contains(t, c.Outcome.Reason, toolRefusal)
	assert.Empty(t, h.pipeline.runs())
}

// A release that records no package list of the service's image is known
// before any draft: the image_packages context step declines the cluster with
// the reason, no model is called and nothing is packed.
func TestImagePackages_AReleaseWithNoListDeclinesBeforeTheModel(t *testing.T) {
	packager := &refusingPackager{}
	h := newHarness(t, withContractType(2, packager))
	noPackages := fmt.Errorf("release rel-1, service svc-a: %w (the release has no contract bundle)", ports.ErrNoImagePackages)
	h.images.err = noPackages
	h.llm.answer = contractAnswer
	id := h.accept(trigger("validation", contractNode()))

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Zero(t, h.llm.calls())
	assert.Zero(t, packager.calls)
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterHandedOver, c.Status)
	require.NotNil(t, c.Outcome)
	assert.Equal(t, proposal.StatusSkipped, c.Outcome.Status)
	assert.Equal(t, noPackages.Error(), c.Outcome.Reason)
	assert.Empty(t, h.pipeline.runs())
}

// A read of the package list a retry may clear is the context step's error:
// the runner records a failure and the step runs again, never a decline.
func TestImagePackages_AReadErrorIsRetriedNotDeclined(t *testing.T) {
	h := newHarness(t, withContractType(2, &refusingPackager{}))
	h.images.err = errors.New("release-controller unavailable")
	h.llm.answer = contractAnswer
	id := h.accept(trigger("validation", contractNode()))

	require.Error(t, h.runner.Drive(ctx, id))
	assert.Equal(t, 1, h.images.count())
	assert.Zero(t, h.llm.calls())
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterGathering, c.Status, "the cluster still gathers its context")
	assert.Nil(t, c.Outcome)

	h.images.err = nil
	require.NoError(t, h.runner.Drive(ctx, id))
	assert.Equal(t, 2, h.images.count(), "the failed step runs again")
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
}

// A checked cluster whose contract_packages row holds bytes that are not an
// ODCS producer bundle fails its try when the try reaches the gate: the gate
// submits no run, and the next try starts or the tries being spent the failing
// set is handed over.
func TestGate_ACheckedClusterHoldingANonBundleContractFailsTheTry(t *testing.T) {
	const notABundle = "version: 1\nnodes:\n  - id: sales.loader\n    script: scripts/loader.py\n"
	h := newHarness(t, withTries(1), withContractType(1, &refusingPackager{}))
	h.llm.answer = contractAnswer
	h.pipeline.submitErr = errors.New("release-controller unavailable")
	id := h.accept(trigger("validation", contractNode()))
	require.Error(t, h.runner.Drive(ctx, id), "the gate's submit fails, so the cluster stays checked")
	require.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)

	rows := h.stepRows(id, 1, 1)["contract_packages"]
	require.Len(t, rows, 1)
	row := rows[0]
	rec, err := serialization.DecodeRecord[serialization.CheckRecord](row.Output)
	require.NoError(t, err)
	raw, err := json.Marshal(remediation.Bytes(notABundle))
	require.NoError(t, err)
	rec.Evidence[remediation.KeyPackagedContract] = raw
	row.Output, err = serialization.EncodeRecord(rec)
	require.NoError(t, err)
	h.saveStep(row)

	h.artifacts.mu.Lock()
	h.artifacts.objects = nil
	h.artifacts.mu.Unlock()
	h.clock.set(t0.Add(time.Minute))
	require.NoError(t, h.drive(id))

	assert.Empty(t, h.pipeline.runs(), "nothing is submitted to the release pipeline")
	_, wroteContract := h.artifacts.get("svc-a/verify-rel-1-svc-a-a1/contract.yaml")
	assert.False(t, wroteContract, "no contract.yaml is written")
	_, wroteBundle := h.artifacts.get("svc-a/verify-rel-1-svc-a-a1/odcs-bundle.json")
	assert.False(t, wroteBundle, "no bundle is written for bytes that are not one")
	const reason = "the packaged contract is not an ODCS producer bundle: service svc-a"
	p := h.attempt(1).Proposal
	assert.Equal(t, proposal.StatusFailed, p.Status)
	assert.Equal(t, reason, p.Rationale)
	assert.Equal(t, map[string]proposal.NodeOutcome{"sales.loader": {Status: proposal.StatusFailed, Reason: reason}}, p.NodeOutcomes)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status, "the only try being spent, the remediation ends")
	assert.Equal(t, proposal.StatusEscalated, h.attempt(2).Proposal.Status, "the failing set is handed over")
}
