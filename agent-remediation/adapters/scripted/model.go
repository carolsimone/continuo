package scripted

import (
	"context"
	"fmt"
	"sync"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
)

// model is the scripted model. A call about a cluster whose target has answers
// of its own takes the next of them, and any other call takes the next of the
// default answers; a list starts over once every answer in it was given. The
// target is the node the prompt names on the call's context. Every call is
// recorded.
type model struct {
	mu       sync.Mutex
	answers  []ports.ProposeResult
	byTarget map[string][]ports.ProposeResult
	given    map[string]int
	calls    []prompt.ProposeRequest
	onCall   func(calls int)
}

var _ ports.LLMProvider = (*model)(nil)

func newModel(answers []ports.ProposeResult, byTarget map[string][]ports.ProposeResult) *model {
	return &model{answers: answers, byTarget: byTarget, given: map[string]int{}}
}

// Propose answers req from the script.
func (m *model) Propose(ctx context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	target := ""
	if f, ok := promptlog.FailureFromContext(ctx); ok {
		target = f.NodeID
	}
	m.mu.Lock()
	list, key := m.answers, ""
	if own, ok := m.byTarget[target]; ok {
		list, key = own, target
	}
	if len(list) == 0 {
		m.mu.Unlock()
		return ports.ProposeResult{}, fmt.Errorf("the script holds no model answer for a call about %q", target)
	}
	res := list[m.given[key]%len(list)]
	m.given[key]++
	res.Files = append([]ports.ProposedFile(nil), res.Files...)
	m.calls = append(m.calls, req)
	n, onCall := len(m.calls), m.onCall
	m.mu.Unlock()
	if onCall != nil {
		onCall(n)
	}
	return res, nil
}

func (m *model) setOnCall(fn func(calls int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onCall = fn
}

func (m *model) requests() []prompt.ProposeRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]prompt.ProposeRequest(nil), m.calls...)
}

// responseCache is the model's response cache, in memory.
type responseCache struct {
	mu      sync.Mutex
	answers map[string]ports.ProposeResult
}

var _ ports.LLMResponseCache = (*responseCache)(nil)

func newResponseCache() *responseCache {
	return &responseCache{answers: map[string]ports.ProposeResult{}}
}

// Get returns the answer cached under key.
func (c *responseCache) Get(_ context.Context, key string) (ports.ProposeResult, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.answers[key]
	return r, ok, nil
}

// Put caches result under key.
func (c *responseCache) Put(_ context.Context, key string, result ports.ProposeResult) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.answers[key] = result
	return nil
}

// pipeline is the verification pipeline. A run of one of the first failed
// attempts fails with the fixture's gate errors and every other run passes,
// both as soon as they are read.
type pipeline struct {
	mu        sync.Mutex
	failed    int
	errors    map[string]string
	clock     ports.Clock
	submitted []ports.VerificationRequest
}

var _ ports.VerificationPipeline = (*pipeline)(nil)

func newPipeline(failedAttempts int, gateErrors map[string]string, clock ports.Clock) *pipeline {
	return &pipeline{failed: failedAttempts, errors: gateErrors, clock: clock}
}

// Submit records the run.
func (p *pipeline) Submit(_ context.Context, r ports.VerificationRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.submitted = append(p.submitted, r)
	return nil
}

// Status returns the run's verdict.
func (p *pipeline) Status(_ context.Context, runID string) (ports.VerificationStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.submitted {
		if r.RunID != runID {
			continue
		}
		if r.Attempt <= p.failed {
			nodeErrors := make(map[string]string, len(p.errors))
			for node, msg := range p.errors {
				nodeErrors[node] = msg
			}
			return ports.VerificationStatus{Phase: proposal.PhaseFailed, ActivatedAt: p.clock.Now(), NodeErrors: nodeErrors}, nil
		}
		return ports.VerificationStatus{Phase: proposal.PhasePassed, ActivatedAt: p.clock.Now()}, nil
	}
	return ports.VerificationStatus{}, fmt.Errorf("no verification run %s was submitted", runID)
}

func (p *pipeline) runs() []ports.VerificationRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ports.VerificationRequest(nil), p.submitted...)
}
