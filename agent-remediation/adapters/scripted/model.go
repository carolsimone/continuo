package scripted

import (
	"context"
	"fmt"
	"sync"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
)

// model is the scripted model. A call about a cluster whose target has answers
// of its own takes the next of them, and any other call takes the next of the
// default answers. Each answer is given once: a call past the last answer of
// its list is refused, so a run that calls the model more often than its
// scenario scripted fails instead of being answered again. The target is the
// node the prompt names on the call's context. Every answered call is
// recorded with its target.
type model struct {
	mu       sync.Mutex
	answers  []ports.ProposeResult
	byTarget map[string][]ports.ProposeResult
	given    map[string]int
	calls    []conformance.ModelCall
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
	n := m.given[key]
	if n >= len(list) {
		m.mu.Unlock()
		return ports.ProposeResult{}, fmt.Errorf("the script holds %d model answers for calls about %q, and this is call %d", len(list), target, n+1)
	}
	res := list[n]
	m.given[key]++
	res.Files = append([]ports.ProposedFile(nil), res.Files...)
	m.calls = append(m.calls, conformance.ModelCall{Target: target, Request: req})
	calls, onCall := len(m.calls), m.onCall
	m.mu.Unlock()
	if onCall != nil {
		onCall(calls)
	}
	return res, nil
}

func (m *model) setOnCall(fn func(calls int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onCall = fn
}

func (m *model) recorded() []conformance.ModelCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]conformance.ModelCall(nil), m.calls...)
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

// releaseChecks answers the release checks. A check of one of the first failed
// tries fails with the fixture's gate errors and every other check passes,
// both as soon as they are read.
type releaseChecks struct {
	mu        sync.Mutex
	failed    int
	errors    map[string]string
	clock     ports.Clock
	submitted []ports.ReleaseCheckRequest
}

var _ ports.ReleaseChecks = (*releaseChecks)(nil)

func newReleaseChecks(failedTries int, gateErrors map[string]string, clock ports.Clock) *releaseChecks {
	return &releaseChecks{failed: failedTries, errors: gateErrors, clock: clock}
}

// Submit records the check.
func (p *releaseChecks) Submit(_ context.Context, r ports.ReleaseCheckRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.submitted = append(p.submitted, r)
	return nil
}

// Status returns the check's verdict.
func (p *releaseChecks) Status(_ context.Context, checkID string) (ports.ReleaseCheckStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.submitted {
		if r.CheckID != checkID {
			continue
		}
		if r.Try <= p.failed {
			nodeErrors := make(map[string]string, len(p.errors))
			for node, msg := range p.errors {
				nodeErrors[node] = msg
			}
			return ports.ReleaseCheckStatus{Phase: fix.PhaseFailed, ActivatedAt: p.clock.Now(), NodeErrors: nodeErrors}, nil
		}
		return ports.ReleaseCheckStatus{Phase: fix.PhasePassed, ActivatedAt: p.clock.Now()}, nil
	}
	return ports.ReleaseCheckStatus{}, fmt.Errorf("no release check %s was submitted", checkID)
}

func (p *releaseChecks) requests() []ports.ReleaseCheckRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ports.ReleaseCheckRequest(nil), p.submitted...)
}
