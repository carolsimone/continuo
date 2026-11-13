package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// promptVersionLen is how many hex characters of the prompt's digest name its
// version.
const promptVersionLen = 12

// model is the handle a prompt step reaches the model through. Every call
// carries the cluster's idempotency key, so a step that runs again after a lost
// commit is answered by the response cache, and the failure it was built for,
// so the logged prompt names it. It sums what the calls report for the step's
// journal row.
type model struct {
	llm     ports.LLMProvider
	key     string
	failure promptlog.Failure

	calls         int
	cached        int
	tokensIn      int
	tokensOut     int
	lastModel     string
	promptVersion string
}

var _ step.Model = (*model)(nil)

// newModel builds the handle of one prompt step run: the remediation's try,
// cluster and round name its idempotency key.
func newModel(llm ports.LLMProvider, id uuid.UUID, try, cluster, round int, failure promptlog.Failure) *model {
	return &model{
		llm:     llm,
		key:     fmt.Sprintf("remediation:%s:try:%d:cluster:%d:round:%d", id, try, cluster, round),
		failure: failure,
	}
}

// Propose calls the model and records the call: its tokens are added up, its
// model id is kept as the last one, and its request names the prompt version.
func (m *model) Propose(ctx context.Context, req prompt.ProposeRequest) (ports.ProposeResult, error) {
	ctx = llmcache.ContextWithIdempotencyKey(ctx, m.key)
	ctx = promptlog.ContextWithFailure(ctx, m.failure)
	res, err := m.llm.Propose(ctx, req)
	if err != nil {
		return ports.ProposeResult{}, err
	}
	m.calls++
	if res.Cached {
		m.cached++
	}
	m.tokensIn += res.Usage.Input
	m.tokensOut += res.Usage.Output
	if res.Model != "" {
		m.lastModel = res.Model
	}
	m.promptVersion = promptVersion(req)
	return res, nil
}

// cacheHit reports whether the step called the model and every call was
// answered by the response cache.
func (m *model) cacheHit() bool {
	return m.calls > 0 && m.cached == m.calls
}

// promptVersion is the first characters of the sha256 of a request's system
// prompt followed by its tool name: two requests built from the same prompt
// template share it.
func promptVersion(req prompt.ProposeRequest) string {
	sum := sha256.Sum256([]byte(req.System + req.ToolName))
	return hex.EncodeToString(sum[:])[:promptVersionLen]
}
