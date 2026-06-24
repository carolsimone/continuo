package llm_test

import (
	"net/http/httptest"
	"net/http"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/adapters/llm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewProvider_OpenAICompatible_RequiresBaseURL verifies that creating an
// openai-compatible provider without a base URL fails immediately at construction
// time, rather than deferring the error to call time.
func TestNewProvider_OpenAICompatible_RequiresBaseURL(t *testing.T) {
	_, err := llm.NewProvider("openai-compatible", "key", "model", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LLM_BASE_URL")
}

// TestNewProvider_OpenAICompatible_SucceedsWithBaseURL verifies that an
// openai-compatible provider can be constructed when a base URL is provided.
func TestNewProvider_OpenAICompatible_SucceedsWithBaseURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	provider, err := llm.NewProvider("openai-compatible", "key", "model", srv.URL, srv.Client())
	require.NoError(t, err)
	assert.NotNil(t, provider)
}
