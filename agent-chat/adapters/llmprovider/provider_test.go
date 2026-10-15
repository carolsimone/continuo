package llmprovider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_EveryAcceptedProviderBuilds(t *testing.T) {
	for _, p := range pkgconfig.AcceptedLLMProviders() {
		got, err := New(p, "k", "m", "http://llm.internal:8000", nil)
		require.NoError(t, err, p)
		require.NotNil(t, got, p)
	}
}

func TestNew_OpenAICompatibleRequiresBaseURL(t *testing.T) {
	_, err := New("openai-compatible", "k", "m", "", nil)
	require.ErrorContains(t, err, "LLM_BASE_URL")
}

func TestNew_RejectsUnknownProvider(t *testing.T) {
	_, err := New("grok", "k", "m", "", nil)
	require.ErrorContains(t, err, `unsupported LLM provider "grok"`)
}

// Every provider sends its requests to the configured root, trailing slash or
// not, at its own API path.
func TestNew_BaseURLRoutesEveryProvider(t *testing.T) {
	cases := map[string]string{
		"anthropic":         "/v1/messages",
		"openai":            "/v1/chat/completions",
		"openai-compatible": "/v1/chat/completions",
	}
	for provider, wantPath := range cases {
		t.Run(provider, func(t *testing.T) {
			var gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				http.Error(w, "stop", http.StatusTeapot)
			}))
			defer srv.Close()

			p, err := New(provider, "k", "m", srv.URL+"/", srv.Client())
			require.NoError(t, err)
			_, _ = p.StreamTurn(context.Background(), ports.TurnRequest{
				System:    "sys",
				Messages:  []domain.Message{{Role: domain.RoleUser, Content: domain.TextContent{Text: "hi"}}},
				MaxTokens: 16,
			}, func(string) {})
			assert.Equal(t, wantPath, gotPath)
		})
	}
}

func TestNewHTTPClient_UsesProxyFromEnvironment(t *testing.T) {
	tr, ok := NewHTTPClient().Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.Proxy)
	assert.Equal(t, reflect.ValueOf(http.ProxyFromEnvironment).Pointer(), reflect.ValueOf(tr.Proxy).Pointer())
}
