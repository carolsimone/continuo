// Package llmprovider builds the LLMProvider agent-chat calls and the HTTP
// client it calls it with.
package llmprovider

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/agent-chat/adapters/anthropic"
	adapteropenai "github.com/carolsimone/continuo/agent-chat/adapters/openai"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
)

// Public addresses of the providers, used when no base URL is configured.
const (
	AnthropicURL = "https://api.anthropic.com"
	OpenAIURL    = "https://api.openai.com"
)

// New returns the LLMProvider for provider. baseURL, when set, is the server
// root the provider's API paths are joined to, for every provider; empty means
// the provider's public address. openai-compatible has no public address and
// requires one.
func New(provider, apiKey, model, baseURL string, client *http.Client) (ports.LLMProvider, error) {
	switch provider {
	case "anthropic":
		return anthropic.NewProvider(rootOr(baseURL, AnthropicURL), apiKey, model, client), nil
	case "openai":
		return adapteropenai.NewProvider(rootOr(baseURL, OpenAIURL), apiKey, model, client), nil
	case "openai-compatible":
		if baseURL == "" {
			return nil, errors.New("openai-compatible provider requires LLM_BASE_URL")
		}
		return adapteropenai.NewProvider(baseURL, apiKey, model, client), nil
	default:
		return nil, fmt.Errorf("unsupported LLM provider %q", provider)
	}
}

// NewHTTPClient returns the client agent-chat calls its LLM provider with. It
// sends each request through the proxy HTTP_PROXY or HTTPS_PROXY names, except
// to the hosts NO_PROXY lists. No overall timeout is set, because a streamed
// reply can run for minutes; cancelling the turn's context ends a call, and the
// transport timeouts stop a dead connection from hanging.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}
}

func rootOr(baseURL, public string) string {
	if baseURL != "" {
		return baseURL
	}
	return public
}
