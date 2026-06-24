package llm

import (
	"fmt"
	"net/http"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// NewProvider returns an LLMProvider for the named provider.
// Supported values for provider: "anthropic", "openai", "openai-compatible".
// For "anthropic", the canonical base URL is https://api.anthropic.com; passing a non-empty
// baseURL overrides it (used in tests against a local httptest server).
// For "openai", the canonical base URL is https://api.openai.com; same override applies.
// For "openai-compatible", baseURL is required and used as the endpoint root.
// If hc is nil, http.DefaultClient is used.
func NewProvider(provider, apiKey, model, baseURL string, hc *http.Client) (ports.LLMProvider, error) {
	if hc == nil {
		hc = http.DefaultClient
	}
	switch provider {
	case "anthropic":
		u := "https://api.anthropic.com"
		if baseURL != "" {
			u = baseURL
		}
		return newAnthropic(u, apiKey, model, hc), nil
	case "openai":
		u := "https://api.openai.com"
		if baseURL != "" {
			u = baseURL
		}
		return newOpenAI(u, apiKey, model, hc), nil
	case "openai-compatible":
		if baseURL == "" {
			return nil, fmt.Errorf("openai-compatible provider requires LLM_BASE_URL")
		}
		return newOpenAI(baseURL, apiKey, model, hc), nil
	default:
		return nil, fmt.Errorf("unsupported LLM provider %q", provider)
	}
}
