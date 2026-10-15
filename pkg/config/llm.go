package config

import (
	"net/url"
	"slices"
	"strings"
)

// acceptedLLMProviders are the LLM_PROVIDER values agent-chat and
// agent-remediation accept. deploy/continuo/values.schema.json enumerates the
// same values for llm.provider, and pkg/deploycheck's
// TestChartLLMProvidersMatchTheServices pins the two lists to each other.
var acceptedLLMProviders = []string{"anthropic", "openai", "openai-compatible"}

// AcceptedLLMProviders returns the LLM_PROVIDER values the agents accept, in
// the order the chart schema lists them.
func AcceptedLLMProviders() []string { return slices.Clone(acceptedLLMProviders) }

// LLMSettings names the API format an agent speaks and where it sends it.
type LLMSettings struct {
	// Provider is the API format: anthropic, openai or openai-compatible.
	Provider string
	// BaseURL is the server root the API paths (/v1/...) are joined to, with
	// any trailing slash removed. Empty means the provider's public address;
	// openai-compatible has none, so it requires one.
	BaseURL string
}

// LoadLLM reads LLM_PROVIDER and LLM_BASE_URL. It records on v an unset or
// unknown provider, an openai-compatible provider without a base URL, and a
// base URL that is not an http(s) URL with a host or whose path ends in /v1:
// the agents add /v1/... themselves, so such a root would request /v1/v1/....
func LoadLLM(v *Validator) LLMSettings {
	s := LLMSettings{
		Provider: v.Require("LLM_PROVIDER"),
		BaseURL:  strings.TrimRight(EnvOrDefault("LLM_BASE_URL", ""), "/"),
	}
	if s.Provider != "" && !slices.Contains(acceptedLLMProviders, s.Provider) {
		v.Add("LLM_PROVIDER (must be " + strings.Join(acceptedLLMProviders, "|") + ")")
	}
	if s.Provider == "openai-compatible" && s.BaseURL == "" {
		v.Add("LLM_BASE_URL")
	}
	if s.BaseURL != "" && !validLLMBaseURL(s.BaseURL) {
		v.Add("LLM_BASE_URL (must be an http:// or https:// server root with no user:password@, ?query or #fragment, without a trailing /v1)")
	}
	return s
}

func validLLMBaseURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	if u.User != nil || strings.ContainsAny(raw, "?#") {
		return false
	}
	return !strings.HasSuffix(u.Path, "/v1")
}
