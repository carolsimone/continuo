package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAcceptedLLMProviders(t *testing.T) {
	assert.Equal(t, []string{"anthropic", "openai", "openai-compatible"}, AcceptedLLMProviders())
	got := AcceptedLLMProviders()
	got[0] = "mutated"
	assert.Equal(t, "anthropic", AcceptedLLMProviders()[0], "callers get a copy")
}

func TestLoadLLM(t *testing.T) {
	cases := []struct {
		name        string
		provider    string
		baseURL     string
		wantBase    string
		wantMissing []string
	}{
		{name: "anthropic, public address", provider: "anthropic", wantMissing: nil},
		{name: "openai behind a gateway", provider: "openai", baseURL: "https://llm-gateway.corp.example/", wantBase: "https://llm-gateway.corp.example"},
		{name: "self-hosted model", provider: "openai-compatible", baseURL: "http://gpu-box.internal:8000", wantBase: "http://gpu-box.internal:8000"},
		{name: "path prefix kept", provider: "openai-compatible", baseURL: "https://openrouter.ai/api", wantBase: "https://openrouter.ai/api"},
		{name: "unset provider", provider: "", wantMissing: []string{"LLM_PROVIDER"}},
		{name: "unknown provider", provider: "grok", wantMissing: []string{"LLM_PROVIDER (must be anthropic|openai|openai-compatible)"}},
		{name: "openai-compatible needs a base URL", provider: "openai-compatible", wantMissing: []string{"LLM_BASE_URL"}},
		{name: "trailing /v1 refused", provider: "openai-compatible", baseURL: "https://api.mistral.ai/v1", wantBase: "https://api.mistral.ai/v1",
			wantMissing: []string{"LLM_BASE_URL (must be an http:// or https:// server root, without a trailing /v1)"}},
		{name: "trailing /v1/ refused", provider: "anthropic", baseURL: "https://gw.example/v1/", wantBase: "https://gw.example/v1",
			wantMissing: []string{"LLM_BASE_URL (must be an http:// or https:// server root, without a trailing /v1)"}},
		{name: "no scheme refused", provider: "openai-compatible", baseURL: "api.mistral.ai", wantBase: "api.mistral.ai",
			wantMissing: []string{"LLM_BASE_URL (must be an http:// or https:// server root, without a trailing /v1)"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("LLM_PROVIDER", tc.provider)
			t.Setenv("LLM_BASE_URL", tc.baseURL)
			v := &Validator{}
			got := LoadLLM(v)
			assert.Equal(t, tc.provider, got.Provider)
			assert.Equal(t, tc.wantBase, got.BaseURL)
			assert.Equal(t, tc.wantMissing, v.Missing())
		})
	}
}
