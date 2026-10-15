package deploycheck

import (
	"encoding/json"
	"slices"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// The chart's schema enumerates the llm.provider values an install may set.
// A value the schema allows but the agents reject would render and then stop
// both agents at startup; one the agents accept but the schema rejects could
// never be configured.
func TestChartLLMProvidersMatchTheServices(t *testing.T) {
	var schema struct {
		Properties struct {
			LLM struct {
				Properties struct {
					Provider struct {
						Enum []string `json:"enum"`
					} `json:"provider"`
				} `json:"properties"`
			} `json:"llm"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.schema.json")), &schema); err != nil {
		t.Fatalf("parse values.schema.json: %v", err)
	}
	got := schema.Properties.LLM.Properties.Provider.Enum
	if want := pkgconfig.AcceptedLLMProviders(); !slices.Equal(got, want) {
		t.Fatalf("values.schema.json llm.provider enum is %v, want %v (pkg/config.AcceptedLLMProviders, in order)", got, want)
	}
}
