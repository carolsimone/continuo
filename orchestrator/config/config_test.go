package config

import (
	"slices"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// orchestrator reads nothing from state: it starts without state's address.
func TestLoad_NeedsNothingFromState(t *testing.T) {
	t.Setenv("STATE_GRPC_ADDR", "")
	var v pkgconfig.Validator
	Load(&v)
	if slices.Contains(v.Missing(), "STATE_GRPC_ADDR") {
		t.Fatalf("Load requires STATE_GRPC_ADDR; missing = %v", v.Missing())
	}
}
