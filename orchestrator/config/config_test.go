package config

import (
	"slices"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// orchestrator reads nothing from run-controller: it starts without run-controller's address.
func TestLoad_NeedsNothingFromRunController(t *testing.T) {
	t.Setenv("STATE_GRPC_ADDR", "")
	var v pkgconfig.Validator
	Load(&v)
	if slices.Contains(v.Missing(), "STATE_GRPC_ADDR") {
		t.Fatalf("Load requires STATE_GRPC_ADDR; missing = %v", v.Missing())
	}
}
