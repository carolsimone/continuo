package config

import (
	"slices"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// catalog reads nothing from run-controller: it starts without run-controller's address.
func TestLoad_NeedsNothingFromRunController(t *testing.T) {
	t.Setenv("RUN_CONTROLLER_GRPC_ADDR", "")
	var v pkgconfig.Validator
	Load(&v)
	if slices.Contains(v.Missing(), "RUN_CONTROLLER_GRPC_ADDR") {
		t.Fatalf("Load requires RUN_CONTROLLER_GRPC_ADDR; missing = %v", v.Missing())
	}
}
