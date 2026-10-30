package config

import (
	"slices"
	"strings"
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

// catalog keeps all of its state in Neo4j: it starts without any Postgres
// setting.
func TestLoad_NeedsNoPostgres(t *testing.T) {
	for _, key := range []string{"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"} {
		t.Setenv(key, "")
	}
	var v pkgconfig.Validator
	Load(&v)
	for _, name := range v.Missing() {
		if strings.HasPrefix(name, "POSTGRES_") {
			t.Fatalf("Load requires %s; missing = %v", name, v.Missing())
		}
	}
}
