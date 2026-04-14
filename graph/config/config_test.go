package config

import (
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

func TestLoad_neo4j_required_missing(t *testing.T) {
	for _, key := range []string{"NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD"} {
		t.Setenv(key, "")
	}
	v := &pkgconfig.Validator{}
	Load(v)
	if got := len(v.Missing()); got != 3 {
		t.Fatalf("want 3 missing vars (NEO4J_*), got %d: %v", got, v.Missing())
	}
}

func TestLoad_neo4j_all_set(t *testing.T) {
	t.Setenv("NEO4J_URI", "bolt://neo4j:7687")
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "secret")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Neo4j.URI != "bolt://neo4j:7687" || cfg.Neo4j.User != "neo4j" || cfg.Neo4j.Password != "secret" {
		t.Fatalf("unexpected config: %+v", cfg.Neo4j)
	}
}
