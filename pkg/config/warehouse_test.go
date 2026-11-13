package config_test

import (
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/warehouse"
)

func TestLoadWarehouse(t *testing.T) {
	t.Setenv("WAREHOUSE_ENGINE", "trino")
	v := &pkgconfig.Validator{}
	engine, family := pkgconfig.LoadWarehouse(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("unexpected problems: %v", v.Missing())
	}
	if engine != "trino" || family != warehouse.LateBinding {
		t.Errorf("trino: got (%q, %q)", engine, family)
	}

	t.Setenv("WAREHOUSE_ENGINE", "")
	engine, family = pkgconfig.LoadWarehouse(&pkgconfig.Validator{})
	if engine != "postgres" || family != warehouse.SchemaBound {
		t.Errorf("unset: got (%q, %q), want (postgres, schema_bound)", engine, family)
	}

	t.Setenv("WAREHOUSE_ENGINE", "duckdb")
	v = &pkgconfig.Validator{}
	_, family = pkgconfig.LoadWarehouse(v)
	if family.IsValid() {
		t.Errorf("unsupported engine returned a valid family %q", family)
	}
	want := `WAREHOUSE_ENGINE (unsupported "duckdb": expected one of postgres, trino)`
	found := false
	for _, m := range v.Missing() {
		if m == want {
			found = true
		}
	}
	if !found {
		t.Errorf("Missing() = %v, want it to contain %q", v.Missing(), want)
	}
}
