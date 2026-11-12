package warehouse

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

func TestFamilyOf(t *testing.T) {
	for engine, want := range map[string]Family{"": SchemaBound, "postgres": SchemaBound, "trino": LateBinding} {
		got, err := FamilyOf(engine)
		if err != nil {
			t.Fatalf("FamilyOf(%q): %v", engine, err)
		}
		if got != want {
			t.Errorf("FamilyOf(%q) = %q, want %q", engine, got, want)
		}
	}
	_, err := FamilyOf("duckdb")
	if !errors.Is(err, ErrUnsupportedEngine) {
		t.Fatalf("FamilyOf(duckdb) error = %v, want ErrUnsupportedEngine", err)
	}
	if want := `unsupported engine "duckdb": expected one of postgres, trino`; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not contain %q", err, want)
	}
}

// The chart refuses an install whose validation.engine is not in its list, so
// an engine the chart accepts but this map lacks would stop run-controller at boot.
func TestFamiliesMatchTheChartsEngines(t *testing.T) {
	tpl, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy/continuo/templates/_helpers.tpl"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`\$supported := list ((?:"[a-z]+" ?)+)`).FindSubmatch(tpl)
	if m == nil {
		t.Fatal("the chart's engine list not found in _helpers.tpl")
	}
	var chart []string
	for _, e := range regexp.MustCompile(`"([a-z]+)"`).FindAllSubmatch(m[1], -1) {
		chart = append(chart, string(e[1]))
	}
	sort.Strings(chart)
	if !slices.Equal(chart, Engines()) {
		t.Errorf("chart engines %v, warehouse.Engines() %v", chart, Engines())
	}
}

func TestDisplayName(t *testing.T) {
	cases := map[string]string{"postgres": "Postgres", "trino": "Trino", "duckdb": "duckdb"}
	for engine, want := range cases {
		if got := DisplayName(engine); got != want {
			t.Errorf("DisplayName(%q) = %q, want %q", engine, got, want)
		}
	}
}

// repoRoot walks up from the test working directory until it finds go.work.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.work not found above %s", dir)
		}
		dir = parent
	}
}
