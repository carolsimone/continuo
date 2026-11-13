// Package warehouse maps each supported warehouse engine to its drop family.
//
// The family says what dropping a table does to the objects that read it. A
// schema_bound engine (postgres) binds a view to its base table, so dropping
// that table with CASCADE, as a dbt seed load does, drops every view and
// other object that depends on it. A late_binding engine (trino) resolves a
// view's table names at query time, so a DROP succeeds and the view turns
// stale until its table is created again.
package warehouse

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Family is the drop behaviour of a warehouse engine.
type Family string

const (
	// SchemaBound is an engine whose views hold a dependency on their base
	// tables, so dropping a table with CASCADE drops its dependants with it.
	SchemaBound Family = "schema_bound"
	// LateBinding is an engine whose views resolve their tables when queried,
	// so a table can be dropped while views still name it.
	LateBinding Family = "late_binding"
)

// ErrUnsupportedEngine reports a WAREHOUSE_ENGINE value with no family.
var ErrUnsupportedEngine = errors.New("warehouse: unsupported engine")

// families maps each supported engine to its family. An empty engine name is
// resolved to postgres by FamilyOf, the engine an install that never chose one runs.
var families = map[string]Family{
	"postgres": SchemaBound,
	"trino":    LateBinding,
}

var displayNames = map[string]string{
	"postgres": "Postgres",
	"trino":    "Trino",
}

// IsValid reports whether f is one of the defined families.
func (f Family) IsValid() bool {
	return f == SchemaBound || f == LateBinding
}

// FamilyOf returns the family of engine. An empty engine means postgres. An
// engine with no family wraps ErrUnsupportedEngine.
func FamilyOf(engine string) (Family, error) {
	if engine == "" {
		engine = "postgres"
	}
	f, ok := families[engine]
	if !ok {
		return "", fmt.Errorf("%w %q: expected one of %s", ErrUnsupportedEngine, engine, strings.Join(Engines(), ", "))
	}
	return f, nil
}

// Engines returns the supported engine names, sorted.
func Engines() []string {
	engines := make([]string, 0, len(families))
	for e := range families {
		engines = append(engines, e)
	}
	slices.Sort(engines)
	return engines
}

// DisplayName returns the human name of engine, or the engine as given when it
// has none.
func DisplayName(engine string) string {
	if name, ok := displayNames[engine]; ok {
		return name
	}
	return engine
}
