package main

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// parseMain parses this service's main.go.
func parseMain(t *testing.T) *ast.File {
	t.Helper()
	return parseFile(t, "main.go")
}

// parseFile parses the Go file at path, relative to this directory.
func parseFile(t *testing.T, path string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return f
}

// backupCommandMain is main.go of the catalog-backup binary, which ships in
// catalog's image and keeps no Postgres either.
const backupCommandMain = "cmd/catalog-backup/main.go"

// TestMain_ConsumesOnlyTopologyAndCaseBaseStreams pins the streams catalog
// reads: the two release.promoted:v2 groups (topology swap, code versions) and
// the three case-base groups. run-controller plans, dispatches, unblocks and finalizes
// runs, so catalog consumes no run stream.
func TestMain_ConsumesOnlyTopologyAndCaseBaseStreams(t *testing.T) {
	seen := map[string]bool{}
	ast.Inspect(parseMain(t), func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "streams" {
			seen[sel.Sel.Name] = true
		}
		return true
	})
	got := make([]string, 0, len(seen))
	for name := range seen {
		got = append(got, name)
	}
	slices.Sort(got)
	want := []string{
		"CatalogReleaseTopology",
		"CatalogReleaseVersions",
		"CatalogRemediationPrClosedProvenance",
		"CatalogRemediationPrOpenedProposals",
		"CatalogRemediationRequestedRejections",
		"ReleasePromotedV2",
		"RemediationPrClosedV1",
		"RemediationPrOpenedV1",
		"RemediationRequestedV2",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("main.go names streams %v, want %v", got, want)
	}
}

// TestMain_RunsNoRunReconciler pins that catalog holds no run status to
// converge: it imports no reconciler.
func TestMain_RunsNoRunReconciler(t *testing.T) {
	for _, imp := range parseMain(t).Imports {
		if path := strings.Trim(imp.Path.Value, `"`); strings.HasSuffix(path, "/internal/reconciler") {
			t.Fatalf("main.go imports %s", path)
		}
	}
}

// TestMain_ProbesRedisAndNeo4j pins the dependencies readiness checks: the two
// stores catalog reads and writes. It keeps no SQL database, so it registers no
// SQL probe.
func TestMain_ProbesRedisAndNeo4j(t *testing.T) {
	var got []string
	ast.Inspect(parseMain(t), func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "AddProbe" {
			return true
		}
		if lit, ok := call.Args[0].(*ast.BasicLit); ok {
			got = append(got, strings.Trim(lit.Value, `"`))
		}
		return true
	})
	slices.Sort(got)
	if want := []string{"neo4j", "redis"}; !slices.Equal(got, want) {
		t.Fatalf("main.go registers readiness probes %v, want %v", got, want)
	}
}

// TestMain_ImportsNoSQLPackage pins that catalog keeps all of its state in
// Neo4j: main.go imports no SQL connection, no SQL driver, no
// message-processing dedup table and no transactional outbox. The retention
// sweeper it runs comes from pkg/retention, which holds no database code
// (see TestMain_SweepsProcessedMessageMarkers).
func TestMain_ImportsNoSQLPackage(t *testing.T) {
	banned := []string{
		"database/sql",
		"github.com/jmoiron/sqlx",
		"github.com/lib/pq",
		"github.com/carolsimone/continuo/pkg/db",
		"github.com/carolsimone/continuo/pkg/messageprocessing",
		"github.com/carolsimone/continuo/pkg/outbox",
	}
	for _, file := range []string{"main.go", backupCommandMain} {
		for _, imp := range parseFile(t, file).Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if slices.Contains(banned, path) {
				t.Errorf("%s imports %s", file, path)
			}
		}
	}
}

// postgresPackages are the packages that link a Postgres connection into a
// binary: the two SQL drivers the services use (sqlx and lib/pq, with their
// subpackages) and this repository's Postgres-backed packages, the connection
// helper and the transactional outbox. The standard library's database/sql is
// not among them: it opens no connection by itself, and packages catalog
// needs link it whatever catalog does (google/uuid's Scan and the database
// statistics collector of prometheus/client_golang).
var postgresPackages = []string{
	"github.com/jmoiron/sqlx",
	"github.com/lib/pq",
	"github.com/carolsimone/continuo/pkg/db",
	"github.com/carolsimone/continuo/pkg/outbox",
}

// TestMain_LinksNoSQLDriver pins that no package catalog's binary links, its
// own or a dependency's, is a SQL driver or a Postgres-backed package of this
// repository: catalog keeps no Postgres, so a shared package that pulls one
// in is in the wrong place. It reads the transitive closure from
// `go list -deps` and fails, never skips, when go cannot run.
func TestMain_LinksNoSQLDriver(t *testing.T) {
	for _, tc := range []struct{ pkg, mustLink string }{
		{".", "github.com/carolsimone/continuo/pkg/retention"},
		{"./cmd/catalog-backup", "github.com/carolsimone/continuo/catalog/adapters/neo4j"},
	} {
		t.Run(tc.pkg, func(t *testing.T) {
			cmd := exec.Command("go", "list", "-deps", tc.pkg)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("go list -deps %s: %v\n%s", tc.pkg, err, stderr.String())
			}
			deps := strings.Fields(string(out))
			if !slices.Contains(deps, tc.mustLink) {
				t.Fatalf("go list -deps %s does not list %s, which the binary runs: the closure it read is not catalog's (%d packages)", tc.pkg, tc.mustLink, len(deps))
			}
			for _, dep := range deps {
				for _, banned := range postgresPackages {
					if dep == banned || strings.HasPrefix(dep, banned+"/") {
						t.Errorf("%s links %s", tc.pkg, dep)
					}
				}
			}
		})
	}
}

// TestMain_SweepsProcessedMessageMarkers pins how main.go runs the marker
// sweeper: through the shared retention sweeper, with one target named
// processed_messages whose prune is the marker sweeper's and whose minimum
// retention is the replay horizon, so a RETENTION_DAYS below 30 days is raised
// to it; with the install's RETENTION_DAYS and RETENTION_SWEEP_INTERVAL_MINUTES
// as its retention and interval; and as a goroutine the lifecycle manager
// tracks, so shutdown waits for it.
func TestMain_SweepsProcessedMessageMarkers(t *testing.T) {
	file := parseMain(t)

	// The call that builds the sweeper, and the variable that holds it.
	var build *ast.CallExpr
	var sweeper string
	ast.Inspect(file, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		call, ok := assign.Rhs[0].(*ast.CallExpr)
		if !ok || !isSelector(call.Fun, "pkgretention", "NewSweeper") {
			return true
		}
		build = call
		if id, ok := assign.Lhs[0].(*ast.Ident); ok {
			sweeper = id.Name
		}
		return false
	})
	if build == nil || sweeper == "" || len(build.Args) != 3 {
		t.Fatal("main.go does not assign pkgretention.NewSweeper(targets, config, logger) to a variable")
	}

	targets, ok := build.Args[0].(*ast.CompositeLit)
	if !ok || len(targets.Elts) != 1 {
		t.Fatal("the retention sweeper must have exactly one target: the processed-message markers")
	}
	target := fields(targets.Elts[0])
	if lit, ok := target["Name"].(*ast.BasicLit); !ok || lit.Value != `"processed_messages"` {
		t.Errorf("the target's Name = %v, want \"processed_messages\"", exprText(target["Name"]))
	}
	if sel, ok := target["Prune"].(*ast.SelectorExpr); !ok || sel.Sel.Name != "Prune" {
		t.Errorf("the target's Prune = %v, want the marker sweeper's Prune", exprText(target["Prune"]))
	}
	if !isSelector(target["MinRetention"], "model", "ReplayHorizon") {
		t.Errorf("the target's MinRetention = %v, want model.ReplayHorizon", exprText(target["MinRetention"]))
	}

	config, ok := build.Args[1].(*ast.CompositeLit)
	if !ok {
		t.Fatal("the retention sweeper's config must be a pkgretention.Config literal")
	}
	cfg := fields(config)
	if !mentionsSelector(cfg["Retention"], "cfg", "RetentionDays") {
		t.Errorf("the config's Retention = %v, want it computed from cfg.RetentionDays", exprText(cfg["Retention"]))
	}
	if !mentionsSelector(cfg["Interval"], "cfg", "RetentionSweepIntervalMin") {
		t.Errorf("the config's Interval = %v, want it computed from cfg.RetentionSweepIntervalMin", exprText(cfg["Interval"]))
	}

	// The loop runs inside lifecycleManager.Go.
	tracked := false
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isSelector(call.Fun, "lifecycleManager", "Go") || len(call.Args) != 1 {
			return true
		}
		body, ok := call.Args[0].(*ast.FuncLit)
		if !ok {
			return true
		}
		ast.Inspect(body.Body, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok && isSelector(c.Fun, sweeper, "Run") {
				tracked = true
			}
			return true
		})
		return true
	})
	if !tracked {
		t.Errorf("main.go must start %s.Run inside lifecycleManager.Go, so shutdown waits for the sweep", sweeper)
	}
}

// TestMain_RunsTheBackupOnlyWhenEnabled pins how main.go runs the nightly
// export: only inside `if cfg.Backup.Enabled`, where it registers the backup
// series, builds the exporter with the install's keep and the scheduler with
// the install's schedule, and starts the scheduler's Run inside
// lifecycleManager.Go, so shutdown cancels an export in flight and waits for
// it to abort its upload and release its lease.
func TestMain_RunsTheBackupOnlyWhenEnabled(t *testing.T) {
	file := parseMain(t)

	var enabled *ast.IfStmt
	ast.Inspect(file, func(n ast.Node) bool {
		if s, ok := n.(*ast.IfStmt); ok && exprText(s.Cond) == "cfg.Backup.Enabled" {
			enabled = s
			return false
		}
		return true
	})
	if enabled == nil {
		t.Fatal("main.go has no `if cfg.Backup.Enabled` block")
	}

	calls := map[string]*ast.CallExpr{}
	var scheduler string
	ast.Inspect(enabled.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if call, ok := x.Rhs[0].(*ast.CallExpr); ok && isSelector(call.Fun, "backup", "NewScheduler") {
				if id, ok := x.Lhs[0].(*ast.Ident); ok {
					scheduler = id.Name
				}
			}
		case *ast.CallExpr:
			for _, name := range []string{"backup.NewExporter", "backup.NewScheduler", "metricsinfra.NewBackupMetrics", "s3infra.NewBackupStore"} {
				pkg, fn, _ := strings.Cut(name, ".")
				if isSelector(x.Fun, pkg, fn) {
					calls[name] = x
				}
			}
		}
		return true
	})
	for _, name := range []string{"backup.NewExporter", "backup.NewScheduler", "metricsinfra.NewBackupMetrics", "s3infra.NewBackupStore"} {
		if calls[name] == nil {
			t.Errorf("main.go does not call %s inside `if cfg.Backup.Enabled`", name)
		}
	}
	if t.Failed() {
		return
	}
	if args := calls["backup.NewExporter"].Args; len(args) != 7 || exprText(args[4]) != "cfg.Backup.Keep" {
		t.Errorf("backup.NewExporter must take cfg.Backup.Keep as its keep, got %v", exprText(calls["backup.NewExporter"]))
	}
	if args := calls["backup.NewScheduler"].Args; len(args) != 5 || exprText(args[0]) != "cfg.Backup.Schedule" {
		t.Errorf("backup.NewScheduler must take cfg.Backup.Schedule as its spec, got %v", exprText(calls["backup.NewScheduler"]))
	}
	if scheduler == "" {
		t.Fatal("main.go does not assign backup.NewScheduler's scheduler to a variable")
	}

	tracked := false
	ast.Inspect(enabled.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isSelector(call.Fun, "lifecycleManager", "Go") || len(call.Args) != 1 {
			return true
		}
		if body, ok := call.Args[0].(*ast.FuncLit); ok {
			ast.Inspect(body.Body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok && isSelector(c.Fun, scheduler, "Run") {
					tracked = true
				}
				return true
			})
		}
		return true
	})
	if !tracked {
		t.Errorf("main.go must start %s.Run inside lifecycleManager.Go", scheduler)
	}

	// Nothing of the backup is built outside the block.
	outside := 0
	ast.Inspect(file, func(n ast.Node) bool {
		if n == enabled {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok {
			if isSelector(call.Fun, "backup", "NewScheduler") || isSelector(call.Fun, "metricsinfra", "NewBackupMetrics") {
				outside++
			}
		}
		return true
	})
	if outside != 0 {
		t.Errorf("main.go builds the backup scheduler or its series outside `if cfg.Backup.Enabled` (%d calls)", outside)
	}
}

// fields maps the keys of a keyed composite literal to their values.
func fields(e ast.Expr) map[string]ast.Expr {
	out := map[string]ast.Expr{}
	lit, ok := e.(*ast.CompositeLit)
	if !ok {
		return out
	}
	for _, elt := range lit.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			if key, ok := kv.Key.(*ast.Ident); ok {
				out[key.Name] = kv.Value
			}
		}
	}
	return out
}

// isSelector reports whether e is the selector expression pkg.name.
func isSelector(e ast.Expr, pkg, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg && sel.Sel.Name == name
}

// mentionsSelector reports whether the selector pkg.name occurs anywhere in e.
func mentionsSelector(e ast.Expr, pkg, name string) bool {
	found := false
	if e != nil {
		ast.Inspect(e, func(n ast.Node) bool {
			if x, ok := n.(ast.Expr); ok && isSelector(x, pkg, name) {
				found = true
			}
			return !found
		})
	}
	return found
}

// exprText renders e for a failure message; a missing expression reads as
// "absent".
func exprText(e ast.Expr) string {
	if e == nil {
		return "absent"
	}
	var b strings.Builder
	if err := format.Node(&b, token.NewFileSet(), e); err != nil {
		return "unprintable"
	}
	return b.String()
}
