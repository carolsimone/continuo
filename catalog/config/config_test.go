package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	croninfra "github.com/carolsimone/continuo/catalog/adapters/cron"
	"github.com/carolsimone/continuo/catalog/service/ports"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// catalog reads nothing from run-controller: it starts without run-controller's address.
func TestLoad_NeedsNothingFromRunController(t *testing.T) {
	t.Setenv("RUN_CONTROLLER_GRPC_ADDR", "")
	var v pkgconfig.Validator
	Load(&v, croninfra.BackupScheduleParser{})
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
	Load(&v, croninfra.BackupScheduleParser{})
	for _, name := range v.Missing() {
		if strings.HasPrefix(name, "POSTGRES_") {
			t.Fatalf("Load requires %s; missing = %v", name, v.Missing())
		}
	}
}

// With nothing set, catalog exports every tenant at 02:00 UTC and keeps the
// newest seven exports, the chart's defaults.
func TestConfig_BackupDefaults(t *testing.T) {
	for _, key := range []string{"CATALOG_BACKUP_ENABLED", "CATALOG_BACKUP_SCHEDULE", "CATALOG_BACKUP_KEEP"} {
		t.Setenv(key, "")
	}
	var v pkgconfig.Validator
	cfg := Load(&v, croninfra.BackupScheduleParser{})
	if want := (BackupConfig{Enabled: true, Schedule: "0 2 * * *", Keep: 7}); cfg.Backup != want {
		t.Fatalf("Backup = %+v, want %+v", cfg.Backup, want)
	}
	assertNoBackupProblem(t, v.Missing())
}

func TestConfig_BackupReadsTheThreeKeys(t *testing.T) {
	t.Setenv("CATALOG_BACKUP_ENABLED", "false")
	t.Setenv("CATALOG_BACKUP_SCHEDULE", "30 3 * * 1")
	t.Setenv("CATALOG_BACKUP_KEEP", "3")
	var v pkgconfig.Validator
	cfg := Load(&v, croninfra.BackupScheduleParser{})
	if want := (BackupConfig{Enabled: false, Schedule: "30 3 * * 1", Keep: 3}); cfg.Backup != want {
		t.Fatalf("Backup = %+v, want %+v", cfg.Backup, want)
	}
	assertNoBackupProblem(t, v.Missing())
}

// The schedules the chart accepts and robfig/cron's standard parser fires:
// five fields, an @ descriptor, @every of at least a minute, with the
// surrounding blanks the chart allows.
func TestConfig_AcceptsTheSchedulesTheChartRenders(t *testing.T) {
	for _, spec := range []string{"0 2 * * *", "30 3 * * 1", "*/15 * * * *", "@daily", "@hourly", "@every 6h", "@every 1m", "  0 2 * * *  ", "0 0 29 2 *"} {
		t.Run(spec, func(t *testing.T) {
			t.Setenv("CATALOG_BACKUP_SCHEDULE", spec)
			var v pkgconfig.Validator
			cfg := Load(&v, croninfra.BackupScheduleParser{})
			assertNoBackupProblem(t, v.Missing())
			if cfg.Backup.Schedule != spec {
				t.Fatalf("Schedule = %q, want %q", cfg.Backup.Schedule, spec)
			}
		})
	}
}

// catalog refuses to start on a schedule it cannot honour: one the cron
// parser refuses, one that names a timezone (the export runs in UTC), one no
// date satisfies, an @every under a minute, and a keep below one or not a
// whole number, and an enabled flag that is not a boolean.
func TestConfig_RefusesBadCronAndKeepBelowOne(t *testing.T) {
	cases := []struct {
		key, value string
	}{
		{"CATALOG_BACKUP_SCHEDULE", "nightly"},
		{"CATALOG_BACKUP_SCHEDULE", "   "},
		{"CATALOG_BACKUP_SCHEDULE", "0 2 * *"},
		{"CATALOG_BACKUP_SCHEDULE", "0 2 * * * *"},
		{"CATALOG_BACKUP_SCHEDULE", "61 2 * * *"},
		{"CATALOG_BACKUP_SCHEDULE", "@nightly"},
		{"CATALOG_BACKUP_SCHEDULE", "CRON_TZ=UTC 0 2 * * *"},
		{"CATALOG_BACKUP_SCHEDULE", "TZ=UTC 0 2 * * *"},
		{"CATALOG_BACKUP_SCHEDULE", "CRON_TZ=UTC 0 2 * *"},
		{"CATALOG_BACKUP_SCHEDULE", "TZ=Europe/Paris @daily"},
		{"CATALOG_BACKUP_SCHEDULE", " CRON_TZ=UTC 0 2 * * *"},
		{"CATALOG_BACKUP_SCHEDULE", "0 0 30 2 *"},
		{"CATALOG_BACKUP_SCHEDULE", "@every 30s"},
		{"CATALOG_BACKUP_SCHEDULE", "@every 0s"},
		{"CATALOG_BACKUP_KEEP", "0"},
		{"CATALOG_BACKUP_KEEP", "-1"},
		{"CATALOG_BACKUP_KEEP", "seven"},
		{"CATALOG_BACKUP_KEEP", "1.5"},
		{"CATALOG_BACKUP_ENABLED", "yes please"},
		{"CATALOG_BACKUP_ENABLED", "enabled"},
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.value, func(t *testing.T) {
			t.Setenv(c.key, c.value)
			var v pkgconfig.Validator
			Load(&v, croninfra.BackupScheduleParser{})
			var named []string
			for _, problem := range v.Missing() {
				if strings.HasPrefix(problem, c.key+" ") {
					named = append(named, problem)
				}
			}
			if len(named) != 1 {
				t.Fatalf("Load recorded %v for %s=%q, want one problem naming %s", v.Missing(), c.key, c.value, c.key)
			}
			if !strings.Contains(named[0], strconvQuote(c.value)) {
				t.Errorf("the problem %q does not quote the value %q", named[0], c.value)
			}
		})
	}
}

// assertNoBackupProblem fails when Load recorded a problem with one of the
// backup keys.
func assertNoBackupProblem(t *testing.T, missing []string) {
	t.Helper()
	for _, problem := range missing {
		if strings.HasPrefix(problem, "CATALOG_BACKUP_") {
			t.Fatalf("Load recorded %q", problem)
		}
	}
}

func strconvQuote(s string) string { return fmt.Sprintf("%q", s) }

// LoadNeo4j reads the three Neo4j settings the operator command shares with
// the service, and records each one that is missing.
func TestLoadNeo4j_ReadsAndRequiresTheThreeKeys(t *testing.T) {
	t.Setenv("NEO4J_URI", "bolt://neo4j:7687")
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "secret")
	var v pkgconfig.Validator
	got := LoadNeo4j(&v)
	if want := (Neo4jConfig{URI: "bolt://neo4j:7687", User: "neo4j", Password: "secret"}); got != want {
		t.Fatalf("LoadNeo4j = %+v, want %+v", got, want)
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("LoadNeo4j recorded %v", v.Missing())
	}

	for _, key := range []string{"NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD"} {
		t.Setenv(key, "")
	}
	var missing pkgconfig.Validator
	LoadNeo4j(&missing)
	if len(missing.Missing()) != 3 {
		t.Fatalf("LoadNeo4j recorded %v, want the three keys", missing.Missing())
	}
}

// LoadBackupKeep is the keep Load reads: the default 7 when unset, the value
// when it is a whole number of at least 1, and a recorded problem with the
// default returned otherwise.
func TestLoadBackupKeep(t *testing.T) {
	for _, c := range []struct {
		raw     string
		want    int
		problem bool
	}{
		{"", 7, false},
		{"1", 1, false},
		{"30", 30, false},
		{"0", 7, true},
		{"-2", 7, true},
		{"many", 7, true},
	} {
		t.Run(c.raw, func(t *testing.T) {
			t.Setenv("CATALOG_BACKUP_KEEP", c.raw)
			var v pkgconfig.Validator
			if got := LoadBackupKeep(&v); got != c.want {
				t.Fatalf("LoadBackupKeep = %d, want %d", got, c.want)
			}
			if problem := len(v.Missing()) > 0; problem != c.problem {
				t.Fatalf("problems = %v, want a problem: %v", v.Missing(), c.problem)
			}
		})
	}
}

// refusingParser refuses every spec with a fixed reason.
type refusingParser struct{}

func (refusingParser) Parse(string) (ports.BackupSchedule, error) {
	return nil, errors.New("not a schedule")
}

// Load takes its judgement of a schedule from the parser it is given, so the
// config package names no cron library.
func TestConfig_ChecksTheScheduleWithTheParserItIsGiven(t *testing.T) {
	t.Setenv("CATALOG_BACKUP_SCHEDULE", "0 2 * * *")
	var v pkgconfig.Validator
	Load(&v, refusingParser{})
	var found bool
	for _, problem := range v.Missing() {
		found = found || strings.Contains(problem, "CATALOG_BACKUP_SCHEDULE") && strings.Contains(problem, "not a schedule")
	}
	if !found {
		t.Fatalf("Load recorded %v, want CATALOG_BACKUP_SCHEDULE refused with the parser's reason", v.Missing())
	}
}

func TestNeo4jPasswordFromFile(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "NEO4J_PASSWORD")
	if err := os.WriteFile(passwordFile, []byte("pw-from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NEO4J_URI", "bolt://neo4j:7687")
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "")
	t.Setenv("NEO4J_PASSWORD_FILE", passwordFile)

	var v pkgconfig.Validator
	cfg := LoadNeo4j(&v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no problems, got %v", v.Missing())
	}
	if cfg.Password != "pw-from-file" {
		t.Fatalf("Password = %q, want %q", cfg.Password, "pw-from-file")
	}
}

func TestNeo4jPasswordMissingIsReported(t *testing.T) {
	t.Setenv("NEO4J_URI", "bolt://neo4j:7687")
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "")
	t.Setenv("NEO4J_PASSWORD_FILE", "")

	var v pkgconfig.Validator
	LoadNeo4j(&v)
	if !slices.Contains(v.Missing(), "NEO4J_PASSWORD") {
		t.Fatalf("missing = %v, want NEO4J_PASSWORD", v.Missing())
	}
}
