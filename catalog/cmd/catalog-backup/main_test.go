package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	"github.com/carolsimone/continuo/catalog/service/backup"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeExporter records the tenants it was asked to export and answers each
// with its configured result or error.
type fakeExporter struct {
	calls   []string
	results map[string]backup.Result
	errs    map[string]error
	// onExport runs inside ExportTenant, before it answers.
	onExport func(ctx context.Context, tenantID string)
}

func (f *fakeExporter) ExportTenant(ctx context.Context, tenantID string) (backup.Result, error) {
	f.calls = append(f.calls, tenantID)
	if f.onExport != nil {
		f.onExport(ctx, tenantID)
	}
	if err := f.errs[tenantID]; err != nil {
		return backup.Result{}, err
	}
	return f.results[tenantID], nil
}

// fakeRestorer records the keys it was asked to restore.
type fakeRestorer struct {
	keys []string
	res  backup.RestoreResult
	err  error
}

func (f *fakeRestorer) Restore(_ context.Context, key string) (backup.RestoreResult, error) {
	f.keys = append(f.keys, key)
	return f.res, f.err
}

// fakeStore lists each tenant's configured exports.
type fakeStore struct {
	objects map[string][]ports.BackupObject
	err     error
	listed  []string
}

func (f *fakeStore) List(_ context.Context, tenantID string) ([]ports.BackupObject, error) {
	f.listed = append(f.listed, tenantID)
	return f.objects[tenantID], f.err
}

type fakeTenants struct {
	tenants []string
	err     error
}

func (f *fakeTenants) Tenants(context.Context) ([]string, error) { return f.tenants, f.err }

// harness runs the command over the fakes and counts how often it connected
// and released its connections.
type harness struct {
	exporter   *fakeExporter
	restorer   *fakeRestorer
	store      *fakeStore
	tenants    *fakeTenants
	connectErr error
	connects   int
	closes     int
}

func newHarness() *harness {
	return &harness{
		exporter: &fakeExporter{results: map[string]backup.Result{}, errs: map[string]error{}},
		restorer: &fakeRestorer{},
		store:    &fakeStore{objects: map[string][]ports.BackupObject{}},
		tenants:  &fakeTenants{},
	}
}

func (h *harness) connect(_ context.Context, logger *slog.Logger) (deps, func(), error) {
	h.connects++
	if h.connectErr != nil {
		return deps{}, nil, h.connectErr
	}
	logger.Info("connected to the fakes")
	return deps{exporter: h.exporter, restorer: h.restorer, store: h.store, tenants: h.tenants}, func() { h.closes++ }, nil
}

func (h *harness) run(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return h.runCtx(t, context.Background(), args...)
}

func (h *harness) runCtx(t *testing.T, ctx context.Context, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(ctx, args, &out, &errOut, s3infra.BackupKeys{}, h.connect)
	return code, out.String(), errOut.String()
}

func (h *harness) result(tenant string, bytesWritten, nodes, rels int64, held time.Duration) {
	h.exporter.results[tenant] = backup.Result{
		ObjectKey:     "tenants/" + tenant + "/backups/catalog/20261030T020000Z.jsonl.gz",
		Bytes:         bytesWritten,
		Nodes:         nodes,
		Relationships: rels,
		Duration:      held,
	}
}

// lines decodes every line of s as a JSON object.
func lines(t *testing.T, s string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m), "stdout line %q", line)
		out = append(out, m)
	}
	return out
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

const acmeKey = "tenants/acme/backups/catalog/20261030T020000Z.jsonl.gz"

// export with no --tenant exports every tenant the catalog holds and prints
// one JSON object per tenant on stdout: the object key, the bytes, nodes and
// relationships stored and the seconds the tenant's writes were paused.
func TestExport_PrintsOneResultPerTenant(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme", "default"}
	h.result("acme", 1234, 10, 20, 1500*time.Millisecond)
	h.result("default", 99, 1, 0, 250*time.Millisecond)

	code, stdout, stderr := h.run(t, "export")

	require.Equal(t, exitOK, code, stderr)
	assert.Equal(t, []string{"acme", "default"}, h.exporter.calls)
	got := lines(t, stdout)
	require.Len(t, got, 2)
	for _, line := range got {
		assert.Equal(t, []string{"bytes", "duration_seconds", "nodes", "object_key", "relationships"}, keysOf(line))
	}
	assert.Equal(t, map[string]any{
		"object_key":       "tenants/acme/backups/catalog/20261030T020000Z.jsonl.gz",
		"bytes":            float64(1234),
		"nodes":            float64(10),
		"relationships":    float64(20),
		"duration_seconds": 1.5,
	}, got[0])
	assert.Equal(t, "tenants/default/backups/catalog/20261030T020000Z.jsonl.gz", got[1]["object_key"])
	assert.Equal(t, 0.25, got[1]["duration_seconds"])
	assert.Equal(t, 1, h.closes, "the connections are released")
}

// --tenant exports that tenant alone.
func TestExport_TenantFlagExportsOnlyThatTenant(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme", "default"}
	h.result("acme", 5, 1, 1, time.Second)

	code, stdout, stderr := h.run(t, "export", "--tenant", "acme")

	require.Equal(t, exitOK, code, stderr)
	assert.Equal(t, []string{"acme"}, h.exporter.calls)
	assert.Len(t, lines(t, stdout), 1)
}

// A lease held by another export or a restore skips the tenant: exit 3, and
// nothing about it on stdout.
func TestExport_SkippedExitsThree(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme"}
	h.exporter.errs["acme"] = fmt.Errorf("export of tenant %q: %w", "acme", backup.ErrSkipped)

	code, stdout, stderr := h.run(t, "export", "--tenant", "acme")

	require.Equal(t, exitSkipped, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "acme")
	assert.Contains(t, stderr, "lease")
}

// Among several tenants, a skipped one does not hide the others' results, but
// the exit code still says something was skipped.
func TestExport_ASkippedTenantAmongSeveralExitsThreeAfterTheOthers(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"a", "b", "c"}
	h.result("a", 1, 1, 1, time.Second)
	h.exporter.errs["b"] = backup.ErrSkipped
	h.result("c", 3, 3, 3, time.Second)

	code, stdout, _ := h.run(t, "export")

	require.Equal(t, exitSkipped, code)
	assert.Equal(t, []string{"a", "b", "c"}, h.exporter.calls)
	got := lines(t, stdout)
	require.Len(t, got, 2)
	assert.Contains(t, got[0]["object_key"], "tenants/a/")
	assert.Contains(t, got[1]["object_key"], "tenants/c/")
}

// A tenant that fails does not stop the others: their results are printed, the
// failure is named on stderr and the exit code is 1, which outranks a skip.
func TestExport_AFailingTenantAmongSeveralGoesOn(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"a", "b", "c", "d"}
	h.result("a", 1, 1, 1, time.Second)
	h.exporter.errs["b"] = errors.New("the store refused the upload")
	h.exporter.errs["c"] = backup.ErrSkipped
	h.result("d", 4, 4, 4, time.Second)

	code, stdout, stderr := h.run(t, "export")

	require.Equal(t, exitError, code)
	assert.Equal(t, []string{"a", "b", "c", "d"}, h.exporter.calls)
	got := lines(t, stdout)
	require.Len(t, got, 2)
	assert.Contains(t, got[0]["object_key"], "tenants/a/")
	assert.Contains(t, got[1]["object_key"], "tenants/d/")
	assert.Contains(t, stderr, "the store refused the upload")
	assert.Contains(t, stderr, `tenant_id=b`)
}

// A catalog that holds no tenant has nothing to export: exit 0, nothing on
// stdout, and stderr says why.
func TestExport_NoTenantIsNothingToExport(t *testing.T) {
	h := newHarness()

	code, stdout, stderr := h.run(t, "export")

	require.Equal(t, exitOK, code, stderr)
	assert.Empty(t, stdout)
	assert.Empty(t, h.exporter.calls)
	assert.Contains(t, stderr, "no tenant")
	assert.Contains(t, stderr, "--tenant")
}

func TestExport_TenantListFailureIsAnError(t *testing.T) {
	h := newHarness()
	h.tenants.err = errors.New("neo4j is down")

	code, stdout, stderr := h.run(t, "export")

	require.Equal(t, exitError, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "neo4j is down")
	assert.Empty(t, h.exporter.calls)
}

// --tenant names a tenant by its id. A tenant the catalog holds no graph for
// is exported as an empty graph, the way the first export of an install that
// has not written yet is taken, and stderr warns that the id is not one the
// catalog lists.
func TestExport_TenantTheCatalogDoesNotListIsExportedWithAWarning(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"default"}
	h.result("typo", 20, 0, 0, time.Second)

	code, stdout, stderr := h.run(t, "export", "--tenant", "typo")

	require.Equal(t, exitOK, code, stderr)
	assert.Equal(t, []string{"typo"}, h.exporter.calls)
	require.Len(t, lines(t, stdout), 1)
	assert.Contains(t, stderr, "typo")
	assert.Contains(t, stderr, "not listed")
}

// A tenant that is listed is exported without a warning.
func TestExport_AListedTenantHasNoWarning(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme"}
	h.result("acme", 1, 1, 1, time.Second)

	_, _, stderr := h.run(t, "export", "--tenant", "acme")

	assert.NotContains(t, stderr, "not listed")
}

// Failing to read the tenant list does not stop an export of a tenant the
// operator named: the warning is only a courtesy.
func TestExport_NamedTenantExportsEvenWhenTheTenantListFails(t *testing.T) {
	h := newHarness()
	h.tenants.err = errors.New("neo4j is down")
	h.result("acme", 1, 1, 1, time.Second)

	code, stdout, _ := h.run(t, "export", "--tenant", "acme")

	require.Equal(t, exitOK, code)
	assert.Len(t, lines(t, stdout), 1)
}

// A tenant id that cannot name a backup prefix is a usage error, before the
// command connects to anything.
func TestExport_TenantIDThatNamesNoPrefixIsUsage(t *testing.T) {
	for _, id := range []string{"", ".", "..", "a/b", "../x"} {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			h := newHarness()

			code, stdout, stderr := h.run(t, "export", "--tenant", id)

			require.Equal(t, exitUsage, code)
			assert.Empty(t, stdout)
			assert.NotEmpty(t, stderr)
			assert.Zero(t, h.connects)
			assert.Empty(t, h.exporter.calls)
		})
	}
}

// The command is cancelled by SIGINT/SIGTERM through its context: the context
// reaches every export, and no tenant after the cancelled one is started.
func TestExport_ACancelledContextStopsTheTenantLoop(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"a", "b", "c"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.exporter.onExport = func(ctx context.Context, tenantID string) {
		if tenantID == "a" {
			cancel()
			<-ctx.Done()
		}
	}
	h.exporter.errs["a"] = context.Canceled

	code, stdout, stderr := h.runCtx(t, ctx, "export")

	require.Equal(t, exitError, code)
	assert.Equal(t, []string{"a"}, h.exporter.calls)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "interrupted")
	assert.Equal(t, 1, h.closes)
}

func TestExport_ConnectFailureIsAnError(t *testing.T) {
	h := newHarness()
	h.connectErr = errors.New("missing or invalid env vars: NEO4J_URI")

	code, stdout, stderr := h.run(t, "export")

	require.Equal(t, exitError, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "NEO4J_URI")
}

// restore deletes the tenant's graph, so it refuses to run without --yes, or
// with --yes=false: exit 2, and nothing is connected to, let alone wiped.
func TestRestore_WithoutYesIsUsage(t *testing.T) {
	for name, args := range map[string][]string{
		"no --yes":    {"restore", acmeKey},
		"--yes=false": {"restore", acmeKey, "--yes=false"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness()

			code, stdout, stderr := h.run(t, args...)

			require.Equal(t, exitUsage, code)
			assert.Empty(t, stdout)
			assert.Empty(t, h.restorer.keys, "nothing is restored")
			assert.Zero(t, h.connects, "nothing is connected to")
			assert.Contains(t, stderr, "--yes")
			assert.Contains(t, stderr, "acme")
		})
	}
}

// A "--" ends the flags: what follows is positional, even an argument that
// looks like a flag, so "restore -- KEY --yes" names two keys and confirms
// nothing, while "restore --yes -- KEY" restores KEY.
func TestRestore_DoubleDashEndsTheFlags(t *testing.T) {
	for name, args := range map[string][]string{
		"--yes after --":         {"restore", "--", acmeKey, "--yes"},
		"--yes after key and --": {"restore", acmeKey, "--", "--yes"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness()

			code, stdout, _ := h.run(t, args...)

			require.Equal(t, exitUsage, code, "--yes after -- is a second key, not the confirmation")
			assert.Empty(t, stdout)
			assert.Empty(t, h.restorer.keys)
			assert.Zero(t, h.connects)
		})
	}

	h := newHarness()
	code, _, stderr := h.run(t, "restore", "--yes", "--", acmeKey)
	require.Equal(t, exitOK, code, stderr)
	assert.Equal(t, []string{acmeKey}, h.restorer.keys)
}

// The tenant a restore acts on is the one in the object key, and --yes may
// come before or after the key.
func TestRestore_TenantComesFromTheKey(t *testing.T) {
	for name, args := range map[string][]string{
		"yes after the key":  {"restore", acmeKey, "--yes"},
		"yes before the key": {"restore", "--yes", acmeKey},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness()
			h.restorer.res = backup.RestoreResult{Nodes: 12, Relationships: 34}

			code, stdout, stderr := h.run(t, args...)

			require.Equal(t, exitOK, code, stderr)
			assert.Equal(t, []string{acmeKey}, h.restorer.keys)
			got := lines(t, stdout)
			require.Len(t, got, 1)
			assert.Equal(t, map[string]any{
				"object_key":    acmeKey,
				"tenant_id":     "acme",
				"nodes":         float64(12),
				"relationships": float64(34),
			}, got[0])
			assert.Equal(t, 1, h.closes)
		})
	}
}

// A key that is not one the exporter writes is a usage error, before the
// command connects: another prefix, a URI, a partial name, a path that climbs.
func TestRestore_KeyOutsideTheBackupPrefixIsUsage(t *testing.T) {
	for _, key := range []string{
		"tenants/acme/topologies/rel-1/topology.json.gz",
		"code-bundles/rel-1/bundle.json",
		"s3://continuo/" + acmeKey,
		"/" + acmeKey,
		"tenants/acme/backups/catalog/20261030T020000Z.jsonl.gz.part",
		"tenants/acme/backups/catalog/latest.jsonl.gz",
		"tenants/acme/backups/catalog/../../other/20261030T020000Z.jsonl.gz",
		"tenants//backups/catalog/20261030T020000Z.jsonl.gz",
		"tenants/../backups/catalog/20261030T020000Z.jsonl.gz",
		"",
	} {
		t.Run(key, func(t *testing.T) {
			h := newHarness()

			code, stdout, stderr := h.run(t, "restore", key, "--yes")

			require.Equal(t, exitUsage, code)
			assert.Empty(t, stdout)
			assert.Empty(t, h.restorer.keys)
			assert.Zero(t, h.connects)
			assert.Contains(t, stderr, "tenants/<tenant>/backups/catalog/")
		})
	}
}

func TestRestore_NeedsExactlyOneKey(t *testing.T) {
	for name, args := range map[string][]string{
		"no key":   {"restore", "--yes"},
		"two keys": {"restore", acmeKey, acmeKey, "--yes"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness()

			code, stdout, _ := h.run(t, args...)

			require.Equal(t, exitUsage, code)
			assert.Empty(t, stdout)
			assert.Empty(t, h.restorer.keys)
			assert.Zero(t, h.connects)
		})
	}
}

// A key with no export stored at it is an error that says so and where to look
// for the keys that exist; the graph is untouched.
func TestRestore_MissingExportIsAnError(t *testing.T) {
	h := newHarness()
	h.restorer.err = fmt.Errorf("the export %s cannot be restored, and the graph of tenant \"acme\" is untouched: %w", acmeKey, ports.ErrBackupNotFound)

	code, stdout, stderr := h.run(t, "restore", acmeKey, "--yes")

	require.Equal(t, exitError, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, acmeKey)
	assert.Contains(t, stderr, "no export is stored")
	assert.Contains(t, stderr, "catalog-backup list --tenant acme")
	assert.Contains(t, stderr, "untouched")
}

// A restore that wiped the graph and did not finish loading leaves it partial:
// exit 1, and stderr says so and tells the operator to run the restore again.
func TestRestore_PartialRestoreSaysToRunItAgain(t *testing.T) {
	h := newHarness()
	h.restorer.err = fmt.Errorf("restore of tenant %q from %s: load the export: %w: %w", "acme", acmeKey, backup.ErrPartialRestore, errors.New("connection reset"))

	code, stdout, stderr := h.run(t, "restore", acmeKey, "--yes")

	require.Equal(t, exitError, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "partial")
	assert.Contains(t, stderr, "run the same restore again")
	assert.Contains(t, stderr, "connection reset")
	assert.Contains(t, stderr, "acme")
}

// A restore whose lease is held is skipped: exit 3.
func TestRestore_SkippedExitsThree(t *testing.T) {
	h := newHarness()
	h.restorer.err = fmt.Errorf("restore of tenant %q: %w", "acme", backup.ErrSkipped)

	code, stdout, stderr := h.run(t, "restore", acmeKey, "--yes")

	require.Equal(t, exitSkipped, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "lease")
}

func TestRestore_OtherFailureIsAnError(t *testing.T) {
	h := newHarness()
	h.restorer.err = errors.New("the export cannot be restored: bad checksum")

	code, stdout, stderr := h.run(t, "restore", acmeKey, "--yes")

	require.Equal(t, exitError, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "bad checksum")
	assert.NotContains(t, stderr, "run the same restore again", "a graph that may be untouched is not called partial")
}

func TestRestore_ConnectFailureIsAnError(t *testing.T) {
	h := newHarness()
	h.connectErr = errors.New("neo4j is down")

	code, stdout, stderr := h.run(t, "restore", acmeKey, "--yes")

	require.Equal(t, exitError, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "neo4j is down")
	assert.Empty(t, h.restorer.keys)
}

// list prints a JSON array of the stored exports, newest first, across
// tenants when no --tenant is named.
func TestList_PrintsNewestFirst(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme", "default"}
	at := func(s string) time.Time {
		ts, err := time.Parse(time.RFC3339, s)
		require.NoError(t, err)
		return ts
	}
	// The time each export was taken orders the list, not the time its
	// object was last modified.
	h.store.objects["acme"] = []ports.BackupObject{
		{Key: "tenants/acme/backups/catalog/20261029T020000Z.jsonl.gz", TenantID: "acme", TakenAt: at("2026-10-29T02:00:00Z"), Size: 300, LastModified: at("2026-10-31T02:00:05Z")},
		{Key: "tenants/acme/backups/catalog/20261027T020000Z.jsonl.gz", TenantID: "acme", TakenAt: at("2026-10-27T02:00:00Z"), Size: 100, LastModified: at("2026-10-27T02:00:04Z")},
	}
	h.store.objects["default"] = []ports.BackupObject{
		{Key: "tenants/default/backups/catalog/20261030T020000Z.jsonl.gz", TenantID: "default", TakenAt: at("2026-10-30T02:00:00Z"), Size: 400, LastModified: at("2026-10-30T02:00:06Z")},
		{Key: "tenants/default/backups/catalog/20261028T020000Z.jsonl.gz", TenantID: "default", TakenAt: at("2026-10-28T02:00:00Z"), Size: 200, LastModified: at("2026-10-28T02:00:03Z")},
	}

	code, stdout, stderr := h.run(t, "list")

	require.Equal(t, exitOK, code, stderr)
	var got []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
	require.Len(t, got, 4)
	var keys []any
	for _, e := range got {
		assert.Equal(t, []string{"key", "last_modified", "size"}, keysOf(e))
		keys = append(keys, e["key"])
	}
	assert.Equal(t, []any{
		"tenants/default/backups/catalog/20261030T020000Z.jsonl.gz",
		"tenants/acme/backups/catalog/20261029T020000Z.jsonl.gz",
		"tenants/default/backups/catalog/20261028T020000Z.jsonl.gz",
		"tenants/acme/backups/catalog/20261027T020000Z.jsonl.gz",
	}, keys)
	assert.Equal(t, float64(400), got[0]["size"])
	assert.Equal(t, "2026-10-30T02:00:06Z", got[0]["last_modified"])
	assert.Equal(t, 1, h.closes)
}

func TestList_TenantFlagListsThatTenantOnly(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme", "default"}
	h.store.objects["acme"] = []ports.BackupObject{{Key: acmeKey, Size: 1, LastModified: time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)}}
	h.store.objects["default"] = []ports.BackupObject{{Key: "tenants/default/backups/catalog/20261030T020000Z.jsonl.gz", Size: 2}}

	code, stdout, stderr := h.run(t, "list", "--tenant", "acme")

	require.Equal(t, exitOK, code, stderr)
	assert.Equal(t, []string{"acme"}, h.store.listed)
	var got []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &got))
	require.Len(t, got, 1)
	assert.Equal(t, acmeKey, got[0]["key"])
}

// With no export at all, list prints an empty array, not null.
func TestList_NothingStoredPrintsAnEmptyArray(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme"}

	code, stdout, _ := h.run(t, "list")

	require.Equal(t, exitOK, code)
	assert.JSONEq(t, `[]`, stdout)
}

// A bare list takes its tenants from the graph, which a Neo4j loss empties
// while the bucket still holds the exports. With no tenant listed it still
// prints an empty array and exits 0, and stderr says to name a tenant.
func TestList_BareListWithNoTenantWarnsToNameOne(t *testing.T) {
	h := newHarness()

	code, stdout, stderr := h.run(t, "list")

	require.Equal(t, exitOK, code, stderr)
	assert.JSONEq(t, `[]`, stdout)
	assert.Empty(t, h.store.listed, "no tenant, so nothing was listed")
	assert.Contains(t, stderr, "no tenant")
	assert.Contains(t, stderr, "--tenant <tenant>")
}

// The warning is for a bare list that found no tenant: a tenant that is named
// or listed has none, whether or not it holds an export.
func TestList_NoWarningWhenATenantIsNamedOrListed(t *testing.T) {
	for name, args := range map[string][]string{
		"named, nothing stored":  {"list", "--tenant", "acme"},
		"listed, nothing stored": {"list"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness()
			h.tenants.tenants = []string{"acme"}

			code, stdout, stderr := h.run(t, args...)

			require.Equal(t, exitOK, code, stderr)
			assert.JSONEq(t, `[]`, stdout)
			assert.NotContains(t, stderr, "no tenant")
		})
	}
}

// A tenant the catalog does not list simply has no exports.
func TestList_UnknownTenantHasNoExports(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme"}

	code, stdout, _ := h.run(t, "list", "--tenant", "nobody")

	require.Equal(t, exitOK, code)
	assert.JSONEq(t, `[]`, stdout)
}

// A tenant whose exports cannot be listed fails the whole listing, with
// nothing on stdout: a partial list would read as a complete one.
func TestList_FailureIsAnErrorWithNothingOnStdout(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme", "default"}
	h.store.objects["acme"] = []ports.BackupObject{{Key: acmeKey}}
	h.store.err = errors.New("the store is unreachable")

	code, stdout, stderr := h.run(t, "list")

	require.Equal(t, exitError, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "the store is unreachable")
}

func TestList_TenantIDThatNamesNoPrefixIsUsage(t *testing.T) {
	for _, id := range []string{"a/b", ".."} {
		t.Run(id, func(t *testing.T) {
			h := newHarness()

			code, stdout, _ := h.run(t, "list", "--tenant", id)

			require.Equal(t, exitUsage, code)
			assert.Empty(t, stdout)
			assert.Zero(t, h.connects)
		})
	}
}

// Usage errors exit 2 before the command connects to anything, and put the
// usage on stderr.
func TestUsageErrorsExitTwoBeforeConnecting(t *testing.T) {
	for name, args := range map[string][]string{
		"no arguments":          {},
		"unknown subcommand":    {"backup"},
		"unknown flag":          {"export", "--all"},
		"unexpected argument":   {"export", "acme"},
		"list unknown flag":     {"list", "--yes"},
		"restore with bad flag": {"restore", acmeKey, "--force"},
		"export takes no yes":   {"export", "--yes"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness()

			code, stdout, stderr := h.run(t, args...)

			require.Equal(t, exitUsage, code)
			assert.Empty(t, stdout)
			assert.Contains(t, stderr, "catalog-backup")
			assert.Zero(t, h.connects)
		})
	}
}

// -h prints the usage on stderr, never on stdout, and exits 0.
func TestHelpPrintsUsageOnStderr(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {"help"}, {"export", "-h"}, {"restore", "--help"}, {"list", "-h"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			h := newHarness()

			code, stdout, stderr := h.run(t, args...)

			require.Equal(t, exitOK, code)
			assert.Empty(t, stdout)
			assert.Contains(t, stderr, "catalog-backup")
			assert.Zero(t, h.connects)
		})
	}
}

// Diagnostics go to stderr through slog: the logger the command hands to its
// collaborators writes to stderr, never to stdout.
func TestDiagnosticsGoToStderr(t *testing.T) {
	h := newHarness()
	h.tenants.tenants = []string{"acme"}
	h.result("acme", 1, 1, 1, time.Second)

	_, stdout, stderr := h.run(t, "export")

	assert.Contains(t, stderr, "connected to the fakes")
	assert.NotContains(t, stdout, "connected to the fakes")
}

// stdout carries only JSON, in the success paths and in every error path:
// each line of an export is an object, list is one array, restore is one
// object, and a failing command prints nothing.
func TestStdoutIsOnlyJSON(t *testing.T) {
	scenarios := map[string]func(h *harness) (args []string, wantEmpty bool){
		"export ok": func(h *harness) ([]string, bool) {
			h.tenants.tenants = []string{"acme"}
			h.result("acme", 1, 2, 3, time.Second)
			return []string{"export"}, false
		},
		"export skipped": func(h *harness) ([]string, bool) {
			h.tenants.tenants = []string{"acme"}
			h.exporter.errs["acme"] = backup.ErrSkipped
			return []string{"export"}, true
		},
		"export failed": func(h *harness) ([]string, bool) {
			h.tenants.tenants = []string{"acme"}
			h.exporter.errs["acme"] = errors.New("boom")
			return []string{"export"}, true
		},
		"export no tenant": func(h *harness) ([]string, bool) { return []string{"export"}, true },
		"list ok": func(h *harness) ([]string, bool) {
			h.tenants.tenants = []string{"acme"}
			h.store.objects["acme"] = []ports.BackupObject{{Key: acmeKey, Size: 1}}
			return []string{"list"}, false
		},
		"list failed": func(h *harness) ([]string, bool) {
			h.tenants.tenants = []string{"acme"}
			h.store.err = errors.New("boom")
			return []string{"list"}, true
		},
		"restore ok": func(h *harness) ([]string, bool) { return []string{"restore", acmeKey, "--yes"}, false },
		"restore failed": func(h *harness) ([]string, bool) {
			h.restorer.err = errors.New("boom")
			return []string{"restore", acmeKey, "--yes"}, true
		},
		"restore partial": func(h *harness) ([]string, bool) {
			h.restorer.err = backup.ErrPartialRestore
			return []string{"restore", acmeKey, "--yes"}, true
		},
		"restore without yes": func(h *harness) ([]string, bool) { return []string{"restore", acmeKey}, true },
		"connect failed": func(h *harness) ([]string, bool) {
			h.connectErr = errors.New("boom")
			return []string{"list"}, true
		},
		"usage": func(h *harness) ([]string, bool) { return []string{"nonsense"}, true },
	}
	for name, setup := range scenarios {
		t.Run(name, func(t *testing.T) {
			h := newHarness()
			args, wantEmpty := setup(h)

			_, stdout, stderr := h.run(t, args...)

			if wantEmpty {
				assert.Empty(t, stdout)
			} else {
				require.NotEmpty(t, stdout)
			}
			dec := json.NewDecoder(strings.NewReader(stdout))
			for {
				var v any
				err := dec.Decode(&v)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "stdout is not only JSON: %q", stdout)
			}
			if wantEmpty {
				assert.NotEmpty(t, stderr, "a command that prints nothing says why on stderr")
			}
		})
	}
}

// The command needs NEO4J_* and the S3 settings and nothing else of catalog's
// configuration: no Redis, no ports, no backup schedule.
func TestLoadConfig_NeedsOnlyNeo4jAndS3(t *testing.T) {
	clearEnv(t)
	t.Setenv("NEO4J_URI", "bolt://neo4j:7687")
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "secret")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("S3_BUCKET", "continuo")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")

	cfg, err := loadConfig()

	require.NoError(t, err)
	assert.Equal(t, "bolt://neo4j:7687", cfg.neo4j.URI)
	assert.Equal(t, "neo4j", cfg.neo4j.User)
	assert.Equal(t, "secret", cfg.neo4j.Password)
	assert.Equal(t, "continuo", cfg.s3.Bucket)
	assert.Equal(t, 7, cfg.keep, "the chart's default keep applies when CATALOG_BACKUP_KEEP is unset")
}

// A hand-run export prunes with the install's keep, so it never deletes more
// than the scheduled export would.
func TestLoadConfig_KeepIsTheInstallsKeep(t *testing.T) {
	clearEnv(t)
	setRequired(t)
	t.Setenv("CATALOG_BACKUP_KEEP", "14")

	cfg, err := loadConfig()

	require.NoError(t, err)
	assert.Equal(t, 14, cfg.keep)
}

func TestLoadConfig_NamesEveryMissingKey(t *testing.T) {
	clearEnv(t)

	_, err := loadConfig()

	require.Error(t, err)
	for _, key := range []string{"NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD", "S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION"} {
		assert.Contains(t, err.Error(), key)
	}
	for _, key := range []string{"REDIS_HOST", "CATALOG_BACKUP_SCHEDULE", "GRPC_PORT", "POSTGRES_HOST"} {
		assert.NotContains(t, err.Error(), key)
	}
}

func TestLoadConfig_RefusesAKeepItCannotHonour(t *testing.T) {
	for _, keep := range []string{"0", "-3", "seven"} {
		t.Run(keep, func(t *testing.T) {
			clearEnv(t)
			setRequired(t)
			t.Setenv("CATALOG_BACKUP_KEEP", keep)

			_, err := loadConfig()

			require.Error(t, err)
			assert.Contains(t, err.Error(), "CATALOG_BACKUP_KEEP")
		})
	}
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD", "S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION",
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "CATALOG_BACKUP_KEEP", "CATALOG_BACKUP_SCHEDULE",
		"CATALOG_BACKUP_ENABLED", "REDIS_HOST", "GRPC_PORT",
	} {
		t.Setenv(key, "")
	}
}

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("NEO4J_URI", "bolt://neo4j:7687")
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "secret")
	t.Setenv("S3_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("S3_BUCKET", "continuo")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
}
