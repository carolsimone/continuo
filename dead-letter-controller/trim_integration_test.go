//go:build integration

package main_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	dlpostgres "github.com/carolsimone/continuo/dead-letter-controller/adapters/postgres"
	dlredis "github.com/carolsimone/continuo/dead-letter-controller/adapters/redis"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/dead-letter-controller/service/trimmer"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const trimRetention = 72 * time.Hour

type nopTrimObserver struct{}

func (nopTrimObserver) Quarantined(string, string, int) {}
func (nopTrimObserver) Trimmed(string, int64)           {}
func (nopTrimObserver) TrimSucceeded(time.Time)         {}

// trimEnv is the real Redis and Postgres a trim loop test runs against, with a
// stream named after the test.
type trimEnv struct {
	rc     *goredis.Client
	db     *sqlx.DB
	stream string
}

func newTrimEnv(t *testing.T) *trimEnv {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set — skipping Redis integration test")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = rc.Close() })

	v := &pkgconfig.Validator{}
	cfg := pkgconfig.LoadPostgres(v)
	if len(v.Missing()) > 0 {
		testdeps.Unavailable(t, "POSTGRES_* not set — skipping Postgres integration test")
	}
	db, err := pkgdb.Open(context.Background(), cfg, pkgconfig.PoolConfig{MaxOpenConns: 4, MaxIdleConns: 2})
	if err != nil {
		testdeps.Unavailable(t, "Postgres unreachable: %v", err)
	}
	var exists bool
	if err := db.Get(&exists, `SELECT to_regclass('public.dead_letters') IS NOT NULL`); err != nil || !exists {
		t.Fatalf("dead_letters is missing: run the Flyway migrations (make test-deps-up)")
	}

	e := &trimEnv{rc: rc, db: db, stream: "test.trim." + strings.ToLower(t.Name()) + ":v1"}
	cleanup := func() {
		_ = rc.Del(context.Background(), e.stream).Err()
		_, _ = db.Exec(`DELETE FROM dead_letters WHERE stream = $1`, e.stream)
	}
	cleanup()
	t.Cleanup(func() { cleanup(); _ = db.Close() })
	return e
}

func (e *trimEnv) trimmer(groups []string, retired ...string) *trimmer.Trimmer {
	return trimmer.New(
		trimmer.Config{Retention: trimRetention, Groups: map[string][]string{e.stream: groups}, Retired: retired},
		dlredis.NewStreamInspector(e.rc), dlpostgres.NewDeadLetterRepository(e.db), dlpostgres.NewAdvisoryLock(e.db),
		ports.SystemClock{}, nopTrimObserver{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// recentID is an entry id from an hour ago, well inside the retention window.
func recentID() string { return fmt.Sprintf("%d-0", time.Now().Add(-time.Hour).UnixMilli()) }

// addEntries adds the old entries and then one recent one, so ids stay ascending.
func (e *trimEnv) addEntries(t *testing.T, old ...string) string {
	t.Helper()
	recent := recentID()
	for _, id := range append(old, recent) {
		require.NoError(t, e.rc.XAdd(context.Background(), &goredis.XAddArgs{
			Stream: e.stream, ID: id, Values: map[string]any{"k": "v" + id},
		}).Err())
	}
	return recent
}

func (e *trimEnv) group(t *testing.T, name, start string) {
	t.Helper()
	require.NoError(t, e.rc.XGroupCreate(context.Background(), e.stream, name, start).Err())
}

func (e *trimEnv) remaining(t *testing.T) []string {
	t.Helper()
	msgs, err := e.rc.XRange(context.Background(), e.stream, "-", "+").Result()
	require.NoError(t, err)
	var ids []string
	for _, m := range msgs {
		ids = append(ids, m.ID)
	}
	return ids
}

type storedRow struct {
	DedupKey string `db:"dedup_key"`
	Source   string `db:"source"`
	Kind     string `db:"failure_kind"`
	Group    string `db:"consumer_group"`
	Fields   string `db:"fields"`
}

func (e *trimEnv) rows(t *testing.T) []storedRow {
	t.Helper()
	var rows []storedRow
	require.NoError(t, e.db.Select(&rows, `SELECT dedup_key, source, failure_kind, consumer_group, fields::text AS fields
		FROM dead_letters WHERE stream = $1 ORDER BY dedup_key`, e.stream))
	return rows
}

func TestTrimLoop_LaggingGroupQuarantinedThenTrimmed(t *testing.T) {
	e := newTrimEnv(t)
	recent := e.addEntries(t, "10-0", "20-0")
	e.group(t, "fast", "$")
	e.group(t, "lag", "0")
	tr := e.trimmer([]string{"fast", "lag"})

	require.NoError(t, tr.RunOnce(context.Background()))

	rows := e.rows(t)
	require.Len(t, rows, 2)
	for i, id := range []string{"10-0", "20-0"} {
		assert.Equal(t, deadletter.QuarantineKey(e.stream, "lag", id), rows[i].DedupKey)
		assert.Equal(t, string(deadletter.SourceQuarantine), rows[i].Source)
		assert.Equal(t, string(model.DeadLetterKindTrimmed), rows[i].Kind)
		assert.Equal(t, "lag", rows[i].Group)
		assert.JSONEq(t, fmt.Sprintf(`{"k":"v%s"}`, id), rows[i].Fields)
	}
	assert.Equal(t, []string{recent}, e.remaining(t))

	require.NoError(t, tr.RunOnce(context.Background()))
	assert.Len(t, e.rows(t), 2, "a second run stores nothing more")
	assert.Equal(t, []string{recent}, e.remaining(t))
}

func TestTrimLoop_UnknownGroupIgnored(t *testing.T) {
	e := newTrimEnv(t)
	recent := e.addEntries(t, "10-0", "20-0")
	// "a" has consumed the ancient entries and nothing since, so it needs only
	// the recent one; "orphan" is at 0 and would need all three if it counted.
	e.group(t, "a", "20-0")
	e.group(t, "orphan", "0")

	require.NoError(t, e.trimmer([]string{"a"}).RunOnce(context.Background()))

	assert.Empty(t, e.rows(t), "an unlisted group's lag quarantines nothing")
	assert.Equal(t, []string{recent}, e.remaining(t), "and holds nothing back")
}

func TestTrimLoop_MissingGroupKeepsRecentEntries(t *testing.T) {
	e := newTrimEnv(t)
	recent := e.addEntries(t, "10-0")
	e.group(t, "a", "$")

	require.NoError(t, e.trimmer([]string{"a", "never"}).RunOnce(context.Background()))

	assert.Empty(t, e.rows(t))
	assert.Equal(t, []string{recent}, e.remaining(t), "age cap only: the recent entry stays for the group that has not started")
}

func TestTrimLoop_RetiredStreamDeleted(t *testing.T) {
	e := newTrimEnv(t)
	retired := e.stream + ".retired"
	t.Cleanup(func() { _ = e.rc.Del(context.Background(), retired).Err() })
	require.NoError(t, e.rc.XAdd(context.Background(), &goredis.XAddArgs{Stream: retired, ID: "10-0", Values: map[string]any{"k": "v"}}).Err())

	require.NoError(t, e.trimmer(nil, retired).RunOnce(context.Background()))

	n, err := e.rc.Exists(context.Background(), retired).Result()
	require.NoError(t, err)
	assert.Zero(t, n)
}
