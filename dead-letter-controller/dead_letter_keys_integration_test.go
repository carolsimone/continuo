//go:build integration

package main_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	dlpostgres "github.com/carolsimone/continuo/dead-letter-controller/adapters/postgres"
	dlredis "github.com/carolsimone/continuo/dead-letter-controller/adapters/redis"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/service/handlers"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sharedEntryID is the entry ID of the test entries. An entry of a stream's
// unprefixed key and one of its prefixed key, each numbered by its own key, can
// share it.
const sharedEntryID = "1700000000000-0"

type nopObserver struct{}

func (nopObserver) Recorded(deadletter.DeadLetter) {}
func (nopObserver) Redriven(deadletter.DeadLetter) {}
func (nopObserver) Expired(deadletter.DeadLetter)  {}

// keysEnv is a stream named after the test, with the real Redis and Postgres
// dead letters are stored in.
type keysEnv struct {
	rc     *goredis.Client
	db     *sqlx.DB
	stream string
	group  string
}

func newKeysEnv(t *testing.T) *keysEnv {
	t.Helper()
	rc, db := integrationDeps(t)
	e := &keysEnv{rc: rc, db: db, stream: "test.dlkeys." + strings.ToLower(t.Name()) + ":v1", group: "dlkeys-test"}
	ctx := context.Background()
	cleanup := func() {
		_ = rc.Del(ctx, pkgredis.Key(e.stream)).Err()
		_, _ = db.Exec(`DELETE FROM dead_letters WHERE stream = $1`, e.stream)
	}
	cleanup()
	t.Cleanup(cleanup)
	return e
}

// consume runs a stream consumer of the test stream with handle until the test ends.
func (e *keysEnv) consume(t *testing.T, handle pkgredis.MessageHandler, opts ...pkgredis.ConsumerOption) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := pkgredis.NewStreamConsumer(e.rc, e.stream, e.group, handle, discardLogger(), opts...)
	c.SetService("dead-letter-controller-test")
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
	})
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// ensureDeadLetterStream creates the prefixed consumer dead-letter stream when
// it is missing, as the streams-init step does in a deployment, since a
// consumer never creates it.
func ensureDeadLetterStream(t *testing.T, rc *goredis.Client) {
	t.Helper()
	ctx := context.Background()
	key := pkgredis.Key(streams.ConsumerDeadLetterV1)
	if rc.Exists(ctx, key).Val() != 0 {
		return
	}
	const probe = "dead-letter-controller-test-probe"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, key, probe, "$").Err())
	require.NoError(t, rc.XGroupDestroy(ctx, key, probe).Err())
}

// deadLettersOf returns the consumer.dead_letter:v1 entries whose payload names stream.
func deadLettersOf(rc *goredis.Client, stream string) ([]goredis.XMessage, error) {
	all, err := rc.XRange(context.Background(), pkgredis.Key(streams.ConsumerDeadLetterV1), "-", "+").Result()
	if err != nil {
		return nil, err
	}
	var out []goredis.XMessage
	for _, m := range all {
		var p events.ConsumerDeadLetter
		payload, _ := m.Values["payload"].(string)
		if json.Unmarshal([]byte(payload), &p) == nil && p.OriginalStream == stream {
			out = append(out, m)
		}
	}
	return out, nil
}

type keyedRow struct {
	DedupKey          string `db:"dedup_key"`
	OriginalMessageID string `db:"original_message_id"`
	Fields            string `db:"fields"`
}

// rowsWhere returns the stored dead letters that match where, whose one
// parameter is arg.
func rowsWhere(db *sqlx.DB, where string, arg any) ([]keyedRow, error) {
	var rows []keyedRow
	err := db.Select(&rows, `SELECT dedup_key, original_message_id, fields::text AS fields
		FROM dead_letters WHERE `+where+` ORDER BY dedup_key`, arg)
	return rows, err
}

// The dead-letter stream can hold dead letters for an entry of a stream's
// unprefixed key and one of its prefixed key with the same entry ID, written by
// consumers that read both keys. They are two records.
func TestDeadLettersOfBothKeysOfAStreamWithOneEntryIDAreStoredApart(t *testing.T) {
	e := newKeysEnv(t)
	ensureDeadLetterStream(t, e.rc)
	for _, key := range []string{e.stream, pkgredis.Key(e.stream)} {
		fields, err := events.ConsumerDeadLetterFields(events.DeadLetteredMessage{
			Producer: "dead-letter-controller-test", OccurredAt: time.Now().UTC(), Stream: e.stream, Group: e.group,
			MessageID: sharedEntryID, Key: key, Fields: map[string]string{"from": key},
			FailureKind: model.DeadLetterKindPermanent, Error: "every entry fails", DeliveryCount: 1,
		})
		require.NoError(t, err)
		require.NoError(t, e.rc.XAdd(context.Background(), &goredis.XAddArgs{Stream: pkgredis.Key(streams.ConsumerDeadLetterV1), Values: fields}).Err())
	}
	dls, err := deadLettersOf(e.rc, e.stream)
	require.NoError(t, err)
	require.Len(t, dls, 2)
	t.Cleanup(func() {
		for _, m := range dls {
			_ = e.rc.XDel(context.Background(), pkgredis.Key(streams.ConsumerDeadLetterV1), m.ID).Err()
		}
	})

	binding := dlredis.NewConsumerDeadLetterBinding(
		handlers.NewRecorder(dlpostgres.NewDeadLetterRepository(e.db), nopObserver{}, discardLogger()), ports.SystemClock{})
	for _, m := range dls {
		require.NoError(t, binding.Handle(context.Background(), m))
	}

	rows, err := rowsWhere(e.db, "stream = $1", e.stream)
	require.NoError(t, err)
	require.Len(t, rows, 2, "a dead letter from each key is its own record")
	byFrom := map[string]keyedRow{}
	for _, r := range rows {
		assert.Equal(t, sharedEntryID, r.OriginalMessageID, "the record names the entry by its own ID")
		var fields map[string]string
		require.NoError(t, json.Unmarshal([]byte(r.Fields), &fields))
		byFrom[fields["from"]] = r
	}
	assert.Equal(t, deadletter.ConsumerKey(e.stream, e.group, sharedEntryID), byFrom[e.stream].DedupKey,
		"an entry of the unprefixed key is stored under its own entry ID")
	assert.Equal(t, deadletter.ConsumerKey(e.stream, e.group, sharedEntryID+"@"+pkgredis.Key(e.stream)),
		byFrom[pkgredis.Key(e.stream)].DedupKey)

	for _, m := range dls {
		require.NoError(t, binding.Handle(context.Background(), m))
	}
	rows, err = rowsWhere(e.db, "stream = $1", e.stream)
	require.NoError(t, err)
	assert.Len(t, rows, 2, "storing the same dead letters again adds nothing")
}

// An unreadable entry of a dead-letter stream is stored under its entry ID
// qualified by the key it was read from.
func TestAnUnreadableDeadLetterIsStoredUnderItsKeyQualifiedID(t *testing.T) {
	e := newKeysEnv(t)
	require.NoError(t, e.rc.XGroupCreateMkStream(context.Background(), pkgredis.Key(e.stream), e.group, "0").Err())
	// The binding records an unreadable entry under the dead-letter stream's
	// name whatever key it was read from, so the rows are found by dedup key.
	prefix := deadletter.UnreadableKey(streams.ConsumerDeadLetterV1, sharedEntryID)
	cleanup := func() { _, _ = e.db.Exec(`DELETE FROM dead_letters WHERE dedup_key LIKE $1`, prefix+"%") }
	cleanup()
	t.Cleanup(cleanup)

	recorder := handlers.NewRecorder(dlpostgres.NewDeadLetterRepository(e.db), nopObserver{}, discardLogger())
	e.consume(t, dlredis.NewConsumerDeadLetterBinding(recorder, ports.SystemClock{}).Handle, pkgredis.WithoutDeadLetters())
	require.NoError(t, e.rc.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: pkgredis.Key(e.stream), ID: sharedEntryID, Values: map[string]any{"garbage": "1"},
	}).Err())

	var rows []keyedRow
	assert.Eventually(t, func() bool {
		var err error
		rows, err = rowsWhere(e.db, "dedup_key LIKE $1", prefix+"%")
		return err == nil && len(rows) == 1
	}, 10*time.Second, 20*time.Millisecond, "the unreadable entry is stored")
	require.Len(t, rows, 1)
	assert.Equal(t, prefix+"@"+pkgredis.Key(e.stream), rows[0].DedupKey)
}
