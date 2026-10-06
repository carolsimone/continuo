//go:build integration

package redis

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/testdeps"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inspectorStream returns a Redis client and a stream named after the test, and
// deletes the stream when the test ends.
func inspectorStream(t *testing.T) (*goredis.Client, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set — skipping Redis integration test")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = rc.Close() })
	stream := "test.trim." + strings.ToLower(t.Name()) + ":v1"
	t.Cleanup(func() { _ = rc.Del(context.Background(), stream).Err() })
	require.NoError(t, rc.Del(context.Background(), stream).Err())
	return rc, stream
}

func xadd(t *testing.T, rc *goredis.Client, stream string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		require.NoError(t, rc.XAdd(context.Background(), &goredis.XAddArgs{
			Stream: stream, ID: id, Values: map[string]any{"k": "v" + id},
		}).Err())
	}
}

func ids(entries []trim.Entry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.ID.String())
	}
	return out
}

func TestSnapshot_ContractGroupsOnly(t *testing.T) {
	rc, stream := inspectorStream(t)
	ctx := context.Background()
	xadd(t, rc, stream, "10-0", "20-0", "30-0")
	require.NoError(t, rc.XGroupCreate(ctx, stream, "a", "20-0").Err())
	require.NoError(t, rc.XGroupCreate(ctx, stream, "orphan", "0").Err())

	snap, unknown, exists, err := NewStreamInspector(rc).Snapshot(ctx, stream, []string{"a", "never"})
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []trim.Group{{Name: "a", LastDelivered: trim.StreamID{Ms: 20}}}, snap.Present)
	assert.Nil(t, snap.Present[0].OldestPending)
	assert.Equal(t, []string{"never"}, snap.Missing)
	assert.Equal(t, []string{"orphan"}, unknown)
}

func TestSnapshot_OldestPending(t *testing.T) {
	rc, stream := inspectorStream(t)
	ctx := context.Background()
	xadd(t, rc, stream, "10-0", "20-0")
	require.NoError(t, rc.XGroupCreate(ctx, stream, "a", "0").Err())
	require.NoError(t, rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: "a", Consumer: "c", Streams: []string{stream, ">"}, Count: 2,
	}).Err())

	snap, _, exists, err := NewStreamInspector(rc).Snapshot(ctx, stream, []string{"a"})
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, snap.Present, 1)
	assert.Equal(t, trim.StreamID{Ms: 20}, snap.Present[0].LastDelivered)
	require.NotNil(t, snap.Present[0].OldestPending)
	assert.Equal(t, trim.StreamID{Ms: 10}, *snap.Present[0].OldestPending)
}

func TestSnapshot_MissingStream(t *testing.T) {
	rc, stream := inspectorStream(t)
	_, _, exists, err := NewStreamInspector(rc).Snapshot(context.Background(), stream, []string{"a"})
	require.NoError(t, err)
	assert.False(t, exists)
}

// pendingFixture adds 10-0 … 40-0 and leaves 10-0 and 20-0 delivered to group
// "a", with 20-0 acknowledged: 10-0 is pending, 30-0 and 40-0 are undelivered.
func pendingFixture(t *testing.T, rc *goredis.Client, stream string) {
	t.Helper()
	ctx := context.Background()
	xadd(t, rc, stream, "10-0", "20-0", "30-0", "40-0")
	require.NoError(t, rc.XGroupCreate(ctx, stream, "a", "0").Err())
	require.NoError(t, rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: "a", Consumer: "c", Streams: []string{stream, ">"}, Count: 2,
	}).Err())
	require.NoError(t, rc.XAck(ctx, stream, "a", "20-0").Err())
}

func TestNeededEntries_PendingAndUndeliveredBelowCutoff(t *testing.T) {
	rc, stream := inspectorStream(t)
	pendingFixture(t, rc, stream)

	got, err := NewStreamInspector(rc).NeededEntries(context.Background(), stream, "a",
		trim.StreamID{Ms: 20}, trim.StreamID{Ms: 35}, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"10-0", "30-0"}, ids(got))
	assert.Equal(t, map[string]string{"k": "v10-0"}, got[0].Fields)
	assert.Equal(t, map[string]string{"k": "v30-0"}, got[1].Fields)
}

func TestNeededEntries_SkipsPendingAlreadyTrimmed(t *testing.T) {
	rc, stream := inspectorStream(t)
	pendingFixture(t, rc, stream)
	require.NoError(t, rc.XTrimMinID(context.Background(), stream, "15-0").Err())

	got, err := NewStreamInspector(rc).NeededEntries(context.Background(), stream, "a",
		trim.StreamID{Ms: 20}, trim.StreamID{Ms: 35}, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"30-0"}, ids(got))
}

// The limit counts existing entries: pending ids whose entry is gone must not
// eat into it, or the caller would believe it had seen every needed entry.
func TestNeededEntries_LimitCountsExistingPendingEntries(t *testing.T) {
	rc, stream := inspectorStream(t)
	ctx := context.Background()
	xadd(t, rc, stream, "10-0", "20-0", "30-0", "40-0")
	require.NoError(t, rc.XGroupCreate(ctx, stream, "a", "0").Err())
	require.NoError(t, rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: "a", Consumer: "c", Streams: []string{stream, ">"}, Count: 4,
	}).Err())
	require.NoError(t, rc.XTrimMinID(ctx, stream, "25-0").Err()) // 10-0 and 20-0 are gone but still pending

	got, err := NewStreamInspector(rc).NeededEntries(ctx, stream, "a", trim.StreamID{Ms: 40}, trim.StreamID{Ms: 100}, 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"30-0"}, ids(got))
}

func TestTrimBefore_RemovesOnlyBelow(t *testing.T) {
	rc, stream := inspectorStream(t)
	ctx := context.Background()
	xadd(t, rc, stream, "10-0", "20-0", "30-0", "40-0", "50-0")
	before, err := rc.XRange(ctx, stream, "-", "+").Result()
	require.NoError(t, err)

	n, err := NewStreamInspector(rc).TrimBefore(ctx, stream, trim.StreamID{Ms: 30})
	require.NoError(t, err)

	after, err := rc.XRange(ctx, stream, "-", "+").Result()
	require.NoError(t, err)
	remaining := map[string]bool{}
	for _, m := range after {
		remaining[m.ID] = true
	}
	for _, keep := range []string{"30-0", "40-0", "50-0"} {
		assert.True(t, remaining[keep], "%s must survive", keep)
	}
	assert.EqualValues(t, len(before)-len(after), n)
	assert.EqualValues(t, 2, n)
}

func TestDeleteIfExists(t *testing.T) {
	rc, stream := inspectorStream(t)
	ctx := context.Background()
	xadd(t, rc, stream, "10-0")
	ins := NewStreamInspector(rc)

	deleted, err := ins.DeleteIfExists(ctx, stream)
	require.NoError(t, err)
	assert.True(t, deleted)
	deleted, err = ins.DeleteIfExists(ctx, stream)
	require.NoError(t, err)
	assert.False(t, deleted)
}

// A redriven entry addressed to another group is skipped by this group when it
// consumes, so it is not this group's to quarantine: a later redrive would
// retarget it and hand this group an event meant for the other.
func TestNeededEntries_SkipsEntriesTargetedToAnotherGroup(t *testing.T) {
	rc, stream := inspectorStream(t)
	ctx := context.Background()
	add := func(id string, extra ...any) {
		values := map[string]any{"k": "v" + id}
		for i := 0; i < len(extra); i += 2 {
			values[extra[i].(string)] = extra[i+1]
		}
		require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, ID: id, Values: values}).Err())
	}
	add("10-0")
	add("20-0", pkgredis.RedriveGroupField, "other")
	add("30-0", pkgredis.RedriveGroupField, "mine")
	require.NoError(t, rc.XGroupCreate(ctx, stream, "mine", "0").Err())

	got, err := NewStreamInspector(rc).NeededEntries(ctx, stream, "mine", trim.StreamID{}, trim.StreamID{Ms: 100}, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"10-0", "30-0"}, ids(got))
}

// Entries addressed to another group must not use up the limit: with the first
// raw window made only of them, the needed entries beyond it are still returned,
// so a caller that sees limit entries knows there are more and never trims past
// one it has not stored.
func TestNeededEntries_PagesPastEntriesTargetedToAnotherGroup(t *testing.T) {
	rc, stream := inspectorStream(t)
	ctx := context.Background()
	add := func(id string, extra ...any) {
		values := map[string]any{"k": "v" + id}
		for i := 0; i < len(extra); i += 2 {
			values[extra[i].(string)] = extra[i+1]
		}
		require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, ID: id, Values: values}).Err())
	}
	add("10-0", pkgredis.RedriveGroupField, "other")
	add("20-0", pkgredis.RedriveGroupField, "other")
	add("30-0")
	add("40-0", pkgredis.RedriveGroupField, "mine")
	add("50-0")
	require.NoError(t, rc.XGroupCreate(ctx, stream, "mine", "0").Err())
	insp := NewStreamInspector(rc)

	got, err := insp.NeededEntries(ctx, stream, "mine", trim.StreamID{}, trim.StreamID{Ms: 100}, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"30-0", "40-0"}, ids(got), "the limit counts kept entries, so a full result is returned")

	got, err = insp.NeededEntries(ctx, stream, "mine", trim.StreamID{}, trim.StreamID{Ms: 100}, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"30-0", "40-0", "50-0"}, ids(got))
}
