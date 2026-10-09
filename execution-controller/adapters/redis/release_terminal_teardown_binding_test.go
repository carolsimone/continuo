package redis_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	redis "github.com/carolsimone/continuo/execution-controller/adapters/redis"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// --- release.rejected:v1 teardown ---

func TestReleaseRejectedTeardownBinding_DropsCandidateSchema(t *testing.T) {
	var dropped string
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { dropped = schema; return nil })
	h := redis.NewReleaseRejectedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","reason":"seed_build_failed","candidate_schema":"_candidate_rel"}`,
	}}
	require.NoError(t, h(context.Background(), msg))
	require.Equal(t, "_candidate_rel", dropped)
}

func TestReleaseRejectedTeardownBinding_BestEffortOnCleanerError(t *testing.T) {
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { return errors.New("boom") })
	h := redis.NewReleaseRejectedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","candidate_schema":"_candidate_rel"}`,
	}}
	// best-effort: cleaner error is logged, message ACKed (nil)
	require.NoError(t, h(context.Background(), msg))
}

func TestReleaseRejectedTeardownBinding_NoSchemaIsNoop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewReleaseRejectedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{"payload": `{"release_id":"rel","reason":"seed_build_failed"}`}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called) // no candidate_schema => nothing to drop
}

func TestReleaseRejectedTeardownBinding_MissingPayloadIsNoop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewReleaseRejectedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called)
}

// --- release.promoted:v2 teardown ---

// releasePromotedFields renders one release.promoted:v2 entry naming
// candidateSchema ("" for an announced topology, which has none).
func releasePromotedFields(t *testing.T, candidateSchema string) map[string]any {
	t.Helper()
	at := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	fields, err := events.ReleasePromotedFields(events.DefaultTenantID, "release-controller", at, events.ReleasePromoted{
		ReleaseID:       "rel",
		PromotedAt:      at,
		PromotionSeq:    3,
		TopologyURI:     "s3://continuo/tenants/default/topologies/rel/topology.json.gz",
		TopologySHA256:  strings.Repeat("a", 64),
		ChangedNodeIDs:  []string{},
		CandidateSchema: candidateSchema,
	})
	require.NoError(t, err)
	return fields
}

func TestReleasePromotedTeardownBinding_DropsCandidateSchema(t *testing.T) {
	var dropped string
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { dropped = schema; return nil })
	h := redis.NewReleasePromotedTeardownBinding(cleaner, slog.Default())
	require.NoError(t, h(context.Background(), goredis.XMessage{ID: "1-0", Values: releasePromotedFields(t, "_candidate_rel")}))
	require.Equal(t, "_candidate_rel", dropped)
}

func TestReleasePromotedTeardownBinding_BestEffortOnCleanerError(t *testing.T) {
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { return errors.New("boom") })
	h := redis.NewReleasePromotedTeardownBinding(cleaner, slog.Default())
	// best-effort: cleaner error is logged, message ACKed (nil)
	require.NoError(t, h(context.Background(), goredis.XMessage{ID: "1-0", Values: releasePromotedFields(t, "_candidate_rel")}))
}

func TestReleasePromotedTeardownBinding_NoSchemaIsNoop(t *testing.T) {
	// An announced topology (announce-topology) carries no candidate schema.
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewReleasePromotedTeardownBinding(cleaner, slog.Default())
	require.NoError(t, h(context.Background(), goredis.XMessage{ID: "1-0", Values: releasePromotedFields(t, "")}))
	require.False(t, called)
}

func TestReleasePromotedTeardownBinding_UnreadableEntryIsAcknowledged(t *testing.T) {
	cases := map[string]map[string]any{
		"no fields": {},
		// A v1-shaped entry has a payload but no envelope: the binding reads the
		// v2 envelope only, so it drops nothing even though the payload names a schema.
		"v1 payload without envelope": {"payload": `{"release_id":"rel","topology":[],"candidate_schema":"_candidate_rel"}`},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			called := false
			cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
			h := redis.NewReleasePromotedTeardownBinding(cleaner, slog.Default())
			require.NoError(t, h(context.Background(), goredis.XMessage{ID: "1-0", Values: values}))
			require.False(t, called)
		})
	}
}
