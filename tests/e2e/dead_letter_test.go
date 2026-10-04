package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeadLetter_MalformedMessagesAreDeadLetteredNotDropped publishes a message
// each consumer can never parse and checks that the consumer writes it to
// consumer.dead_letter:v1 and only then removes it from its pending list.
func TestDeadLetter_MalformedMessagesAreDeadLetteredNotDropped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)

	cases := []struct{ producer, stream, group string }{
		{"state", streams.TaskStatusUpdatedV1, streams.StateTaskStatusUpdated},
		{"topology-controller", streams.ReleaseRequestedV1, streams.TopologyControllerReleaseRequested},
	}
	for _, tc := range cases {
		t.Run(tc.producer, func(t *testing.T) {
			id, err := clients.redisClient.XAdd(ctx, &goredis.XAddArgs{
				Stream: tc.stream, Values: map[string]any{"e2e_dead_letter_probe": t.Name()},
			}).Result()
			require.NoError(t, err)

			var entry goredis.XMessage
			require.Eventually(t, func() bool {
				var ok bool
				entry, ok = findDeadLetter(ctx, clients.redisClient, tc.group, id)
				return ok
			}, 2*time.Minute, time.Second, "no dead letter for %s on %s", id, tc.stream)
			t.Cleanup(func() { clients.redisClient.XDel(context.Background(), streams.ConsumerDeadLetterV1, entry.ID) })

			assert.Equal(t, tc.producer, entry.Values["producer"])
			var p events.ConsumerDeadLetter
			require.NoError(t, json.Unmarshal([]byte(entry.Values["payload"].(string)), &p))
			assert.Equal(t, model.DeadLetterKindPermanent, p.FailureKind)
			assert.Equal(t, tc.stream, p.OriginalStream)

			require.Eventually(t, func() bool {
				pending, err := clients.redisClient.XPendingExt(ctx, &goredis.XPendingExtArgs{
					Stream: tc.stream, Group: tc.group, Start: id, End: id, Count: 1,
				}).Result()
				return err == nil && len(pending) == 0
			}, 30*time.Second, time.Second, "the original must be acknowledged once its dead letter exists")
		})
	}
}

// findDeadLetter returns the newest consumer.dead_letter:v1 entry written for
// the message messageID that group gave up on.
func findDeadLetter(ctx context.Context, rc *goredis.Client, group, messageID string) (goredis.XMessage, bool) {
	entries, err := rc.XRevRangeN(ctx, streams.ConsumerDeadLetterV1, "+", "-", 500).Result()
	if err != nil {
		return goredis.XMessage{}, false
	}
	for _, e := range entries {
		raw, _ := e.Values["payload"].(string)
		var p events.ConsumerDeadLetter
		if json.Unmarshal([]byte(raw), &p) == nil && p.OriginalGroup == group && p.OriginalMessageID == messageID {
			return e, true
		}
	}
	return goredis.XMessage{}, false
}
