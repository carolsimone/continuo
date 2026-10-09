package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTopologyByReference_LatePromotionMovesNothing announces two topologies,
// A then B, and then replays A's release.promoted:v2 entry under a fresh event
// id and without its outbox entry id — a late delivery or a redrive that dedup
// cannot recognise. A declares a schedule B dropped, so applying the replay
// would both swap the live graph back to A and revive that schedule in state's
// catalog. Neither may happen: A's promotion seq is below the live one.
func TestTopologyByReference_LatePromotionMovesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	t.Cleanup(func() { clients.close(ctx) })

	const staleSchedule = "e2e-stale-probe"
	t.Cleanup(func() {
		_, _ = clients.stateDB.ExecContext(context.Background(),
			`DELETE FROM schedule_catalog WHERE schedule_name = $1`, staleSchedule)
	})
	tap := startStreamTap(t, ctx, clients.redisClient, streams.ReleasePromotedV2)

	nodes, schedules := e2eTopology(t, ctx, clients)
	probe := topologyartifact.Node{
		UniqueID:           seedSchemaName + ".stale_probe",
		SchemaName:         seedSchemaName,
		TableName:          "stale_probe",
		ResolvedRelationID: seedSchemaName + ".stale_probe",
		ServiceName:        "service-1",
		NodeType:           "dbt-model",
		ImageTag:           readServiceImageTag(t, ctx, clients, "service-1"),
		Schedule:           staleSchedule,
		UpstreamUniqueIDs:  []string{},
	}
	a := announceTopology(t, ctx, "e2e-late-a-"+uuid.NewString()[:8], append(append([]topologyartifact.Node{}, nodes...), probe))
	waitForAnnouncedTopology(t, ctx, clients, a, append(append([]string{}, schedules...), staleSchedule))
	b := announceTopology(t, ctx, "e2e-late-b-"+uuid.NewString()[:8], nodes)
	waitForAnnouncedTopology(t, ctx, clients, b, schedules)
	require.Greater(t, b.PromotionSeq, a.PromotionSeq)
	require.Eventually(t, func() bool { return !scheduleActive(ctx, clients, staleSchedule) },
		30*time.Second, time.Second, "B drops the %s schedule", staleSchedule)

	entry := findPromotionEntry(t, ctx, tap, a.ReleaseID)
	replay := map[string]any{}
	for k, v := range entry.Values {
		if k != "outbox_entry_id" {
			replay[k] = v
		}
	}
	replay["event_id"] = uuid.NewString()
	replayID, err := clients.redisClient.XAdd(ctx, &goredis.XAddArgs{Stream: streams.ReleasePromotedV2, Values: replay}).Result()
	require.NoError(t, err, "replay A's promotion")

	awaitGroupSettled(t, ctx, clients, streams.ReleasePromotedV2, streams.OrchestratorReleasePromoted, replayID)
	assert.Nil(t, findConsumerDeadLetter(ctx, clients, streams.ReleasePromotedV2, streams.OrchestratorReleasePromoted, replayID, "open"),
		"a late promotion is a valid entry: acknowledged, never dead-lettered")
	assert.Equal(t, livePointer{releaseID: b.ReleaseID, promotionSeq: b.PromotionSeq}, readLivePointer(ctx, clients),
		"a late promotion must not move the live pointer")
	require.Never(t, func() bool {
		return scheduleActive(ctx, clients, staleSchedule) || catalogPromotionSeq(ctx, clients) != b.PromotionSeq
	}, negativeWindow, time.Second, "a late promotion must not move state's schedule catalog")
}

// TestTopologyByReference_CorruptArtifactIsDeadLettered publishes a promotion
// whose artifact does not match its checksum. What sits at the key is a
// different, well-formed topology, so only the checksum stands between it and
// the live graph, and the entry's seq is far above the live one, so a reader
// that skipped the check would apply it. The orchestrator's topology group must
// dead-letter the entry as permanent and leave the live pointer where it was.
// The artifact is under a release id nothing has loaded, so no reader's cache
// can answer for it.
func TestTopologyByReference_CorruptArtifactIsDeadLettered(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	t.Cleanup(func() { clients.close(ctx) })

	seedTopology(t, ctx, clients)
	before := readLivePointer(ctx, clients)

	releaseID := "e2e-corrupt-" + uuid.NewString()[:8]
	nodes, _ := e2eTopology(t, ctx, clients)
	_, announcedSHA, err := topologyartifact.Encode(topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion, TenantID: events.DefaultTenantID, ReleaseID: releaseID, Nodes: nodes,
	})
	require.NoError(t, err)
	stored, storedSHA, err := topologyartifact.Encode(topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion, TenantID: events.DefaultTenantID, ReleaseID: releaseID,
		Nodes: append(append([]topologyartifact.Node{}, nodes...), topologyartifact.Node{
			UniqueID: seedSchemaName + ".corrupt_probe", SchemaName: seedSchemaName, TableName: "corrupt_probe",
			ServiceName: "service-1", NodeType: "dbt-model", Schedule: "e2e-corrupt-probe", UpstreamUniqueIDs: []string{},
		}),
	})
	require.NoError(t, err)
	require.NotEqual(t, announcedSHA, storedSHA)
	key := topologyartifact.Key(events.DefaultTenantID, releaseID)
	putS3Object(t, ctx, clients, key, stored)
	t.Cleanup(func() {
		_, _ = clients.s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(e2eS3Bucket), Key: aws.String(key),
		})
	})

	now := time.Now().UTC()
	fields, err := events.ReleasePromotedFields(events.DefaultTenantID, "e2e", now, events.ReleasePromoted{
		ReleaseID:      releaseID,
		PromotedAt:     now,
		PromotionSeq:   before.promotionSeq + 1000,
		TopologyURI:    "s3://" + e2eS3Bucket + "/" + key,
		TopologySHA256: announcedSHA,
		ChangedNodeIDs: []string{},
	})
	require.NoError(t, err)
	id, err := clients.redisClient.XAdd(ctx, &goredis.XAddArgs{Stream: streams.ReleasePromotedV2, Values: fields}).Result()
	require.NoError(t, err)

	dl := awaitConsumerDeadLetter(ctx, t, clients, streams.ReleasePromotedV2, streams.OrchestratorReleasePromoted, id, "open")
	cleanup := []string{dl.GetId()}
	t.Cleanup(func() { deleteDeadLetterRows(t, clients, cleanup...) })
	assert.Equal(t, model.DeadLetterKindPermanent, model.DeadLetterKind(dl.GetFailureKind()))
	assert.Contains(t, strings.ToLower(dl.GetError()), "checksum")
	assert.Equal(t, before, readLivePointer(ctx, clients), "a corrupt artifact must never move the live pointer")

	// The other two groups settle on the entry as well; whichever dead letter
	// the versions group records is removed with this test's.
	for _, g := range []string{streams.OrchestratorReleasePromotedVersions, streams.ExecutorReleasePromoted} {
		awaitGroupSettled(t, ctx, clients, streams.ReleasePromotedV2, g, id)
		if other := findConsumerDeadLetter(ctx, clients, streams.ReleasePromotedV2, g, id, "open"); other != nil {
			cleanup = append(cleanup, other.GetId())
		}
	}
}
