package e2e

import (
	"context"
	"fmt"
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
// cannot recognise. A has a model of a schedule B drops, so applying the
// replay would swap the orchestrator's live graph and state's live release
// back to A, and give that schedule its model again: state would list it and
// start it. Neither may happen: A's promotion seq is below the live one.
func TestTopologyByReference_LatePromotionMovesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	t.Cleanup(func() { clients.close(ctx) })

	const staleSchedule = "e2e-stale-probe"
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
	// With B live in state, the stale schedule has no model and no cron.
	require.Empty(t, staleScheduleKnown(ctx, clients, staleSchedule, b), "B drops the %s schedule", staleSchedule)

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

	// Both groups that apply a promotion handle the replay before the live
	// pointers are read.
	for _, group := range []string{streams.OrchestratorReleasePromoted, streams.StateReleasePromoted} {
		awaitGroupSettled(t, ctx, clients, streams.ReleasePromotedV2, group, replayID)
		assert.Nil(t, findConsumerDeadLetter(ctx, clients, streams.ReleasePromotedV2, group, replayID, "open"),
			"a late promotion is a valid entry: group %s acknowledges it and never dead-letters it", group)
	}
	assert.Equal(t, livePointer{releaseID: b.ReleaseID, promotionSeq: b.PromotionSeq}, readLivePointer(ctx, clients),
		"a late promotion must not move the orchestrator's live pointer")
	window := time.NewTimer(negativeWindow)
	defer window.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		if known := staleScheduleKnown(ctx, clients, staleSchedule, b); known != "" {
			t.Fatalf("a late promotion must not move state's live release off %s or give %s a model again: %s",
				b.ReleaseID, staleSchedule, known)
		}
		select {
		case <-window.C:
			return
		case <-tick.C:
		}
	}
}

// staleScheduleKnown says how state still knows the schedule name, which the
// announcement want has no model of, and is empty when it does not: state's
// live release is another than want (by release id and promotion seq), a node
// of it carries name, or ListSchedules lists name. A failed read says so too.
func staleScheduleKnown(ctx context.Context, clients *testClients, name string, want announceResult) string {
	state, err := readStateLiveRelease(ctx, clients)
	if err != nil {
		return err.Error()
	}
	if state.releaseID != want.ReleaseID || state.promotionSeq != want.PromotionSeq || state.models[name] > 0 {
		return fmt.Sprintf("state's live release is %q at seq %d with %d nodes of %s; want %q at seq %d with none",
			state.releaseID, state.promotionSeq, state.models[name], name, want.ReleaseID, want.PromotionSeq)
	}
	listed, err := findListedSchedule(ctx, clients, name)
	if err != nil {
		return fmt.Sprintf("ListSchedules failed: %v", err)
	}
	if listed != nil {
		return fmt.Sprintf("ListSchedules lists %s: %v", name, listed)
	}
	return ""
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
	assert.Contains(t, dl.GetError(), announcedSHA, "the dead letter names the expected hash")
	assert.Contains(t, dl.GetError(), storedSHA, "the dead letter names the object's actual hash")
	assert.Equal(t, before, readLivePointer(ctx, clients), "a corrupt artifact must never move the live pointer")

	// state's group reads the same artifact and refuses it for the same reason.
	stateDL := awaitConsumerDeadLetter(ctx, t, clients, streams.ReleasePromotedV2, streams.StateReleasePromoted, id, "open")
	cleanup = append(cleanup, stateDL.GetId())
	assert.Equal(t, model.DeadLetterKindPermanent, model.DeadLetterKind(stateDL.GetFailureKind()))
	assert.Contains(t, strings.ToLower(stateDL.GetError()), "checksum")

	// The other two groups settle on the entry as well; whichever dead letter
	// the versions group records is removed with this test's.
	for _, g := range []string{streams.OrchestratorReleasePromotedVersions, streams.ExecutorReleasePromoted} {
		awaitGroupSettled(t, ctx, clients, streams.ReleasePromotedV2, g, id)
		if other := findConsumerDeadLetter(ctx, clients, streams.ReleasePromotedV2, g, id, "open"); other != nil {
			cleanup = append(cleanup, other.GetId())
		}
	}
}
