package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/lib/pq"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	// releaseControllerContainer is the compose container release-controller runs in.
	releaseControllerContainer = "release-controller"
	// announceTopologyBin is the announce-topology command the release-controller
	// dev image builds next to the service binary (release-controller/Dockerfile.dev).
	announceTopologyBin = "/app/release-controller/bin/announce-topology"
)

// announceResult is what announce-topology prints on stdout.
type announceResult struct {
	ReleaseID      string `json:"release_id"`
	PromotionSeq   int64  `json:"promotion_seq"`
	TopologyURI    string `json:"topology_uri"`
	TopologySHA256 string `json:"topology_sha256"`
}

// announceTopology publishes nodes as the topology of releaseID through
// release-controller's announce-topology: release-controller writes the artifact
// to MinIO, takes the next promotion seq and queues release.promoted:v2, which
// the orchestrator applies as it applies a promoted release. The command runs
// inside the release-controller container, which carries the service's
// Postgres and S3 settings.
func announceTopology(t *testing.T, ctx context.Context, releaseID string, nodes []topologyartifact.Node) announceResult {
	t.Helper()
	body, err := json.Marshal(nodes)
	require.NoError(t, err, "marshal the topology of %s", releaseID)
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", releaseControllerContainer, //nolint:gosec // fixed container and binary; releaseID is built by the test
		announceTopologyBin, "--release-id", releaseID, "--topology", "-")
	cmd.Stdin = bytes.NewReader(body)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run(), "announce-topology %s failed: %s", releaseID, stderr.String())
	var res announceResult
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &res), "announce-topology printed non-JSON stdout: %s", stdout.String())
	require.Equal(t, releaseID, res.ReleaseID)
	require.Positive(t, res.PromotionSeq, "every announcement takes a promotion seq")
	t.Logf("announced %s as promotion seq %d (%s)", res.ReleaseID, res.PromotionSeq, res.TopologyURI)
	return res
}

// livePointer is the orchestrator's live topology pointer, (:Meta {key:
// 'current_release'}): the release it holds and the promotion seq it was
// applied under (0 before any).
type livePointer struct {
	releaseID    string
	promotionSeq int64
}

// readLivePointer reads :Meta in one query, so the two values belong to the
// same swap. A read failure returns the zero pointer.
func readLivePointer(ctx context.Context, clients *testClients) livePointer {
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)
	res, err := session.Run(ctx,
		`MATCH (m:Meta {key: 'current_release'})
		 RETURN m.release_id AS release_id, coalesce(m.promotion_seq, 0) AS seq`, nil)
	if err != nil || !res.Next(ctx) {
		return livePointer{}
	}
	rel, _ := res.Record().Get("release_id")
	seq, _ := res.Record().Get("seq")
	var p livePointer
	p.releaseID, _ = rel.(string)
	p.promotionSeq, _ = seq.(int64)
	return p
}

// catalogPromotionSeq is the promotion seq state's schedule catalog last
// applied, or -1 when it cannot be read.
func catalogPromotionSeq(ctx context.Context, clients *testClients) int64 {
	var seq int64
	if err := clients.stateDB.QueryRowContext(ctx,
		`SELECT promotion_seq FROM schedule_catalog_state WHERE id = TRUE`).Scan(&seq); err != nil {
		return -1
	}
	return seq
}

// scheduleActive reports whether state's schedule catalog lists name as active.
func scheduleActive(ctx context.Context, clients *testClients, name string) bool {
	var active bool
	err := clients.stateDB.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schedule_catalog WHERE schedule_name = $1 AND removed_at IS NULL)`,
		name).Scan(&active)
	return err == nil && active
}

// waitForAnnouncedTopology waits until the orchestrator has applied the
// announcement (:Meta names its release, at its seq or later) and state's
// schedule catalog has reconciled to it: the catalog's applied seq reached the
// announcement's and every schedule the topology declares is active.
func waitForAnnouncedTopology(t *testing.T, ctx context.Context, clients *testClients, res announceResult, schedules []string) {
	t.Helper()
	pollUntil(t, ctx, 60*time.Second, time.Second, func() (bool, error) {
		live := readLivePointer(ctx, clients)
		if live.releaseID != res.ReleaseID || live.promotionSeq < res.PromotionSeq {
			return false, nil
		}
		if catalogPromotionSeq(ctx, clients) < res.PromotionSeq {
			return false, nil
		}
		var active int
		if err := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schedule_catalog WHERE removed_at IS NULL AND schedule_name = ANY($1)`,
			pq.Array(schedules)).Scan(&active); err != nil {
			return false, nil
		}
		return active == len(schedules), nil
	}, fmt.Sprintf("timeout waiting for announced topology %s (promotion seq %d) to be applied", res.ReleaseID, res.PromotionSeq))
}

// entryFields returns a stream entry's string-valued fields, the form the
// pkg/events decoders read.
func entryFields(msg goredis.XMessage) map[string]string {
	out := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// findPromotionEntry returns the release.promoted:v2 entry the tap recorded for
// releaseID.
func findPromotionEntry(t *testing.T, ctx context.Context, tap *streamTap, releaseID string) goredis.XMessage {
	t.Helper()
	var found goredis.XMessage
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		for _, msg := range tap.Entries() {
			if _, p, err := events.DecodeReleasePromoted(entryFields(msg)); err == nil && p.ReleaseID == releaseID {
				found = msg
				return true, nil
			}
		}
		return false, nil
	}, "no release.promoted:v2 entry recorded for "+releaseID)
	return found
}

// awaitGroupSettled waits until group has been delivered stream entry id and
// holds it pending no longer: acknowledged after its handler ran, or after its
// dead letter was written.
func awaitGroupSettled(t *testing.T, ctx context.Context, clients *testClients, stream, group, id string) {
	t.Helper()
	require.Eventually(t, func() bool {
		groups, err := clients.redisClient.XInfoGroups(ctx, stream).Result()
		if err != nil {
			return false
		}
		delivered := false
		for _, g := range groups {
			if g.Name == group && !streamIDBefore(g.LastDeliveredID, id) {
				delivered = true
			}
		}
		if !delivered {
			return false
		}
		pending, err := clients.redisClient.XPendingExt(ctx, &goredis.XPendingExtArgs{
			Stream: stream, Group: group, Start: id, End: id, Count: 1,
		}).Result()
		return err == nil && len(pending) == 0
	}, deadLetterAwait, time.Second, "group %s never settled on %s entry %s", group, stream, id)
}

// streamIDBefore reports whether Redis stream id a ("<ms>-<seq>") sorts before b.
func streamIDBefore(a, b string) bool {
	am, as := splitStreamID(a)
	bm, bs := splitStreamID(b)
	if am != bm {
		return am < bm
	}
	return as < bs
}

func splitStreamID(id string) (uint64, uint64) {
	ms, seq, _ := strings.Cut(id, "-")
	m, _ := strconv.ParseUint(ms, 10, 64)
	s, _ := strconv.ParseUint(seq, 10, 64)
	return m, s
}
