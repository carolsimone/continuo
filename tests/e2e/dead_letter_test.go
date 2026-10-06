package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	deadletterv1 "github.com/carolsimone/continuo/dead-letter-controller/api/deadletter/v1"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

const (
	// sourceConsumer is the dead-letter source of a message a stream consumer
	// gave up on.
	sourceConsumer = "consumer"
	// deadLetterAwait bounds how long a consumer takes to dead-letter a message
	// and dead-letter-controller to store it.
	deadLetterAwait = 60 * time.Second
	// negativeWindow is how long an absence (no new dead letter, no second
	// publish) is observed before it is believed.
	negativeWindow = 10 * time.Second
)

// TestDeadLetter_MalformedMessagesAreDeadLetteredNotDropped publishes a message
// each consumer can never parse and checks that the consumer dead-letters it,
// that dead-letter-controller stores it and serves it through ListDeadLetters,
// and that the consumer acknowledges the original only after that.
func TestDeadLetter_MalformedMessagesAreDeadLetteredNotDropped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	t.Cleanup(func() { clients.close(ctx) })

	cases := []struct{ producer, stream, group string }{
		{"state", streams.TaskStatusUpdatedV1, streams.StateTaskStatusUpdated},
		{"topology-controller", streams.ReleaseRequestedV1, streams.TopologyControllerReleaseRequested},
	}
	for _, tc := range cases {
		t.Run(tc.producer, func(t *testing.T) {
			id := publishMalformed(ctx, t, clients, tc.stream)
			dl := awaitConsumerDeadLetter(ctx, t, clients, tc.stream, tc.group, id, "open")
			t.Cleanup(func() { deleteDeadLetterRows(t, clients, dl.GetId()) })

			assert.Equal(t, tc.producer, dl.GetProducer())
			assert.Equal(t, model.DeadLetterKindPermanent, model.DeadLetterKind(dl.GetFailureKind()))
			assert.Equal(t, tc.stream, dl.GetStream())
			assert.Equal(t, "open", dl.GetStatus())

			require.Eventually(t, func() bool {
				pending, err := clients.redisClient.XPendingExt(ctx, &goredis.XPendingExtArgs{
					Stream: tc.stream, Group: tc.group, Start: id, End: id, Count: 1,
				}).Result()
				return err == nil && len(pending) == 0
			}, 30*time.Second, time.Second, "the original must be acknowledged once its dead letter exists")
		})
	}
}

// TestDeadLetter_RedriveReachesOnlyItsGroup dead-letters a message on a stream
// with several consumer groups, redrives it, and checks the redriven entry is
// acknowledged by the other groups without reprocessing and handled by the
// group that failed it.
func TestDeadLetter_RedriveReachesOnlyItsGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	t.Cleanup(func() { clients.close(ctx) })

	// release.promoted:v1 has three consumer groups. Both orchestrator groups
	// reject an entry with no payload as a permanent failure (their bindings
	// return events.ErrPermanent from ParseReleasePromoted), so each dead-letters
	// the original. group is the one the redrive addresses; the others must
	// acknowledge the redriven entry without a handler.
	const stream = streams.ReleasePromotedV1
	const group = streams.OrchestratorReleasePromoted
	otherGroups := []string{streams.OrchestratorReleasePromotedVersions, streams.ExecutorReleasePromoted}

	// 1-2. Publish the malformed entry and wait for the dead letter of the group
	// that will be redriven. The sibling orchestrator group dead-letters the same
	// original; waiting for it too means its late arrival cannot be mistaken for
	// a result of the redrive.
	origID := publishMalformed(ctx, t, clients, stream)
	dl := awaitConsumerDeadLetter(ctx, t, clients, stream, group, origID, "open")
	sibling := awaitConsumerDeadLetter(ctx, t, clients, stream, streams.OrchestratorReleasePromotedVersions, origID, "open")
	cleanupIDs := []string{dl.GetId(), sibling.GetId()}
	t.Cleanup(func() { deleteDeadLetterRows(t, clients, cleanupIDs...) })
	require.True(t, dl.GetRedrivable(), "a dead letter with fields must be redrivable")

	// 3. Redrive as "e2e".
	callCtx := metadata.AppendToOutgoingContext(ctx, identity.MetadataKey, "e2e")
	resp, err := clients.deadLetterClient.RedriveDeadLetters(callCtx, &deadletterv1.RedriveDeadLettersRequest{
		Ids: []string{dl.GetId()}, Reason: "e2e redrive",
	})
	require.NoError(t, err)
	require.Len(t, resp.GetDeadLetters(), 1)
	first := resp.GetDeadLetters()[0]
	assert.Equal(t, "redriven", first.GetStatus())
	require.NotNil(t, first.GetRedrive())
	assert.Equal(t, "e2e", first.GetRedrive().GetActor())
	assert.Equal(t, "e2e redrive", first.GetRedrive().GetReason())

	// 4. The publish happened: one entry names the dead letter and the group.
	var redriven goredis.XMessage
	require.Eventually(t, func() bool {
		entries := redrivenEntries(ctx, clients.redisClient, stream, origID, dl.GetId())
		if len(entries) == 0 {
			return false
		}
		redriven = entries[0]
		return true
	}, deadLetterAwait, time.Second, "no entry with redriven_from=%s published to %s", dl.GetId(), stream)
	assert.Equal(t, group, redriven.Values["redrive_group"], "the redrive must address only the group that failed")
	redrivenID := redriven.ID

	// 5. The redriven entry reached the addressed group's handler, which rejected
	// it again: a new dead letter, for the redriven entry's own message id.
	again := awaitConsumerDeadLetter(ctx, t, clients, stream, group, redrivenID, "open")
	cleanupIDs = append(cleanupIDs, again.GetId())
	assert.NotEqual(t, origID, again.GetOriginalMessageId())
	assert.NotEqual(t, dl.GetId(), again.GetId())

	// The other groups acknowledged the entry without handling it: no dead letter
	// is recorded for it on their group, and it is not left pending there.
	for _, other := range otherGroups {
		require.Eventually(t, func() bool {
			pending, err := clients.redisClient.XPendingExt(ctx, &goredis.XPendingExtArgs{
				Stream: stream, Group: other, Start: redrivenID, End: redrivenID, Count: 1,
			}).Result()
			return err == nil && len(pending) == 0
		}, 30*time.Second, time.Second, "group %s must acknowledge the redriven entry", other)
	}
	require.Never(t, func() bool {
		for _, other := range otherGroups {
			if findConsumerDeadLetter(ctx, clients, stream, other, redrivenID, "open") != nil {
				return true
			}
		}
		return false
	}, negativeWindow, time.Second, "a group the redrive did not address handled the redriven entry")

	// 6. A second redrive of the same dead letter returns the same record and
	// publishes nothing.
	second, err := clients.deadLetterClient.RedriveDeadLetters(callCtx, &deadletterv1.RedriveDeadLettersRequest{
		Ids: []string{dl.GetId()}, Reason: "a different reason",
	})
	require.NoError(t, err)
	require.Len(t, second.GetDeadLetters(), 1)
	assert.Equal(t, "redriven", second.GetDeadLetters()[0].GetStatus())
	assert.Equal(t, first.GetRedrive().GetActor(), second.GetDeadLetters()[0].GetRedrive().GetActor())
	assert.Equal(t, first.GetRedrive().GetReason(), second.GetDeadLetters()[0].GetRedrive().GetReason())
	assert.Equal(t, first.GetRedrive().GetAt(), second.GetDeadLetters()[0].GetRedrive().GetAt())

	time.Sleep(5 * time.Second)
	assert.Len(t, redrivenEntries(ctx, clients.redisClient, stream, origID, dl.GetId()), 1,
		"a repeated redrive must not publish a second entry")
}

// TestDeadLetter_CLIListsAndRedrives drives the stored dead letter through the
// real continuo binary: dlq list shows it, dlq redrive republishes it.
func TestDeadLetter_CLIListsAndRedrives(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	t.Cleanup(func() { clients.close(ctx) })

	const stream = streams.ReleasePromotedV1
	const group = streams.OrchestratorReleasePromoted
	bin := buildCLI(t, ctx)
	env := []string{
		"CONTINUO_DEAD_LETTER_ADDR=" + getEnv("DEAD_LETTER_HOST", "dead-letter-controller") + ":50055",
		"CONTINUO_ACTOR=e2e-cli",
	}

	origID := publishMalformed(ctx, t, clients, stream)
	dl := awaitConsumerDeadLetter(ctx, t, clients, stream, group, origID, "open")
	sibling := awaitConsumerDeadLetter(ctx, t, clients, stream, streams.OrchestratorReleasePromotedVersions, origID, "open")
	cleanupIDs := []string{dl.GetId(), sibling.GetId()}
	t.Cleanup(func() { deleteDeadLetterRows(t, clients, cleanupIDs...) })

	// dlq list consumer <stream>: exit 0 and a dead_letters array holding the
	// stored dead letter.
	var listed struct {
		TotalOpen   int64           `json:"total_open"`
		DeadLetters []cliDeadLetter `json:"dead_letters"`
	}
	runCLI(t, ctx, bin, env, &listed, "dlq", "list", sourceConsumer, stream)
	require.NotNil(t, listed.DeadLetters, "dead_letters must be a JSON array")
	var found *cliDeadLetter
	for i := range listed.DeadLetters {
		if listed.DeadLetters[i].ID == dl.GetId() {
			found = &listed.DeadLetters[i]
		}
	}
	require.NotNil(t, found, "dlq list did not show dead letter %s", dl.GetId())
	assert.Equal(t, sourceConsumer, found.Source)
	assert.Equal(t, stream, found.Stream)
	assert.Equal(t, group, found.Group)
	assert.Equal(t, origID, found.OriginalMessageID)
	assert.Equal(t, "open", found.Status)

	// dlq redrive <id> <reason>: the actor comes from CONTINUO_ACTOR.
	var redriven struct {
		DeadLetters []cliDeadLetter `json:"dead_letters"`
	}
	runCLI(t, ctx, bin, env, &redriven, "dlq", "redrive", dl.GetId(), "e2e cli redrive")
	require.Len(t, redriven.DeadLetters, 1)
	assert.Equal(t, "redriven", redriven.DeadLetters[0].Status)
	require.NotNil(t, redriven.DeadLetters[0].Redrive)
	assert.Equal(t, "e2e-cli", redriven.DeadLetters[0].Redrive.Actor)
	assert.Equal(t, "e2e cli redrive", redriven.DeadLetters[0].Redrive.Reason)

	var entry goredis.XMessage
	require.Eventually(t, func() bool {
		entries := redrivenEntries(ctx, clients.redisClient, stream, origID, dl.GetId())
		if len(entries) == 0 {
			return false
		}
		entry = entries[0]
		return true
	}, deadLetterAwait, time.Second, "the CLI redrive published nothing to %s", stream)
	again := awaitConsumerDeadLetter(ctx, t, clients, stream, group, entry.ID, "open")
	cleanupIDs = append(cleanupIDs, again.GetId())
}

// TestDeadLetter_BacklogMetricIsExposed checks dead-letter-controller's
// per-source backlog gauge, which is exported only while a dead letter is open.
func TestDeadLetter_BacklogMetricIsExposed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	t.Cleanup(func() { clients.close(ctx) })

	id := publishMalformed(ctx, t, clients, streams.TaskStatusUpdatedV1)
	dl := awaitConsumerDeadLetter(ctx, t, clients, streams.TaskStatusUpdatedV1, streams.StateTaskStatusUpdated, id, "open")
	t.Cleanup(func() { deleteDeadLetterRows(t, clients, dl.GetId()) })

	body := scrapeMetrics(t, getEnv("DEAD_LETTER_HOST", "dead-letter-controller"))
	assert.Contains(t, body, `continuo_dead_letter_backlog{`)
	assert.Contains(t, body, `continuo_dead_letter_oldest_open_age_seconds{`)
	assert.Contains(t, body, `continuo_dead_letters_total{`)
}

// cliDeadLetter is one dead letter as `continuo dlq` prints it.
type cliDeadLetter struct {
	ID                string `json:"id"`
	Source            string `json:"source"`
	Stream            string `json:"stream"`
	Group             string `json:"group"`
	OriginalMessageID string `json:"original_message_id"`
	Status            string `json:"status"`
	Redrive           *struct {
		Actor  string `json:"actor"`
		Reason string `json:"reason"`
	} `json:"redrive"`
}

// publishMalformed adds an entry with no payload field to stream, which every
// consumer of the stream rejects as a permanent failure, and returns its id.
func publishMalformed(ctx context.Context, t *testing.T, clients *testClients, stream string) string {
	t.Helper()
	id, err := clients.redisClient.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream, Values: map[string]any{"e2e_dead_letter_probe": t.Name()},
	}).Result()
	require.NoError(t, err)
	return id
}

// findConsumerDeadLetter returns the consumer dead letter that group recorded
// for the stream entry messageID, or nil when none is stored with that status.
func findConsumerDeadLetter(ctx context.Context, clients *testClients, stream, group, messageID, status string) *deadletterv1.DeadLetter {
	resp, err := clients.deadLetterClient.ListDeadLetters(ctx, &deadletterv1.ListDeadLettersRequest{
		Source: sourceConsumer, Stream: stream, Status: status, Limit: 500,
	})
	if err != nil {
		return nil
	}
	for _, d := range resp.GetDeadLetters() {
		if d.GetConsumerGroup() == group && d.GetOriginalMessageId() == messageID {
			return d
		}
	}
	return nil
}

// awaitConsumerDeadLetter polls ListDeadLetters until the dead letter that group
// recorded for messageID appears.
func awaitConsumerDeadLetter(ctx context.Context, t *testing.T, clients *testClients, stream, group, messageID, status string) *deadletterv1.DeadLetter {
	t.Helper()
	var dl *deadletterv1.DeadLetter
	require.Eventually(t, func() bool {
		dl = findConsumerDeadLetter(ctx, clients, stream, group, messageID, status)
		return dl != nil
	}, deadLetterAwait, time.Second, "no %s dead letter for %s on %s [%s]", status, messageID, stream, group)
	return dl
}

// redrivenEntries returns the entries of stream, from fromID on, that
// dead-letter-controller published when it redrove deadLetterID.
func redrivenEntries(ctx context.Context, rc *goredis.Client, stream, fromID, deadLetterID string) []goredis.XMessage {
	// fromID is the id of the entry the test published before triggering the
	// redrive, so the range starts at the work under test; the redriven entry is
	// read right after it is published, before any trim run can reach it.
	entries, err := rc.XRangeN(ctx, stream, fromID, "+", 5000).Result()
	if err != nil {
		return nil
	}
	var out []goredis.XMessage
	for _, e := range entries {
		if e.Values["redriven_from"] == deadLetterID {
			out = append(out, e)
		}
	}
	return out
}

// deleteDeadLetterRows removes the rows a test created: the dead letters and the
// redrive events their redrive wrote to the outbox. It deletes only the ids the
// test names.
func deleteDeadLetterRows(t *testing.T, clients *testClients, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := clients.deadLetterDB.Exec(`DELETE FROM dead_letter_outbox WHERE aggregate_id = $1::uuid`, id); err != nil {
			t.Logf("cleanup: delete outbox rows of %s: %v", id, err)
		}
		if _, err := clients.deadLetterDB.Exec(`DELETE FROM dead_letters WHERE id = $1::uuid`, id); err != nil {
			t.Logf("cleanup: delete dead letter %s: %v", id, err)
		}
	}
}

// buildCLI compiles the continuo binary from the mounted cli module (a separate
// module outside go.work, so the workspace is switched off for the build).
func buildCLI(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := getEnv("CONTINUO_CLI_DIR", "/app/cli")
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatalf("cli module not found at %s (mount ./cli there or set CONTINUO_CLI_DIR): %v", dir, err)
	}
	bin := filepath.Join(t.TempDir(), "continuo")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/continuo") //nolint:gosec // fixed arguments; dir is the cli module path
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build ./cmd/continuo: %s", out)
	return bin
}

// runCLI runs the binary, requires exit 0, and decodes its stdout JSON into v.
func runCLI(t *testing.T, ctx context.Context, bin string, env []string, v any, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, args...) //nolint:gosec // bin is the binary buildCLI just compiled
	cmd.Env = append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run(), "continuo %v failed: stdout=%s stderr=%s", args, stdout.String(), stderr.String())
	require.NoError(t, json.Unmarshal(stdout.Bytes(), v), "continuo %v printed non-JSON stdout: %s", args, stdout.String())
}
