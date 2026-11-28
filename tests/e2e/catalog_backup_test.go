package e2e

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

const (
	// catalogSourceDir is where the catalog container mounts catalog's source.
	// The bind mount hides any binary the image built under it, so the command
	// runs from source.
	catalogSourceDir = "/app/catalog"

	// defaultBackupPrefix is where the default tenant's backups are stored.
	defaultBackupPrefix = "tenants/default/backups/catalog/"

	// backupMetricWait is how long the last-success gauge may take to show a
	// backup: it is read from the bucket when scraped and cached for a minute.
	backupMetricWait = 90 * time.Second

	// backupSkippedAttempts is how many times the command is run while another
	// backup holds the tenant's lease (the scheduled backup, should it coincide
	// with the test), and backupSkippedPause the wait between two runs.
	backupSkippedAttempts = 3
	backupSkippedPause    = 20 * time.Second
)

// backupLastSuccessPattern captures the value of the last-success gauge from
// Prometheus exposition text, whether or not the sample carries labels.
var backupLastSuccessPattern = regexp.MustCompile(
	`(?m)^continuo_catalog_backup_last_success_timestamp_seconds(?:\{[^}]*\})? (\S+)$`)

// printedBackup is the one JSON line `catalog-backup take` prints for each
// backup it stores.
type printedBackup struct {
	ObjectKey       string  `json:"object_key"`
	Bytes           int64   `json:"bytes"`
	Nodes           int64   `json:"nodes"`
	Relationships   int64   `json:"relationships"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// printedListing is one element of the JSON array `catalog-backup list`
// prints.
type printedListing struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
}

// backupFileHeader is the first line of a backup file.
type backupFileHeader struct {
	FormatVersion  int              `json:"format_version"`
	TenantID       string           `json:"tenant_id"`
	TakenAt     string           `json:"taken_at"`
	CatalogVersion string           `json:"catalog_version"`
	Nodes          map[string]int64 `json:"nodes"`
	Relationships  map[string]int64 `json:"relationships"`
}

// TestCatalogBackupTakeWritesAnObjectAndMetric takes a backup of the
// default tenant's graph with the operator command, from source as the compose
// stack runs it (`go run ./cmd/catalog-backup` in the catalog container), and
// checks what it leaves behind:
//
//   - the command exits 0 and prints one JSON line naming the stored object;
//   - the bucket holds that object, new, under the tenant's backup prefix; it
//     gunzips to a header with format_version 1 and tenant_id default, followed
//     by one line per node and per relationship, as many as the header and the
//     printed line count;
//   - `list` names the object, newest first;
//   - catalog's /metrics shows the last-success gauge at or after the test's
//     start. The gauge is read from the bucket when scraped and cached for a
//     minute, so a scrape inside the cache window can still show the value from
//     before the backup: the test polls.
//
// The test assumes the e2e graph holds the default tenant only, which is true
// of every message and request the suite sends: the gauge reports the tenant
// whose newest backup is the oldest. The scheduled backup (02:00 UTC) may
// coincide with the test and store an object of its own, so the test asserts on
// the object the command printed; it runs the command again when the scheduled
// backup holds the tenant's lease. Only the object the test wrote is deleted
// afterwards. The test changes nothing else the suite reads: the backup pauses
// catalog's writes for the length of the backup, which its consumers retry
// after. Its file sorts before chat_test.go and after cancel_test.go; it needs
// no test before it.
func TestCatalogBackupTakeWritesAnObjectAndMetric(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Building the command (twice, cold), the backup, and the gauge's wait
	// (90s), with headroom.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	// The command stores the object in the bucket of catalog's own settings,
	// which the test process shares with the catalog container.
	bucket := getEnv("S3_BUCKET", e2eS3Bucket)

	known := storedBackupObjects(t, ctx, clients, bucket)
	t.Logf("%d backup(s) of the default tenant stored before the test", len(known))

	start := time.Now()
	printed := backUpDefaultTenant(t, ctx)
	key := printed.ObjectKey
	t.Cleanup(func() { deleteBackupObject(t, clients, bucket, key) })

	require.True(t, strings.HasPrefix(key, defaultBackupPrefix) && strings.HasSuffix(key, ".jsonl.gz"),
		"the printed object key %q must be a .jsonl.gz backup under %s", key, defaultBackupPrefix)
	require.NotContains(t, known, key, "the command must store a new object, not overwrite one")

	// The object is in the bucket. The scheduled backup may have stored one as
	// well; only the key the command printed is this test's.
	stored := storedBackupObjects(t, ctx, clients, bucket)
	size, ok := stored[key]
	require.True(t, ok, "the printed object %s is not in s3://%s/%s", key, bucket, defaultBackupPrefix)
	require.Equal(t, printed.Bytes, size, "the printed byte count must be the stored object's size")
	var added []string
	for k := range stored {
		if _, was := known[k]; !was {
			added = append(added, k)
		}
	}
	require.Contains(t, added, key, "the printed object must be among the objects stored since the test began")
	t.Logf("backup %s: %d bytes, %d node(s), %d relationship(s), writes paused %.2fs; %d new object(s) in the bucket",
		key, printed.Bytes, printed.Nodes, printed.Relationships, printed.DurationSeconds, len(added))

	requireBackupFile(t, readBackupObject(t, ctx, clients, bucket, key), printed, start)

	// The newest backup heads the list; a later backup (the scheduled one) may
	// stand before it.
	listing := listBackups(t, ctx)
	keys := make([]string, 0, len(listing))
	for _, entry := range listing {
		keys = append(keys, entry.Key)
		_, err := time.Parse(time.RFC3339, entry.LastModified)
		require.NoError(t, err, "last_modified of %s must be RFC 3339", entry.Key)
	}
	require.Contains(t, keys, key, "`catalog-backup list` must name the new backup")
	require.True(t, slices.IsSortedFunc(keys, func(a, b string) int { return strings.Compare(b, a) }),
		"`catalog-backup list` must print the newest backup first: %v", keys)
	require.GreaterOrEqual(t, keys[0], key, "the new backup, or a later one, heads the list")
	for _, entry := range listing {
		if entry.Key == key {
			require.Equal(t, size, entry.Size, "the listed size must be the stored object's")
		}
	}

	// The gauge shows the backup once its cache has refreshed.
	host := getEnv("CATALOG_HOST", "catalog")
	var last string
	pollUntilDescribed(t, ctx, backupMetricWait, 3*time.Second, func() (bool, error) {
		body, err := fetchMetrics(host)
		if err != nil {
			return false, err
		}
		m := backupLastSuccessPattern.FindStringSubmatch(body)
		if m == nil {
			last = "no sample"
			return false, nil
		}
		last = m[1]
		value, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return false, fmt.Errorf("parse the gauge %q: %w", m[1], err)
		}
		// The store's timestamps may be whole seconds, so the comparison is
		// made at the second the test started in.
		return value >= float64(start.Unix()), nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for continuo_catalog_backup_last_success_timestamp_seconds on %s to reach the test's start (%d): last value %s",
			host, start.Unix(), last)
	})
	t.Logf("✅ backup %s stored, listed and counted: last-success gauge %s (test started at %d)", key, last, start.Unix())
}

// backUpDefaultTenant runs `catalog-backup take --tenant default` and
// returns the one backup it printed. A run that was skipped because another
// backup holds the tenant's lease is repeated after a pause.
func backUpDefaultTenant(t *testing.T, ctx context.Context) printedBackup {
	t.Helper()
	for attempt := 1; ; attempt++ {
		stdout, stderr, err := runCatalogBackup(ctx, "take", "--tenant", "default")
		t.Logf("catalog-backup take (attempt %d), stderr:\n%s", attempt, stderr)
		if err == nil {
			return decodePrintedBackup(t, stdout)
		}
		// `go run` reports a command that exited non-zero as status 1 and
		// prints the command's own status on stderr; the command exits 3 when
		// a lease is held.
		if attempt < backupSkippedAttempts && strings.Contains(stderr, "exit status 3") {
			t.Logf("the backup was skipped: another backup or a restore holds the tenant's lease; running it again in %s", backupSkippedPause)
			select {
			case <-ctx.Done():
			case <-time.After(backupSkippedPause):
			}
			continue
		}
		require.NoError(t, err, "catalog-backup take --tenant default must exit 0; stdout: %s; stderr: %s", stdout, stderr)
	}
}

// decodePrintedBackup decodes stdout, which must be exactly one JSON object of
// the printed shape: stdout carries only the command's result.
func decodePrintedBackup(t *testing.T, stdout string) printedBackup {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.DisallowUnknownFields()
	var printed printedBackup
	require.NoError(t, dec.Decode(&printed), "stdout must be the JSON line of the stored backup: %q", stdout)
	require.False(t, dec.More(), "one tenant was backed up, so stdout holds one JSON line: %q", stdout)
	require.NotEmpty(t, printed.ObjectKey, "the printed backup must name its object: %q", stdout)
	return printed
}

// listBackups runs `catalog-backup list --tenant default` and returns the
// array it printed.
func listBackups(t *testing.T, ctx context.Context) []printedListing {
	t.Helper()
	stdout, stderr, err := runCatalogBackup(ctx, "list", "--tenant", "default")
	require.NoError(t, err, "catalog-backup list --tenant default must exit 0; stdout: %s; stderr: %s", stdout, stderr)
	var listing []printedListing
	require.NoError(t, json.Unmarshal([]byte(stdout), &listing), "stdout must be one JSON array: %q", stdout)
	return listing
}

// runCatalogBackup runs the operator command from catalog's source with the
// environment of this process, which is the catalog container's: the Neo4j and
// object-store settings the component itself reads.
func runCatalogBackup(ctx context.Context, args ...string) (stdout, stderr string, err error) {
	cmd := exec.CommandContext(ctx, "go", append([]string{"run", "./cmd/catalog-backup"}, args...)...) //nolint:gosec // fixed command; the arguments are literals of the test
	cmd.Dir = catalogSourceDir
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}

// requireBackupFile checks the gzipped backup file: a header of format 1 for
// the default tenant, taken no earlier than the test, then one JSON line per
// node and per relationship, as many as the header and the command's printed
// line count.
func requireBackupFile(t *testing.T, gz []byte, printed printedBackup, start time.Time) {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	require.NoError(t, err, "the backup object must be a gzip stream")
	defer func() { _ = zr.Close() }()
	// A node's compiled code can run to 256 KiB, so a line may be long.
	r := bufio.NewReaderSize(zr, 1<<20)

	headerLine, err := r.ReadBytes('\n')
	require.NoError(t, err, "the backup must start with a header line")
	dec := json.NewDecoder(bytes.NewReader(headerLine))
	dec.DisallowUnknownFields()
	var header backupFileHeader
	require.NoError(t, dec.Decode(&header), "the header must be one JSON object of the documented shape: %.300s", headerLine)
	require.Equal(t, 1, header.FormatVersion, "the backup's format_version")
	require.Equal(t, "default", header.TenantID, "the backup's tenant_id")
	takenAt, err := time.Parse(time.RFC3339Nano, header.TakenAt)
	require.NoError(t, err, "taken_at must be a timestamp: %q", header.TakenAt)
	require.False(t, takenAt.Before(start.Add(-clockTolerance)),
		"taken_at %s must not precede the test's start %s", takenAt, start)

	var records int64
	for {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			require.Empty(t, line, "the backup must end with a complete line")
			break
		}
		// A truncated or corrupt gzip stream fails here.
		require.NoError(t, err, "read the backup to its end")
		require.True(t, json.Valid(line), "every record must be one JSON value: %.200s", line)
		records++
	}

	nodes, relationships := sumCounts(header.Nodes), sumCounts(header.Relationships)
	require.Equal(t, nodes, printed.Nodes, "the header's node count must be the printed one")
	require.Equal(t, relationships, printed.Relationships, "the header's relationship count must be the printed one")
	require.Equal(t, nodes+relationships, records, "the backup must hold one line per node and per relationship")
}

// sumCounts adds up a header's counts per label or per relationship type.
func sumCounts(counts map[string]int64) int64 {
	var total int64
	for _, n := range counts {
		total += n
	}
	return total
}

// storedBackupObjects lists the default tenant's backups in the bucket, by key,
// with their sizes.
func storedBackupObjects(t *testing.T, ctx context.Context, clients *testClients, bucket string) map[string]int64 {
	t.Helper()
	objects := map[string]int64{}
	paginator := s3.NewListObjectsV2Paginator(clients.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(defaultBackupPrefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		require.NoError(t, err, "list s3://%s/%s", bucket, defaultBackupPrefix)
		for _, obj := range page.Contents {
			objects[aws.ToString(obj.Key)] = aws.ToInt64(obj.Size)
		}
	}
	return objects
}

// readBackupObject downloads the backup stored at key.
func readBackupObject(t *testing.T, ctx context.Context, clients *testClients, bucket, key string) []byte {
	t.Helper()
	resp, err := clients.s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err, "get s3://%s/%s", bucket, key)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read s3://%s/%s", bucket, key)
	return body
}

// deleteBackupObject removes the backup this test stored, and only that one. It
// runs on a context of its own, because the test's is cancelled by the time a
// cleanup runs.
func deleteBackupObject(t *testing.T, clients *testClients, bucket, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := clients.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Logf("delete the backup s3://%s/%s: %v", bucket, key, err)
	}
}
