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

	// defaultBackupPrefix is where the default tenant's exports are stored.
	defaultBackupPrefix = "tenants/default/backups/catalog/"

	// exportMetricWait is how long the last-success gauge may take to show an
	// export: it is read from the bucket when scraped and cached for a minute.
	exportMetricWait = 90 * time.Second

	// exportSkippedAttempts is how many times the command is run while another
	// export holds the tenant's lease (the scheduled export, should it coincide
	// with the test), and exportSkippedPause the wait between two runs.
	exportSkippedAttempts = 3
	exportSkippedPause    = 20 * time.Second
)

// exportLastSuccessPattern captures the value of the last-success gauge from
// Prometheus exposition text, whether or not the sample carries labels.
var exportLastSuccessPattern = regexp.MustCompile(
	`(?m)^continuo_catalog_export_last_success_timestamp_seconds(?:\{[^}]*\})? (\S+)$`)

// printedExport is the one JSON line `catalog-backup export` prints for each
// export it stores.
type printedExport struct {
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

// exportFileHeader is the first line of an export file.
type exportFileHeader struct {
	FormatVersion  int              `json:"format_version"`
	TenantID       string           `json:"tenant_id"`
	ExportedAt     string           `json:"exported_at"`
	CatalogVersion string           `json:"catalog_version"`
	Nodes          map[string]int64 `json:"nodes"`
	Relationships  map[string]int64 `json:"relationships"`
}

// TestCatalogBackupExportWritesAnObjectAndMetric takes an export of the
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
//     before the export: the test polls.
//
// The test assumes the e2e graph holds the default tenant only, which is true
// of every message and request the suite sends: the gauge reports the tenant
// whose newest export is the oldest. The scheduled export (02:00 UTC) may
// coincide with the test and store an object of its own, so the test asserts on
// the object the command printed; it runs the command again when the scheduled
// export holds the tenant's lease. Only the object the test wrote is deleted
// afterwards. The test changes nothing else the suite reads: the export pauses
// catalog's writes for the length of the export, which its consumers retry
// after. Its file sorts before chat_test.go and after cancel_test.go; it needs
// no test before it.
func TestCatalogBackupExportWritesAnObjectAndMetric(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Building the command (twice, cold), the export, and the gauge's wait
	// (90s), with headroom.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	// The command stores the object in the bucket of catalog's own settings,
	// which the test process shares with the catalog container.
	bucket := getEnv("S3_BUCKET", e2eS3Bucket)

	known := storedBackupObjects(t, ctx, clients, bucket)
	t.Logf("%d export(s) of the default tenant stored before the test", len(known))

	start := time.Now()
	printed := exportDefaultTenant(t, ctx)
	key := printed.ObjectKey
	t.Cleanup(func() { deleteBackupObject(t, clients, bucket, key) })

	require.True(t, strings.HasPrefix(key, defaultBackupPrefix) && strings.HasSuffix(key, ".jsonl.gz"),
		"the printed object key %q must be a .jsonl.gz export under %s", key, defaultBackupPrefix)
	require.NotContains(t, known, key, "the command must store a new object, not overwrite one")

	// The object is in the bucket. The scheduled export may have stored one as
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
	t.Logf("export %s: %d bytes, %d node(s), %d relationship(s), writes paused %.2fs; %d new object(s) in the bucket",
		key, printed.Bytes, printed.Nodes, printed.Relationships, printed.DurationSeconds, len(added))

	requireExportFile(t, readBackupObject(t, ctx, clients, bucket, key), printed, start)

	// The newest export heads the list; a later export (the scheduled one) may
	// stand before it.
	listing := listBackupExports(t, ctx)
	keys := make([]string, 0, len(listing))
	for _, entry := range listing {
		keys = append(keys, entry.Key)
		_, err := time.Parse(time.RFC3339, entry.LastModified)
		require.NoError(t, err, "last_modified of %s must be RFC 3339", entry.Key)
	}
	require.Contains(t, keys, key, "`catalog-backup list` must name the new export")
	require.True(t, slices.IsSortedFunc(keys, func(a, b string) int { return strings.Compare(b, a) }),
		"`catalog-backup list` must print the newest export first: %v", keys)
	require.GreaterOrEqual(t, keys[0], key, "the new export, or a later one, heads the list")
	for _, entry := range listing {
		if entry.Key == key {
			require.Equal(t, size, entry.Size, "the listed size must be the stored object's")
		}
	}

	// The gauge shows the export once its cache has refreshed.
	host := getEnv("CATALOG_HOST", "catalog")
	var last string
	pollUntilDescribed(t, ctx, exportMetricWait, 3*time.Second, func() (bool, error) {
		body, err := fetchMetrics(host)
		if err != nil {
			return false, err
		}
		m := exportLastSuccessPattern.FindStringSubmatch(body)
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
		return fmt.Sprintf("timeout waiting for continuo_catalog_export_last_success_timestamp_seconds on %s to reach the test's start (%d): last value %s",
			host, start.Unix(), last)
	})
	t.Logf("✅ export %s stored, listed and counted: last-success gauge %s (test started at %d)", key, last, start.Unix())
}

// exportDefaultTenant runs `catalog-backup export --tenant default` and
// returns the one export it printed. A run that was skipped because another
// export holds the tenant's lease is repeated after a pause.
func exportDefaultTenant(t *testing.T, ctx context.Context) printedExport {
	t.Helper()
	for attempt := 1; ; attempt++ {
		stdout, stderr, err := runCatalogBackup(ctx, "export", "--tenant", "default")
		t.Logf("catalog-backup export (attempt %d), stderr:\n%s", attempt, stderr)
		if err == nil {
			return decodePrintedExport(t, stdout)
		}
		// `go run` reports a command that exited non-zero as status 1 and
		// prints the command's own status on stderr; the command exits 3 when
		// a lease is held.
		if attempt < exportSkippedAttempts && strings.Contains(stderr, "exit status 3") {
			t.Logf("the export was skipped: another export or a restore holds the tenant's lease; running it again in %s", exportSkippedPause)
			select {
			case <-ctx.Done():
			case <-time.After(exportSkippedPause):
			}
			continue
		}
		require.NoError(t, err, "catalog-backup export --tenant default must exit 0; stdout: %s; stderr: %s", stdout, stderr)
	}
}

// decodePrintedExport decodes stdout, which must be exactly one JSON object of
// the printed shape: stdout carries only the command's result.
func decodePrintedExport(t *testing.T, stdout string) printedExport {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.DisallowUnknownFields()
	var printed printedExport
	require.NoError(t, dec.Decode(&printed), "stdout must be the JSON line of the stored export: %q", stdout)
	require.False(t, dec.More(), "one tenant was exported, so stdout holds one JSON line: %q", stdout)
	require.NotEmpty(t, printed.ObjectKey, "the printed export must name its object: %q", stdout)
	return printed
}

// listBackupExports runs `catalog-backup list --tenant default` and returns the
// array it printed.
func listBackupExports(t *testing.T, ctx context.Context) []printedListing {
	t.Helper()
	stdout, stderr, err := runCatalogBackup(ctx, "list", "--tenant", "default")
	require.NoError(t, err, "catalog-backup list --tenant default must exit 0; stdout: %s; stderr: %s", stdout, stderr)
	var listing []printedListing
	require.NoError(t, json.Unmarshal([]byte(stdout), &listing), "stdout must be one JSON array: %q", stdout)
	return listing
}

// runCatalogBackup runs the operator command from catalog's source with the
// environment of this process, which is the catalog container's: the Neo4j and
// object-store settings the service itself reads.
func runCatalogBackup(ctx context.Context, args ...string) (stdout, stderr string, err error) {
	cmd := exec.CommandContext(ctx, "go", append([]string{"run", "./cmd/catalog-backup"}, args...)...) //nolint:gosec // fixed command; the arguments are literals of the test
	cmd.Dir = catalogSourceDir
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}

// requireExportFile checks the gzipped export file: a header of format 1 for
// the default tenant, taken no earlier than the test, then one JSON line per
// node and per relationship, as many as the header and the command's printed
// line count.
func requireExportFile(t *testing.T, gz []byte, printed printedExport, start time.Time) {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	require.NoError(t, err, "the export object must be a gzip stream")
	defer func() { _ = zr.Close() }()
	// A node's compiled code can run to 256 KiB, so a line may be long.
	r := bufio.NewReaderSize(zr, 1<<20)

	headerLine, err := r.ReadBytes('\n')
	require.NoError(t, err, "the export must start with a header line")
	dec := json.NewDecoder(bytes.NewReader(headerLine))
	dec.DisallowUnknownFields()
	var header exportFileHeader
	require.NoError(t, dec.Decode(&header), "the header must be one JSON object of the documented shape: %.300s", headerLine)
	require.Equal(t, 1, header.FormatVersion, "the export's format_version")
	require.Equal(t, "default", header.TenantID, "the export's tenant_id")
	exportedAt, err := time.Parse(time.RFC3339Nano, header.ExportedAt)
	require.NoError(t, err, "exported_at must be a timestamp: %q", header.ExportedAt)
	require.False(t, exportedAt.Before(start.Add(-clockTolerance)),
		"exported_at %s must not precede the test's start %s", exportedAt, start)

	var records int64
	for {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			require.Empty(t, line, "the export must end with a complete line")
			break
		}
		// A truncated or corrupt gzip stream fails here.
		require.NoError(t, err, "read the export to its end")
		require.True(t, json.Valid(line), "every record must be one JSON value: %.200s", line)
		records++
	}

	nodes, relationships := sumCounts(header.Nodes), sumCounts(header.Relationships)
	require.Equal(t, nodes, printed.Nodes, "the header's node count must be the printed one")
	require.Equal(t, relationships, printed.Relationships, "the header's relationship count must be the printed one")
	require.Equal(t, nodes+relationships, records, "the export must hold one line per node and per relationship")
}

// sumCounts adds up a header's counts per label or per relationship type.
func sumCounts(counts map[string]int64) int64 {
	var total int64
	for _, n := range counts {
		total += n
	}
	return total
}

// storedBackupObjects lists the default tenant's exports in the bucket, by key,
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

// readBackupObject downloads the export stored at key.
func readBackupObject(t *testing.T, ctx context.Context, clients *testClients, bucket, key string) []byte {
	t.Helper()
	resp, err := clients.s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err, "get s3://%s/%s", bucket, key)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read s3://%s/%s", bucket, key)
	return body
}

// deleteBackupObject removes the export this test stored, and only that one. It
// runs on a context of its own, because the test's is cancelled by the time a
// cleanup runs.
func deleteBackupObject(t *testing.T, clients *testClients, bucket, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := clients.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Logf("delete the export s3://%s/%s: %v", bucket, key, err)
	}
}
