package e2e

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// baselineImageTagsFile holds the per-service dbt image tags written by
// provision-k8s-test-env.sh / setup.sh (bind-mounted at /app/tests/e2e). It is
// immutable setup metadata — the content-addressed tag each dbt image is loaded
// into kind under — in IMAGE_TAG_PER_SERVICE form:
// "service-1=<tag>,service-2=<tag>,service-3=<tag>".
const baselineImageTagsFile = ".image-tags"

// seedBaselineServiceProd (re)establishes the per-service service_prod pointer
// rows from the immutable deploy metadata. It is called before every consumer
// that reads per-service image tags (seedTopology, baselineServices) so a prior
// blue/green test's mutation of service_prod — promotion rewriting a row, or the
// bootstrap test wiping the table — cannot break a later test's read. The
// baseline is rebuilt from setup metadata, never captured from the (test-mutated)
// table.
func seedBaselineServiceProd(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	for svc, tag := range baselineImageTags(t) {
		_, err := clients.releaseDB.ExecContext(ctx,
			`INSERT INTO service_prod (service_name, release_id, manifest_s3_key, image_tag, updated_at)
			 VALUES ($1, $2, $3, $4, now())
			 ON CONFLICT (service_name) DO UPDATE SET
			   release_id = EXCLUDED.release_id, manifest_s3_key = EXCLUDED.manifest_s3_key,
			   image_tag = EXCLUDED.image_tag, updated_at = EXCLUDED.updated_at`,
			svc, e2eBaselineReleaseID, canonicalManifestS3URI(svc, e2eBaselineReleaseID), tag)
		require.NoError(t, err, "seed baseline service_prod row for %s", svc)
	}
}

// baselineImageTags parses the per-service image tags from the
// IMAGE_TAG_PER_SERVICE env var, falling back to the bind-mounted .image-tags
// file. Format: comma-separated "service=tag" pairs.
func baselineImageTags(t *testing.T) map[string]string {
	t.Helper()
	spec := os.Getenv("IMAGE_TAG_PER_SERVICE")
	if spec == "" {
		b, err := os.ReadFile(baselineImageTagsFile)
		require.NoError(t, err,
			"read %s — provision-k8s-test-env.sh / setup.sh must write the per-service image tags", baselineImageTagsFile)
		spec = strings.TrimSpace(string(b))
	}
	tags := map[string]string{}
	for _, pair := range strings.Split(spec, ",") {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		require.Len(t, kv, 2, "malformed image-tag pair %q in %q", pair, spec)
		require.NotEmpty(t, kv[1], "empty image tag for %q", kv[0])
		tags[kv[0]] = kv[1]
	}
	require.NotEmpty(t, tags, "no per-service image tags found")
	return tags
}
