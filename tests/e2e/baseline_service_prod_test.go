package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// serviceProdRow is one release-controller service_prod pointer row.
type serviceProdRow struct {
	releaseID     string
	manifestS3Key string
	imageTag      string
}

// baselineServiceProd is the snapshot of the service_prod rows that
// provision-k8s-test-env.sh / setup.sh seed before the suite runs — the
// per-service production image pointers the e2e reads via readServiceImageTag.
// It is captured once in TestMain (before any test mutates the table).
var baselineServiceProd map[string]serviceProdRow

// TestMain captures the service_prod baseline before the suite runs. The
// blue/green release tests mutate service_prod (promotion rewrites the changed
// service's row; the bootstrap test wipes the table), so a later test reading a
// per-service image tag would otherwise see a missing row. restoreBaselineServiceProd
// re-applies this snapshot before each consumer, keeping reads order-independent.
func TestMain(m *testing.M) {
	host := getEnv("POSTGRES_HOST", "postgres")
	pw := getEnv("POSTGRES_PASSWORD", "continuo")
	connStr := fmt.Sprintf(
		"host=%s port=5432 dbname=continuo_release user=continuo_svc password=%s sslmode=disable", host, pw)
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "TestMain: connect continuo_release: %v\n", err)
		os.Exit(1)
	}
	rows, err := db.QueryContext(context.Background(),
		`SELECT service_name, release_id, manifest_s3_key, image_tag FROM service_prod`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "TestMain: read service_prod baseline: %v\n", err)
		os.Exit(1)
	}
	baselineServiceProd = map[string]serviceProdRow{}
	for rows.Next() {
		var svc string
		var r serviceProdRow
		if err := rows.Scan(&svc, &r.releaseID, &r.manifestS3Key, &r.imageTag); err != nil {
			fmt.Fprintf(os.Stderr, "TestMain: scan service_prod: %v\n", err)
			os.Exit(1)
		}
		baselineServiceProd[svc] = r
	}
	_ = rows.Close()
	_ = db.Close()
	if len(baselineServiceProd) == 0 {
		fmt.Fprintln(os.Stderr,
			"TestMain: service_prod baseline is empty — provision-k8s-test-env.sh / setup.sh must seed it before e2e")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// restoreBaselineServiceProd re-applies the service_prod snapshot captured at
// suite start. Called at the top of every consumer that reads per-service image
// tags (seedTopology, baselineServices) so a prior test's mutation of
// service_prod cannot break this test's reads.
func restoreBaselineServiceProd(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	for svc, r := range baselineServiceProd {
		_, err := clients.releaseDB.ExecContext(ctx,
			`INSERT INTO service_prod (service_name, release_id, manifest_s3_key, image_tag, updated_at)
			 VALUES ($1, $2, $3, $4, now())
			 ON CONFLICT (service_name) DO UPDATE SET
			   release_id = EXCLUDED.release_id, manifest_s3_key = EXCLUDED.manifest_s3_key,
			   image_tag = EXCLUDED.image_tag, updated_at = EXCLUDED.updated_at`,
			svc, r.releaseID, r.manifestS3Key, r.imageTag)
		require.NoError(t, err, "restore baseline service_prod row for %s", svc)
	}
}
