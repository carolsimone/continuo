package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_DuplicateTable_RejectsBeforePromotion proves the duplicate-relation
// gate end to end: a release whose assembled topology has two nodes claiming one
// relation is rejected at parse time, before any validation Job runs and before
// current_prod moves.
//
// A python-odcs release is used because it has no compile leg — the bundle is
// uploaded directly and parsed on arrival — so the collision is reachable
// without rebuilding a dbt project image, and no project's committed models are
// left permanently colliding.
func TestE2E_DuplicateTable_RejectsBeforePromotion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-dup-" + uuid.NewString()[:8]

	// Every dbt project keeps its live pointer; the colliding python project is
	// new, so the assembled topology is "all prod nodes + this contract's node".
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests in S3 — setup.sh must run before the e2e suite")

	// Pick the victim deterministically: map iteration order is randomized in Go,
	// so collect every candidate node and sort by unique_id before choosing.
	type candidateNode struct {
		uniqueID    string
		project     string
		contentHash string
	}
	var candidates []candidateNode
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			candidates = append(candidates, candidateNode{
				uniqueID: n.uniqueID, project: svc, contentHash: n.contentHash,
			})
		}
	}
	require.NotEmpty(t, candidates, "baseline topology has no nodes to collide with")
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].uniqueID < candidates[j].uniqueID })

	victim := candidates[0]
	victimUniqueID, victimProject := victim.uniqueID, victim.project
	victimSchema, victimTable := splitUniqueID(t, victimUniqueID)
	t.Logf("release_id=%s colliding_with=%s (project %s)", releaseID, victimUniqueID, victimProject)

	var prodNodes []map[string]string
	for _, c := range candidates {
		prodNodes = append(prodNodes, map[string]string{
			"unique_id": c.uniqueID, "project": c.project, "content_hash": c.contentHash,
		})
	}

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allProjects)
	defer func() {
		if _, derr := clients.releaseDB.ExecContext(context.Background(),
			`DELETE FROM service_prod WHERE project_name = $1`, dupE2EProject); derr != nil {
			t.Errorf("cleanup: delete %s service_prod row: %v", dupE2EProject, derr)
		}
	}()

	// One ODCS contract declaring the victim's relation. Its read is non-empty,
	// since a python node must declare one, and names no table, so it brings no
	// upstream. topology-controller folds the bundle's source_hash with the
	// config part it computes from the contract, so any source_hash is
	// accepted. The release names an image no Job runs, so its bundle records
	// no package.
	contract := fmt.Sprintf(`apiVersion: v3.2.0
kind: DataContract
id: %s.%s
version: "0"
team:
  name: data-team
customProperties:
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: script, value: models/collider.py}
  - vendor: continuo
    property: readQueries
    value:
      upstream: select 1
schema:
  - name: %s
    physicalType: table
    properties:
      - {name: id, physicalType: integer, required: true}
`, victimSchema, victimTable, victimTable)
	bundle := encodeODCSBundle(t, dupE2EProject, map[string]string{}, []odcsBundleNode{{
		Path:       "contracts/" + victimTable + ".odcs.yaml",
		Text:       contract,
		SourceHash: sha256Hex("models/collider.py"),
	}})
	putODCSBundle(t, ctx, clients, dupE2EProject, releaseID, bundle)

	postPythonRelease(t, clients, dupE2EProject, releaseID, dupE2EImage, false)

	waitForReleaseRejected(t, ctx, clients, releaseID, 3*time.Minute)

	detail := getReleaseJSON(t, clients, releaseID)
	assert.Equal(t, "duplicate_table", detail["reject_reason"])
	rejectDetail, _ := detail["reject_detail"].(string)
	// victimUniqueID comes from parseManifestNodes, which takes it from
	// topology-controller's own parser, so it is already lowercased, as the
	// relation identity the gate groups on is; lowercasing it here keeps the
	// assertion about the identity rather than its spelling.
	assert.Contains(t, rejectDetail, strings.ToLower(victimUniqueID),
		"the rejection must name the contested relation")
	assert.Contains(t, rejectDetail, victimProject,
		"the rejection must name the incumbent project")
	assert.Contains(t, rejectDetail, dupE2EProject,
		"the rejection must name the newcomer project")

	var currentProdRelease string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT release_id FROM current_prod`).Scan(&currentProdRelease))
	assert.NotEqual(t, releaseID, currentProdRelease,
		"a rejected release must never become current_prod")
}

// dupE2EProject is the synthetic python project this test introduces; it exists
// only for the duration of the test and is removed from service_prod on cleanup.
const dupE2EProject = "e2e-dup-collider"

// dupE2EImage is an arbitrary tag: the release is rejected at parse time, so no
// Job is ever created from it.
const dupE2EImage = "e2e-dup:latest"

// splitUniqueID splits "<schema>.<table>" into its two segments.
func splitUniqueID(t *testing.T, uniqueID string) (schema, table string) {
	t.Helper()
	for i := len(uniqueID) - 1; i >= 0; i-- {
		if uniqueID[i] == '.' {
			return uniqueID[:i], uniqueID[i+1:]
		}
	}
	t.Fatalf("unique_id %q is not <schema>.<table>", uniqueID)
	return "", ""
}

// getReleaseJSON reads GET /releases/{id} from release-controller.
func getReleaseJSON(t *testing.T, clients *testClients, releaseID string) map[string]any {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}
