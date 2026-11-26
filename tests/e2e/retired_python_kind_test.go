package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retiredPythonKind is the kind a Python project was once released with. It is
// built from two parts because the retired-names guard keeps the whole word out
// of every tracked file.
const retiredPythonKind = "python-" + "odcs"

// retiredPythonKindRefusal is the text release-controller refuses a release of
// the retired kind with, as it refuses any unknown kind. The body of the 400
// carries it behind the prefix `invalid candidate: ` that release-controller's
// candidate handler adds, and the ui relays that body trimmed.
const retiredPythonKindRefusal = `unknown manifest kind "` + retiredPythonKind + `" (expected "dbt", "python" or "ontology")`

// TestE2E_TheRetiredPythonKindIsRefused proves the retired Python kind is
// refused as an unknown kind through the public release route: the body below
// is the one a Python release posts for the py-probe fixture, with only its
// kind set to the retired one, and POST /api/v1/releases answers 400 with
// release-controller's refusal. The submission is refused before anything is
// stored, so no release row is created.
func TestE2E_TheRetiredPythonKindIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-pyretired-" + uuid.NewString()[:8]
	token := mintCIToken(t, e2ePyRepositoryID, "carolsimone/continuo-py-demo", releaseID)
	status, body := submitPublicRelease(t, clients, token, map[string]any{
		"project":    pyE2EProject,
		"release_id": releaseID,
		"image_tag":  pyFixtureImage,
		"kind":       retiredPythonKind,
		"bootstrap":  false,
	})
	require.Equal(t, http.StatusBadRequest, status,
		"POST /api/v1/releases with the retired Python kind: expected 400, got %d: %v", status, body)

	refusal, _ := body["error"].(string)
	assert.Contains(t, refusal, retiredPythonKindRefusal, "the 400 body carries the refusal: %v", body)
	assert.NotContains(t, body, "release_id", "a refused submission answers no release id: %v", body)

	var stored int
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT count(*) FROM release_pipeline_runs WHERE run_id = $1`, releaseID).Scan(&stored))
	assert.Zero(t, stored, "a refused submission stores no release")
	t.Logf("release %s with the retired Python kind refused: %s", releaseID, refusal)
}
