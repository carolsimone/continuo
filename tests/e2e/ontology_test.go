package e2e

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"
	"github.com/carolsimone/continuo/pkg/contractbundle"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	// ontologyE2EService is the service that releases the install's ontology
	// (tests/e2e/ci-auth.json binds it to the py-probe repository), and
	// ontologyArtifact the object name of its artifact:
	// <service>/<release_id>/ontology.yaml.
	ontologyE2EService = "ontology-e2e"
	ontologyArtifact   = "ontology.yaml"

	// ontologyBaseURL is the base URL catalog publishes ontology links under
	// while the test has the switch on (contracts.ontology.baseUrl).
	ontologyBaseURL = "https://ontology.example/things"

	// ontologyDisabledText is the answer to a release of kind ontology while
	// the install's ontology switch is off, and ontologyKindChangeText the
	// answer to a dbt service released as kind ontology.
	ontologyDisabledText   = `kind "ontology" is accepted only when the install's ontology switch is on (contracts.ontology.enabled)`
	ontologyKindChangeText = `service "service-1" is released as kind "dbt": a service cannot change between kind "ontology" and a node kind`

	// noOntologyWarning is the finding a release carries when the switch is on
	// and no ontology is released.
	noOntologyWarning = "the ontology switch is on and no ontology is released: bindings are kept as written and not checked"

	// ontologySwitchBudget covers the three services restarting, with a cold
	// `go run` build each, and answering their health checks.
	ontologySwitchBudget = 4 * time.Minute

	// ontologyQuietWindow is how long the test waits after a rejection before
	// it asserts that no remediation trigger names the release.
	ontologyQuietWindow = 15 * time.Second
)

// ontologyFile is the ontology the test releases, and ontologyBreakingFile
// the same ontology with the thing ProbeRow renamed: a valid file that breaks
// the binding of e2e_schema.py_probe.
//
//go:embed fixtures/ontology/ontology.yaml
var ontologyFile string

//go:embed fixtures/ontology/ontology_breaking.yaml
var ontologyBreakingFile string

// pyFixtureNodes are the four nodes of the py-probe service, each bound to a
// thing of ontologyFile.
var pyFixtureNodes = []string{pyProbeUniqueID, pyCsvUniqueID, pyAPIUniqueID, pyBadShapeUniqueID}

// TestE2E_Ontology proves the install's ontology switch and the ontology
// service, with the py-probe service bound to a thing for each of its four
// contracts:
//
//  1. with the switch off, a release of kind ontology is refused with 400;
//  2. the switch goes on (topology-controller, release-controller and
//     catalog restart with it);
//  3. the py-probe service is released and promoted: its contract bundle
//     says the switch is on and no ontology is released, as a warning;
//  4. the ontology service releases ontology.yaml: nothing is validated, the
//     bundle names the ontology, every py-probe contract gets a new version
//     that names it, `continuo node contract` and the live folder show the
//     contract's domain and its absolute ontology links;
//  5. an ontology that breaks a binding of the py-probe service is rejected
//     invalid_contract on the node it breaks, sends no remediation trigger,
//     and leaves the published contracts and the released ontology as they were;
//  6. a dbt service released as kind ontology is refused with 409.
func TestE2E_Ontology(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// additive polls: two restarts of the services (4m each), the py-probe
	// release (validation 8m within the promotion's 10m, the swap 2m, the
	// contracts 2m), the ontology release (10m, contracts 2m) and the
	// rejection (3m).
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	bin, env := contractsCLI(t, ctx)
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.RemediationRequestedV3)

	// The switch is off again, and neither service this test releases is left
	// in service_prod, whatever happens below: a later release would
	// otherwise assemble the ontology and the py-probe contracts into every
	// manifest set.
	t.Cleanup(func() {
		forgetOntologyServices(t, clients)
		setOntology(t, context.Background(), false)
	})
	forgetOntologyServices(t, clients)

	// ── 1. the switch is off: a release of kind ontology is refused ──────────
	status, body := submitOntologyRelease(t, clients, ontologyE2EService, "e2e-ont-"+uuid.NewString()[:8])
	require.Equal(t, http.StatusBadRequest, status, "kind ontology with the switch off: %v", body)
	assert.Equal(t, ontologyDisabledText, body["error"])

	// ── 2. the switch goes on ────────────────────────────────────────────────
	setOntology(t, ctx, true)

	// ── 3. the py-probe service is released with the switch on and no ontology
	firstID, cleanup := promotePythonFixtureRelease(t, ctx, clients, validationTap)
	defer cleanup()
	waitForLiveIndexOf(t, ctx, clients, firstID, contractsBudget)

	first := candidateContractBundle(t, ctx, clients, firstID)
	require.NotEqual(t, "null", strings.TrimSpace(string(first.Ontology)), "the bundle of a release with the switch on holds an ontology value")
	noOntology := bundleOntologyOf(t, first)
	assert.True(t, noOntology.Enabled)
	assert.Nil(t, noOntology.Service, "no ontology is released")
	assert.Nil(t, noOntology.ReleaseID)
	assert.Nil(t, noOntology.SHA256)
	assert.Empty(t, noOntology.DeclaredServices)
	assert.Contains(t, noOntology.Findings,
		ontologyFinding{Severity: "warning", Rule: "contract", Subject: "ontology", Message: noOntologyWarning},
		"a release with the switch on and no ontology says so")
	assert.Empty(t, first.OntologyVersion(), "a release that used no ontology names none")
	beforeOntology := map[string]*catalogv1.ContractVersionView{}
	for _, node := range pyFixtureNodes {
		v := latestContractVersion(t, ctx, clients, node)
		beforeOntology[node] = v
		assert.Empty(t, v.GetOntologyVersion(), "%s: a version made with no ontology names none", node)
	}
	t.Logf("✅ release %s: the switch is on, no ontology is released, and the bundle says so as a warning", firstID)

	// ── 4. the ontology service releases ontology.yaml ───────────────────────
	ontologyID := "e2e-ont-" + uuid.NewString()[:8]
	resetReleaseControllerQueue(t, ctx, clients)
	putOntologyFile(t, ctx, clients, ontologyID, ontologyFile)
	status, body = submitOntologyRelease(t, clients, ontologyE2EService, ontologyID)
	require.Equal(t, http.StatusAccepted, status, "POST /api/v1/releases of the ontology: %v", body)
	waitForReleasePromoted(t, ctx, clients, ontologyID, 10*time.Minute)

	detail := getReleaseJSON(t, clients, ontologyID)
	assert.Empty(t, detail["validation_node_ids"], "an ontology release changes no node's content_hash, so it validates nothing")
	assertNoValidationRequested(t, validationTap, ontologyID)

	var kind, manifestKey, releasedAs string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT manifest_kind, manifest_s3_key, release_id FROM service_prod WHERE service_name = $1`,
		ontologyE2EService).Scan(&kind, &manifestKey, &releasedAs))
	assert.Equal(t, "ontology", kind, "production records the ontology service's kind")
	assert.True(t, strings.HasSuffix(manifestKey, "/"+ontologyE2EService+"/"+ontologyID+"/"+ontologyArtifact),
		"production points at the ontology file, got %s", manifestKey)
	assert.Equal(t, ontologyID, releasedAs)

	wantVersion := ontologyID + "@sha256:" + sha256Hex(ontologyFile)
	released := candidateContractBundle(t, ctx, clients, ontologyID)
	ontology := bundleOntologyOf(t, released)
	assert.True(t, ontology.Enabled)
	require.NotNil(t, ontology.Service, "the bundle names the ontology service")
	assert.Equal(t, ontologyE2EService, *ontology.Service)
	require.NotNil(t, ontology.ReleaseID)
	assert.Equal(t, ontologyID, *ontology.ReleaseID)
	require.NotNil(t, ontology.SHA256)
	assert.Equal(t, sha256Hex(ontologyFile), *ontology.SHA256)
	assert.Equal(t, []string{pyE2EService}, ontology.DeclaredServices)
	assert.Equal(t, wantVersion, released.OntologyVersion())
	for _, f := range ontology.Findings {
		assert.NotEqual(t, "error", f.Severity, "an ontology release that promoted has no error finding: %+v", f)
	}

	waitForLiveIndexOf(t, ctx, clients, ontologyID, contractsBudget)
	for _, node := range pyFixtureNodes {
		v := latestContractVersion(t, ctx, clients, node)
		assert.Greater(t, v.GetVersion(), beforeOntology[node].GetVersion(), "%s: the resolved contract is a new version", node)
		assert.Equal(t, ontologyID, v.GetReleaseId(), "%s: the new version records the ontology release", node)
		assert.Equal(t, wantVersion, v.GetOntologyVersion(), "%s: the new version names the ontology it used", node)
	}

	var history struct {
		Versions []struct {
			Version   int64  `json:"version"`
			ReleaseID string `json:"release_id"`
			IsCurrent bool   `json:"is_current"`
		} `json:"versions"`
	}
	runCLI(t, ctx, bin, env, &history, "node", "contract-history", pyProbeUniqueID)
	require.NotEmpty(t, history.Versions, "contract-history lists the versions of %s", pyProbeUniqueID)
	assert.Equal(t, ontologyID, history.Versions[0].ReleaseID, "the newest version is the ontology release's")
	assert.True(t, history.Versions[0].IsCurrent)
	assert.Greater(t, history.Versions[0].Version, beforeOntology[pyProbeUniqueID].GetVersion())

	probe := showNodeContract(t, ctx, bin, env, pyProbeUniqueID)
	resolved := publishedOntologyOf(t, probe.DocumentYAML)
	assert.Equal(t, "e2e", resolved.Domain, "node contract prints the domain the ontology gives the service")
	assert.Contains(t, resolved.objectLinks(), ontologyBaseURL+"/ProbeRow", "node contract prints the link to the thing as an absolute URL")
	requireLiveContract(t, ctx, clients, ontologyID, probe)
	live := getS3ObjectIfPresent(t, ctx, clients, liveFolderKey(pyProbeUniqueID+".odcs.yaml"))
	require.NotNil(t, live, "the live folder holds %s", pyProbeUniqueID)
	publishedLive := publishedOntologyOf(t, string(live))
	assert.Equal(t, "e2e", publishedLive.Domain, "the live file carries the domain")
	assert.Contains(t, publishedLive.objectLinks(), ontologyBaseURL+"/ProbeRow", "the live file carries the absolute link")
	t.Logf("✅ ontology release %s promoted with nothing validated: %s used by %d nodes, domain %q, link %s/ProbeRow",
		ontologyID, wantVersion, len(pyFixtureNodes), resolved.Domain, ontologyBaseURL)

	// ── 5. an ontology that breaks a binding is rejected ─────────────────────
	liveBefore := string(live)
	probeBefore := latestContractVersion(t, ctx, clients, pyProbeUniqueID)
	breakingID := "e2e-ont-" + uuid.NewString()[:8]
	resetReleaseControllerQueue(t, ctx, clients)
	putOntologyFile(t, ctx, clients, breakingID, ontologyBreakingFile)
	status, body = submitOntologyRelease(t, clients, ontologyE2EService, breakingID)
	require.Equal(t, http.StatusAccepted, status, "POST /api/v1/releases of the breaking ontology: %v", body)
	waitForReleaseRejected(t, ctx, clients, breakingID, 3*time.Minute)

	rejected := getReleaseJSON(t, clients, breakingID)
	assert.Equal(t, "invalid_contract", rejected["reject_reason"])
	assert.Equal(t, []any{pyProbeUniqueID}, rejected["failing_nodes"],
		"the ontology breaks one binding, of a node of the other service")
	results, _ := rejected["per_node_results"].([]any)
	var named bool
	for _, raw := range results {
		r, _ := raw.(map[string]any)
		if r["node_id"] != pyProbeUniqueID {
			continue
		}
		named = true
		assert.Contains(t, r["detail"], "ProbeRow", "the failed node's detail names the thing that no longer exists")
	}
	assert.True(t, named, "per_node_results names %s: %v", pyProbeUniqueID, results)

	time.Sleep(ontologyQuietWindow)
	assert.Empty(t, triggersForRelease(t, requestedTap, breakingID), "invalid_contract is not healable, so no remediation trigger is sent")

	assert.Equal(t, liveBefore, string(getS3ObjectIfPresent(t, ctx, clients, liveFolderKey(pyProbeUniqueID+".odcs.yaml"))),
		"a rejected ontology changes no published contract")
	var indexAfter liveIndex
	require.NoError(t, json.Unmarshal(getS3ObjectIfPresent(t, ctx, clients, liveFolderKey("_index.json")), &indexAfter))
	assert.Equal(t, ontologyID, indexAfter.ReleaseID, "the live folder still names the promoted ontology release")
	probeAfter := latestContractVersion(t, ctx, clients, pyProbeUniqueID)
	assert.Equal(t, probeBefore.GetVersion(), probeAfter.GetVersion(), "a rejected ontology adds no contract version")
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT release_id FROM service_prod WHERE service_name = $1`, ontologyE2EService).Scan(&releasedAs))
	assert.Equal(t, ontologyID, releasedAs, "production keeps the ontology it had")
	t.Logf("✅ ontology release %s rejected invalid_contract on %s, with no trigger and no published change", breakingID, pyProbeUniqueID)

	// ── 6. a dbt service cannot be released as kind ontology ─────────────────
	var dbtKind string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT manifest_kind FROM service_prod WHERE service_name = 'service-1'`).Scan(&dbtKind),
		"setup seeds service_prod for service-1")
	require.Equal(t, "dbt", dbtKind)
	kindChangeID := "e2e-ont-" + uuid.NewString()[:8]
	token := mintCIToken(t, e2eDbtRepositoryID, "carolsimone/continuo-demo", kindChangeID)
	status, body = submitPublicRelease(t, clients, token, map[string]any{
		"service": "service-1", "release_id": kindChangeID, "image_tag": "unused", "kind": "ontology",
	})
	require.Equal(t, http.StatusConflict, status, "a dbt service released as kind ontology: %v", body)
	assert.Equal(t, ontologyKindChangeText, body["error"])
	var runs int
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT count(*) FROM release_pipeline_runs WHERE release_id = $1`, kindChangeID).Scan(&runs))
	assert.Zero(t, runs, "a refused release creates no run")
	t.Logf("✅ service-1 released as kind ontology answers 409: %v", body["error"])
}

// ontologyFinding is one finding of a bundle's ontology value.
type ontologyFinding struct {
	Severity   string  `json:"severity"`
	Rule       string  `json:"rule"`
	Subject    string  `json:"subject"`
	Message    string  `json:"message"`
	ContractID *string `json:"contract_id"`
}

// bundleOntology is the ontology value of a contract bundle with the switch on.
type bundleOntology struct {
	Enabled          bool              `json:"enabled"`
	Service          *string           `json:"service"`
	ReleaseID        *string           `json:"release_id"`
	SHA256           *string           `json:"sha256"`
	DeclaredServices []string          `json:"declared_services"`
	Findings         []ontologyFinding `json:"findings"`
}

// bundleOntologyOf decodes the ontology value of doc.
func bundleOntologyOf(t *testing.T, doc contractbundle.Document) bundleOntology {
	t.Helper()
	var out bundleOntology
	require.NoError(t, json.Unmarshal(doc.Ontology, &out), "decode the bundle's ontology value %s", doc.Ontology)
	return out
}

// publishedOntology is the part of a published contract the ontology fills in:
// its domain and the links of its schema objects.
type publishedOntology struct {
	Domain string `yaml:"domain"`
	Schema []struct {
		AuthoritativeDefinitions []struct {
			Type string `yaml:"type"`
			URL  string `yaml:"url"`
		} `yaml:"authoritativeDefinitions"`
	} `yaml:"schema"`
}

// publishedOntologyOf reads the ontology part of a published contract document.
func publishedOntologyOf(t *testing.T, document string) publishedOntology {
	t.Helper()
	var out publishedOntology
	require.NoError(t, yaml.Unmarshal([]byte(document), &out), "parse the contract document")
	return out
}

// objectLinks returns the url of every ontology link on the schema objects.
func (p publishedOntology) objectLinks() []string {
	var urls []string
	for _, object := range p.Schema {
		for _, d := range object.AuthoritativeDefinitions {
			if d.Type == "ontology" {
				urls = append(urls, d.URL)
			}
		}
	}
	return urls
}

// latestContractVersion returns the newest contract version catalog holds for
// uniqueID, as catalog's gRPC read returns it (the CLI's payload leaves out the
// ontology version). catalog restarts during the test, so the read is retried
// while its connection comes back.
func latestContractVersion(t *testing.T, ctx context.Context, clients *testClients, uniqueID string) *catalogv1.ContractVersionView {
	t.Helper()
	var latest *catalogv1.ContractVersionView
	pollUntil(t, ctx, time.Minute, time.Second, func() (bool, error) {
		resp, err := clients.catalogClient.ListNodeContractVersions(ctx, &catalogv1.ListNodeContractVersionsRequest{UniqueId: uniqueID})
		if err != nil {
			return false, nil
		}
		if len(resp.GetVersions()) == 0 {
			return false, nil
		}
		latest = slices.MaxFunc(resp.GetVersions(), func(a, b *catalogv1.ContractVersionView) int {
			return int(a.GetVersion() - b.GetVersion())
		})
		return true, nil
	}, fmt.Sprintf("catalog holds no contract version of %s", uniqueID))
	return latest
}

// putOntologyFile uploads text as the ontology service's ontology.yaml for
// releaseID, the object a release of kind ontology reads, before the release is
// posted.
func putOntologyFile(t *testing.T, ctx context.Context, clients *testClients, releaseID, text string) {
	t.Helper()
	putS3Object(t, ctx, clients, fmt.Sprintf("%s/%s/%s", ontologyE2EService, releaseID, ontologyArtifact), []byte(text))
}

// submitOntologyRelease posts a release of kind ontology through the public
// API as the CI pipeline of the py-probe repository, and returns the status and
// body of the answer.
func submitOntologyRelease(t *testing.T, clients *testClients, service, releaseID string) (int, map[string]any) {
	t.Helper()
	token := mintCIToken(t, e2ePyRepositoryID, "carolsimone/continuo-py-demo", releaseID)
	return submitPublicRelease(t, clients, token, map[string]any{
		"service": service, "release_id": releaseID, "image_tag": "unused", "kind": "ontology",
	})
}

// forgetOntologyServices removes the production pointers of the two services
// the test releases, and any release of them still in flight.
func forgetOntologyServices(t *testing.T, clients *testClients) {
	t.Helper()
	ctx := context.Background()
	resetReleaseControllerQueue(t, ctx, clients)
	_, err := clients.releaseDB.ExecContext(ctx,
		`DELETE FROM service_prod WHERE service_name IN ($1, $2)`, ontologyE2EService, pyE2EService)
	require.NoError(t, err, "delete the service_prod rows of %s and %s", ontologyE2EService, pyE2EService)
}

// ontologyProcess is one service whose process the ontology switch restarts.
type ontologyProcess struct {
	container string
	// env is the setting the service reads, as the relaunched process gets it.
	env func(on bool) string
	// command starts the service inside its container, and log is the file
	// its output goes to.
	command, log string
	// health is the URL that answers 200 once the service runs.
	health string
	// processName is set for a service whose container also runs this test:
	// restarting the container would end the test, so only the process of
	// that name is stopped.
	processName string
}

// ontologyProcesses are restarted with the switch flipped: topology-controller
// and release-controller read ONTOLOGY_ENABLED, and catalog reads
// ONTOLOGY_BASE_URL, which is set only while the switch is on.
var ontologyProcesses = []ontologyProcess{
	{
		container: "topology-controller",
		env:       func(on bool) string { return fmt.Sprintf("ONTOLOGY_ENABLED=%t", on) },
		command:   "cd /app && PYTHONPATH=/app/proto uv run python main.py > /tmp/mc.log 2>&1",
		log:       "/tmp/mc.log",
		health:    "http://topology-controller:8086/health",
	},
	{
		container: "release-controller",
		env:       func(on bool) string { return fmt.Sprintf("ONTOLOGY_ENABLED=%t", on) },
		command:   "cd /app/release-controller && go run . > /tmp/release-controller.log 2>&1",
		log:       "/tmp/release-controller.log",
		health:    "http://release-controller:8088/healthz",
	},
	{
		container: "catalog",
		env: func(on bool) string {
			if on {
				return "ONTOLOGY_BASE_URL=" + ontologyBaseURL
			}
			return "ONTOLOGY_BASE_URL="
		},
		command:     "cd /app/catalog && go run . > /tmp/catalog.log 2>&1",
		log:         "/tmp/catalog.log",
		health:      "http://catalog:8087/health",
		processName: "catalog",
	},
}

// setOntology restarts topology-controller, release-controller and catalog
// with the ontology switch set to on, in parallel, and waits until each answers
// its health check. topology-controller and release-controller restart their
// container, as setMaintenance does; catalog's container runs this test, so
// only catalog's process is stopped and started again. The relaunch inherits
// the container's compose environment except for the overridden setting.
func setOntology(t *testing.T, ctx context.Context, on bool) {
	t.Helper()
	start := time.Now()
	// A relaunch that dies on startup leaves the health URL unreachable until
	// the poll times out; the log tails name the cause.
	defer func() {
		if t.Failed() {
			logOntologyProcessTails(t)
		}
	}()
	errs := make(chan error, len(ontologyProcesses))
	var wg sync.WaitGroup
	for _, p := range ontologyProcesses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- p.relaunch(ctx, on)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for _, p := range ontologyProcesses {
		pollUntil(t, ctx, ontologySwitchBudget, time.Second, func() (bool, error) {
			resp, err := http.Get(p.health) //nolint:gosec,noctx // fixed health URL
			if err != nil {
				return false, nil
			}
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK, nil
		}, p.container+" did not become healthy after the ontology switch restart")
	}
	t.Logf("ontology=%t restart took %s", on, time.Since(start).Round(time.Second))
}

// relaunch stops the service and starts it again with its setting for on.
func (p ontologyProcess) relaunch(ctx context.Context, on bool) error {
	if p.processName == "" {
		if out, err := exec.CommandContext(ctx, "docker", "restart", "-t", "20", p.container).CombinedOutput(); err != nil { //nolint:gosec // fixed container names
			return fmt.Errorf("docker restart %s: %v: %s", p.container, err, out)
		}
	} else if err := stopProcess(ctx, p.container, p.processName); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "docker", "exec", "-d", "-e", p.env(on), p.container, "bash", "-c", p.command) //nolint:gosec // fixed arguments
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("relaunch %s: %v: %s", p.container, err, out)
	}
	return nil
}

// stopProcess ends the process named name in container with SIGTERM and waits
// for it to exit, then with SIGKILL when it has not exited after a minute.
// pkill and pgrep match the process's own name exactly, so the `go test` of
// this suite, which runs in the same container, is not touched.
func stopProcess(ctx context.Context, container, name string) error {
	run := func(args ...string) (matched bool, err error) {
		out, err := exec.CommandContext(ctx, "docker", append([]string{"exec", container}, args...)...).CombinedOutput() //nolint:gosec // fixed container and process names
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return false, nil // no process matched
		}
		if err != nil {
			return false, fmt.Errorf("docker exec %s %s: %v: %s", container, strings.Join(args, " "), err, out)
		}
		return true, nil
	}
	if _, err := run("pkill", "-x", name); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Minute)
	for {
		running, err := run("pgrep", "-x", name)
		if err != nil {
			return err
		}
		if !running {
			return nil
		}
		if time.Now().After(deadline) {
			if _, err := run("pkill", "-KILL", "-x", name); err != nil {
				return err
			}
			deadline = time.Now().Add(30 * time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// logOntologyProcessTails writes the last lines of each relaunched service's
// log into the test output.
func logOntologyProcessTails(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, p := range ontologyProcesses {
		out, err := exec.CommandContext(ctx, "docker", "exec", p.container, "tail", "-n", "50", p.log).CombinedOutput() //nolint:gosec // fixed container names
		if err != nil {
			t.Logf("tail of %s log failed: %v: %s", p.container, err, out)
			continue
		}
		t.Logf("last lines of %s:%s:\n%s", p.container, p.log, out)
	}
}
