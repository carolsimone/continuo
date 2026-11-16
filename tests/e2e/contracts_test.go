package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	// pyProbeContract is the py_probe node's contract file, by the path the
	// bundle names it.
	pyProbeContract = "contracts/py_probe.odcs.yaml"

	// pyProbeTeam is the team.name the py_probe contract declares, and
	// pyProbeOtherTeam the one the second release of the publish test gives it.
	// The two contracts differ in that one field, which is no part of the
	// node's code, so its content_hash stays the same.
	pyProbeTeam      = "e2e"
	pyProbeOtherTeam = "e2e-contracts"

	// contractsBudget covers catalog reading a promoted release's contract
	// bundle, recording its versions and rewriting the live folder.
	contractsBudget = 2 * time.Minute
)

// TestE2E_PythonODCSRelease_IsPromotedAndPublished proves a Python contract's
// way from an odcs-bundle.json to the readers of continuo:
//
//  1. the py-probe service is released as python-odcs, validated and
//     promoted;
//  2. catalog records the py_probe contract as version 1 (on a stack where an
//     earlier run already promoted it, the version after the highest one it
//     holds, or that one again when the contract is unchanged), and
//     `continuo node contract e2e_schema.py_probe` prints it with the
//     contract's team.name;
//  3. the live folder in MinIO holds e2e_schema.py_probe.odcs.yaml with that
//     version as a string, and _index.json, written last, lists it;
//  4. a second release that changes only team.name validates nothing, since
//     no node's content_hash changes, and makes the next version, with the
//     same content_hash and the new team.name.
func TestE2E_PythonODCSRelease_IsPromotedAndPublished(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// additive polls: the first release (validation 8m within the promotion's
	// 10m, + the swap 2m) + contracts 2m + the second release 10m + contracts
	// 2m = 26m.
	ctx, cancel := context.WithTimeout(context.Background(), 28*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	bin := buildCLI(t, ctx)
	env := []string{
		"CONTINUO_CATALOG_ADDR=" + getEnv("CATALOG_HOST", "catalog") + ":50052",
		"CONTINUO_ACTOR=e2e-cli",
	}
	before := highestContractVersion(t, ctx, clients, pyProbeUniqueID)

	// ── 1. the py-probe service is promoted as python-odcs ──────────────────
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)
	firstID, cleanup := promotePythonFixtureRelease(t, ctx, clients, validationTap)
	defer cleanup()

	// ── 2. catalog records the contract, and the CLI prints it ───────────────
	waitForLiveIndexOf(t, ctx, clients, firstID, contractsBudget)
	first := showNodeContract(t, ctx, bin, env, pyProbeUniqueID)
	if before == 0 {
		require.EqualValues(t, 1, first.Version, "the node's first contract is version 1")
	} else {
		require.Contains(t, []int64{before, before + 1}, first.Version,
			"a promotion adds one version when it changes the contract and none when it does not (highest before: %d)", before)
	}
	if first.Version > before {
		assert.Equal(t, firstID, first.ReleaseID, "a new version records the release that made it current")
	}
	assert.True(t, first.IsCurrent, "node contract prints the current version")
	assert.Equal(t, pyBundleContracts, first.ContinuoContracts, "the version records the library that read it")
	assert.Equal(t, pyProbeTeam, documentTeam(t, first.DocumentYAML), "the document carries the contract's team.name")
	assert.Equal(t, strconv.FormatInt(first.Version, 10), documentVersion(t, first.DocumentYAML),
		"the published document's version is the counted version, as a string")
	t.Logf("continuo node contract %s: version %d, %s", pyProbeUniqueID, first.Version, first.ContractHash)

	// ── 3. the live folder holds it ──────────────────────────────────────────
	requireLiveContract(t, ctx, clients, firstID, first)

	// ── 4. a team.name-only change validates nothing and makes a version ─────
	ownerChanged := strings.Replace(pyProbeContractText(t), "team:\n  name: "+pyProbeTeam+"\n", "team:\n  name: "+pyProbeOtherTeam+"\n", 1)
	require.NotEqual(t, pyProbeContractText(t), ownerChanged, "the py_probe contract declares team.name %q", pyProbeTeam)
	secondID := "e2e-py-" + uuid.NewString()[:8]
	resetReleaseControllerQueue(t, ctx, clients)
	putODCSBundle(t, ctx, clients, pyE2EService, secondID, pyFixtureBundle(t, ctx, map[string]string{pyProbeContract: ownerChanged}))
	postPythonRelease(t, clients, pyE2EService, secondID, pyFixtureImage, false)
	waitForReleasePromoted(t, ctx, clients, secondID, 10*time.Minute)

	detail := getReleaseJSON(t, clients, secondID)
	assert.Empty(t, detail["validation_node_ids"], "a release that changes no node's content_hash validates nothing")
	assertNoValidationRequested(t, validationTap, secondID)

	waitForLiveIndexOf(t, ctx, clients, secondID, contractsBudget)
	second := showNodeContract(t, ctx, bin, env, pyProbeUniqueID)
	assert.Equal(t, first.Version+1, second.Version, "a changed contract is the next version")
	assert.Equal(t, secondID, second.ReleaseID, "the new version records the release that made it current")
	assert.NotEqual(t, first.ContractHash, second.ContractHash, "team.name is part of the contract")
	assert.Equal(t, first.ContentHash, second.ContentHash, "team.name is no part of the node's code")
	assert.Equal(t, pyProbeOtherTeam, documentTeam(t, second.DocumentYAML), "the document carries the new team.name")
	requireLiveContract(t, ctx, clients, secondID, second)
	t.Logf("✅ %s promoted as python-odcs: contract version %d published, then version %d from a team.name change that validated nothing",
		pyProbeUniqueID, first.Version, second.Version)
}

// TestE2E_PythonODCSRelease_AnInvalidContractIsRejected proves a release whose
// bundle holds a contract continuo refuses never reaches validation: the
// py_probe contract declares the type NUMERIC(12), outside the runtime's types,
// so topology-controller rejects the release invalid_contract, naming the node
// and its file, and the public release status names the continuo-contracts
// version that refused it.
func TestE2E_PythonODCSRelease_AnInvalidContractIsRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	requireReleaseControllerHealthy(t, clients)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices)
	var prodNodes []map[string]string
	for svc, si := range allServices {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "service": svc, "content_hash": n.contentHash,
			})
		}
	}
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allServices)

	invalid := strings.Replace(pyProbeContractText(t),
		"{name: id, physicalType: INTEGER, required: true}", `{name: id, physicalType: "NUMERIC(12)", required: true}`, 1)
	require.NotEqual(t, pyProbeContractText(t), invalid, "the py_probe contract declares an INTEGER id")

	releaseID := "e2e-pyinv-" + uuid.NewString()[:8]
	putODCSBundle(t, ctx, clients, pyE2EService, releaseID, pyFixtureBundle(t, ctx, map[string]string{pyProbeContract: invalid}))
	postPythonRelease(t, clients, pyE2EService, releaseID, pyFixtureImage, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, 3*time.Minute)

	detail := getReleaseJSON(t, clients, releaseID)
	assert.Equal(t, "invalid_contract", detail["reject_reason"])
	assert.Equal(t, []any{pyProbeUniqueID}, detail["failing_nodes"], "the one invalid contract's node is the failing node")
	results, _ := detail["per_node_results"].([]any)
	var named bool
	for _, raw := range results {
		r, _ := raw.(map[string]any)
		if r["node_id"] != pyProbeUniqueID {
			continue
		}
		named = true
		assert.Equal(t, pyProbeContract, r["file_path"], "the failed node names its contract file")
		assert.Contains(t, r["detail"], "NUMERIC(12)", "the failed node's detail names the refused type")
	}
	assert.True(t, named, "per_node_results names %s: %v", pyProbeUniqueID, results)

	token := mintCIToken(t, e2ePyRepositoryID, "carolsimone/continuo-py-demo", releaseID)
	public := getPublicRelease(t, clients, token, releaseID)
	assert.Equal(t, "rejected", public["status"])
	assert.Equal(t, true, public["terminal"])
	assert.Equal(t, "invalid_contract", public["reject_reason"])
	assert.Contains(t, public["reject_detail"], pyProbeUniqueID, "the public detail names the failed node")
	assert.Equal(t, pyBundleContracts, public["continuo_contracts"],
		"the public status names the continuo-contracts version that refused the contract")
	t.Logf("✅ release %s rejected invalid_contract on %s by continuo-contracts %v", releaseID, pyProbeUniqueID, public["continuo_contracts"])
}

// cliContract is the contract `continuo node contract` prints.
type cliContract struct {
	UniqueID          string `json:"unique_id"`
	Version           int64  `json:"version"`
	ContractHash      string `json:"contract_hash"`
	ReleaseID         string `json:"release_id"`
	ContentHash       string `json:"content_hash"`
	IsCurrent         bool   `json:"is_current"`
	ContinuoContracts string `json:"continuo_contracts"`
	DocumentYAML      string `json:"document_yaml"`
}

// showNodeContract runs `continuo node contract <uniqueID>` and returns the
// contract it prints.
func showNodeContract(t *testing.T, ctx context.Context, bin string, env []string, uniqueID string) cliContract {
	t.Helper()
	var shown struct {
		Contract cliContract `json:"contract"`
	}
	runCLI(t, ctx, bin, env, &shown, "node", "contract", uniqueID)
	require.Equal(t, uniqueID, shown.Contract.UniqueID)
	require.NotEmpty(t, shown.Contract.DocumentYAML, "node contract prints the contract's document")
	return shown.Contract
}

// highestContractVersion returns the highest contract version catalog holds
// for uniqueID, 0 when it holds none.
func highestContractVersion(t *testing.T, ctx context.Context, clients *testClients, uniqueID string) int64 {
	t.Helper()
	resp, err := clients.catalogClient.ListNodeContractVersions(ctx, &catalogv1.ListNodeContractVersionsRequest{UniqueId: uniqueID})
	require.NoError(t, err, "ListNodeContractVersions(%s)", uniqueID)
	var highest int64
	for _, v := range resp.GetVersions() {
		highest = max(highest, v.GetVersion())
	}
	return highest
}

// pyProbeContractText returns the py_probe contract file as the fixture holds it.
func pyProbeContractText(t *testing.T) string {
	t.Helper()
	text, ok := odcsContracts(t, pyFixtureRepo, pyFixtureRoot)[pyProbeContract]
	require.True(t, ok, "the fixture holds %s", pyProbeContract)
	return text
}

// documentTeam returns the team.name of an ODCS document.
func documentTeam(t *testing.T, document string) string {
	t.Helper()
	var doc struct {
		Team struct {
			Name string `yaml:"name"`
		} `yaml:"team"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(document), &doc), "parse the contract document")
	return doc.Team.Name
}

// documentVersion returns the version of an ODCS document, which must be a
// string.
func documentVersion(t *testing.T, document string) string {
	t.Helper()
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(document), &doc), "parse the contract document")
	version, ok := doc["version"].(string)
	require.True(t, ok, "the document's version must be a string, got %#v", doc["version"])
	return version
}

// liveIndex is the part of a live folder's _index.json the checks read.
type liveIndex struct {
	PromotionSeq int64           `json:"promotion_seq"`
	ReleaseID    string          `json:"release_id"`
	Files        []liveIndexFile `json:"files"`
}

// liveIndexFile is one file _index.json lists.
type liveIndexFile struct {
	UniqueID     string `json:"unique_id"`
	Key          string `json:"key"`
	Version      int64  `json:"version"`
	ContractHash string `json:"contract_hash"`
}

// liveFolderKey is the object key of name in the default tenant's live
// contracts folder.
func liveFolderKey(name string) string {
	return fmt.Sprintf("tenants/%s/contracts/live/%s", events.DefaultTenantID, name)
}

// waitForLiveIndexOf polls the live folder's _index.json until catalog has
// written it for releaseID's promotion, with no file still at version 0 (a
// file an interim index lists while a rewrite is part way). catalog writes the
// index last, after the contract versions of the promotion are recorded.
func waitForLiveIndexOf(t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration) liveIndex {
	t.Helper()
	var index liveIndex
	var last string
	pollUntilDescribed(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		body := getS3ObjectIfPresent(t, ctx, clients, liveFolderKey("_index.json"))
		if body == nil {
			return false, nil
		}
		last = string(body)
		index = liveIndex{}
		if json.Unmarshal(body, &index) != nil || index.ReleaseID != releaseID {
			return false, nil
		}
		for _, f := range index.Files {
			if f.Version == 0 {
				return false, nil
			}
		}
		return true, nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for the live folder's _index.json of release %s (last read: %s)", releaseID, last)
	})
	return index
}

// requireLiveContract asserts the live folder publishes contract as written
// for releaseID's promotion: its file carries the version as a string, and
// _index.json lists the file with that version and contract_hash.
func requireLiveContract(t *testing.T, ctx context.Context, clients *testClients, releaseID string, contract cliContract) {
	t.Helper()
	index := waitForLiveIndexOf(t, ctx, clients, releaseID, contractsBudget)
	fileName := contract.UniqueID + ".odcs.yaml"
	i := slices.IndexFunc(index.Files, func(f liveIndexFile) bool { return f.UniqueID == contract.UniqueID })
	require.GreaterOrEqual(t, i, 0, "_index.json lists %s", contract.UniqueID)
	assert.Equal(t, fileName, index.Files[i].Key)
	assert.Equal(t, contract.Version, index.Files[i].Version, "_index.json lists the current version")
	assert.Equal(t, contract.ContractHash, index.Files[i].ContractHash)

	body := getS3ObjectIfPresent(t, ctx, clients, liveFolderKey(fileName))
	require.NotNil(t, body, "the live folder holds %s", fileName)
	assert.Equal(t, strconv.FormatInt(contract.Version, 10), documentVersion(t, string(body)),
		"%s carries the counted version as a string", fileName)
	assert.Equal(t, documentTeam(t, contract.DocumentYAML), documentTeam(t, string(body)),
		"%s is the contract node contract prints", fileName)
}

// assertNoValidationRequested asserts no validation.requested:v1 message names
// releaseID. Called once the release has promoted, so a request, had there
// been one, would be on the tap.
func assertNoValidationRequested(t *testing.T, validationTap *streamTap, releaseID string) {
	t.Helper()
	for _, msg := range validationTap.Entries() {
		payload, _ := msg.Values["payload"].(string)
		var p struct {
			ReleaseID string `json:"release_id"`
		}
		if json.Unmarshal([]byte(payload), &p) == nil && p.ReleaseID == releaseID {
			t.Fatalf("validation.requested emitted for release %s, which changes no node's content_hash", releaseID)
		}
	}
}
