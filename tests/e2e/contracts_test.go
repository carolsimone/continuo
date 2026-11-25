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
	"github.com/carolsimone/continuo/pkg/contractbundle"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
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

	// topologyContractsVersion is the continuo-contracts version
	// topology-controller pins (topology-controller/pyproject.toml). That
	// library reads and checks every contract of a release, and the release's
	// parse result and contract bundle name it.
	topologyContractsVersion = "0.4.1"

	// dbtProbeTeam and dbtProbeSchedule are what service-1's dbt project
	// (dbt/projects/service-1/dbt_project.yml) gives the rel_probe model: its
	// +meta owner, which its contract carries as team.name, and its schedule
	// tag, which its contract carries as the schedule property.
	dbtProbeTeam     = "test"
	dbtProbeSchedule = "rel-probe"

	// dbtContractsReleaseProject is the project the invalid-contract test
	// releases. Its compile Job uploads the freshly compiled manifest to
	// <project>/<release_id>/manifest.json, which would overwrite a manifest
	// doctored under that key, so the invalid contract is staged in
	// dbtContractsPinnedProject's production-pinned manifest, which nothing in
	// the release rewrites (see parseFixReleaseProject).
	dbtContractsReleaseProject = parseFixReleaseProject
	dbtContractsPinnedProject  = parseFixBrokenProject

	// dbtInvalidContractNodeID is the model of dbtContractsPinnedProject whose
	// criticality the invalid-contract test sets to dbtInvalidCriticality, a
	// value outside REGULATORY, CORE and SECONDARY. The model reads no table
	// and no model reads it, so the edit makes no failure but its own contract.
	dbtInvalidContractNodeID = unfixableInfraUniqueID
	dbtInvalidCriticality    = "high"

	// dbtSeedHeaderReleaseProject is the project the seed-header test
	// releases, and dbtSeedHeaderPinnedProject the project whose
	// production-pinned manifest carries the doctored seed: service-1 is the
	// only project with seeds, and the compile Job of a released project
	// overwrites the manifest under its own key (see dbtContractsReleaseProject).
	dbtSeedHeaderReleaseProject = "service-2"
	dbtSeedHeaderPinnedProject  = "service-1"

	// dbtSeedHeaderNodeID is the seed of dbtSeedHeaderPinnedProject
	// (dbt/projects/service-1/seeds) whose id column the seed-header test drops
	// from the declared columns, dbtSeedHeaderColumn, while the CSV header
	// that the compile extras record keeps it. The seed declares no test, so
	// the edit makes no failure but its own contract.
	dbtSeedHeaderNodeID = seedSchemaName + ".seed_table_2"
	dbtSeedHeaderColumn = "id"
)

// TestE2E_PythonODCSRelease_IsPromotedAndPublished proves a Python contract's
// way from an odcs-bundle.json to the readers of continuo:
//
//  1. the py-probe project is released as python-odcs, validated and
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

	bin, env := contractsCLI(t, ctx)
	before := highestContractVersion(t, ctx, clients, pyProbeUniqueID)

	// ── 1. the py-probe project is promoted as python-odcs ──────────────────
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)
	firstID, cleanup := promotePythonFixtureRelease(t, ctx, clients, validationTap, defaultPythonReleaseBudget)
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
	assert.Equal(t, topologyContractsVersion, first.ContinuoContracts, "the version records the library that read it")
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
	putODCSBundle(t, ctx, clients, pyE2EProject, secondID, pyFixtureBundle(t, ctx, map[string]string{pyProbeContract: ownerChanged}))
	postPythonRelease(t, clients, pyE2EProject, secondID, pyFixtureImage, false)
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

	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects)
	var prodNodes []map[string]string
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "project": svc, "content_hash": n.contentHash,
			})
		}
	}
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allProjects)

	invalid := strings.Replace(pyProbeContractText(t),
		"{name: id, physicalType: INTEGER, required: true,", `{name: id, physicalType: "NUMERIC(12)", required: true,`, 1)
	require.NotEqual(t, pyProbeContractText(t), invalid, "the py_probe contract declares an INTEGER id")

	releaseID := "e2e-pyinv-" + uuid.NewString()[:8]
	putODCSBundle(t, ctx, clients, pyE2EProject, releaseID, pyFixtureBundle(t, ctx, map[string]string{pyProbeContract: invalid}))
	postPythonRelease(t, clients, pyE2EProject, releaseID, pyFixtureImage, false)
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
	assert.Equal(t, topologyContractsVersion, public["continuo_contracts"],
		"the public status names the continuo-contracts version that refused the contract")
	t.Logf("✅ release %s rejected invalid_contract on %s by continuo-contracts %v", releaseID, pyProbeUniqueID, public["continuo_contracts"])
}

// TestE2E_DbtContracts_AnInvalidContractIsRejected proves a dbt node whose
// definitions make an invalid contract rejects the release before validation:
// service-2's production-pinned manifest is a copy of its baseline with the
// criticality of one model set to a value outside REGULATORY, CORE and
// SECONDARY, and a release of service-1 assembles that manifest.
// topology-controller rejects the release invalid_contract, naming the model
// and its file, and the model's parse result names the criticality field. No
// other node fails, and the release never reaches validation.
func TestE2E_DbtContracts_AnInvalidContractIsRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// the rejection (service-1's compile Job and the parse leg, 10m) plus the
	// baseline reads and the seeding before it.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-dbtinv-" + uuid.NewString()[:8]
	t.Logf("release_id=%s released_project=%s invalid_node=%s (project %s)",
		releaseID, dbtContractsReleaseProject, dbtInvalidContractNodeID, dbtContractsPinnedProject)

	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects, "no baseline manifests in S3 — setup.sh must run before the e2e suite")
	released, ok := allProjects[dbtContractsReleaseProject]
	require.True(t, ok, "%s has no baseline manifest", dbtContractsReleaseProject)
	require.NotEmpty(t, released.imageTag)
	pinned, ok := allProjects[dbtContractsPinnedProject]
	require.True(t, ok, "%s has no baseline manifest", dbtContractsPinnedProject)

	// Production holds every baseline node, so the release changes nothing
	// the parse does not reject.
	var prodNodes []map[string]string
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{"unique_id": n.uniqueID, "project": svc, "content_hash": n.contentHash})
		}
	}
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allProjects)

	// Set the criticality of one model in a copy of service-2's baseline
	// manifest, copy the baseline's compile extras beside it, and pin
	// service-2's production pointer at it, so the assembled set the parse leg
	// reads carries the invalid contract.
	pinnedReleaseID := releaseID + "-src"
	baselineKey := fmt.Sprintf("%s/%s/manifest.json", dbtContractsPinnedProject, e2eBaselineReleaseID)
	doc, err := decodeJSONDoc(getS3Object(t, ctx, clients, baselineKey))
	require.NoError(t, err)
	filePath := setModelCriticality(t, doc, dbtInvalidContractNodeID, dbtInvalidCriticality)
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	putS3Object(t, ctx, clients,
		fmt.Sprintf("%s/%s/manifest.json", dbtContractsPinnedProject, pinnedReleaseID), body)
	copyBaselineExtras(t, ctx, clients, dbtContractsPinnedProject, pinnedReleaseID)
	pinServiceProd(t, ctx, clients, dbtContractsPinnedProject, pinnedReleaseID, pinned.imageTag)
	// Put the pointer back so no later test parses the doctored manifest.
	// Deferred, so it runs when the test function returns, while the pool is
	// open.
	defer pinServiceProd(t, context.Background(), clients,
		dbtContractsPinnedProject, e2eBaselineReleaseID, pinned.imageTag)

	postRelease(t, clients, dbtContractsReleaseProject, releaseID, released.imageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, 10*time.Minute)

	detail := getReleaseJSON(t, clients, releaseID)
	assert.Equal(t, string(model.RejectReasonInvalidContract), detail["reject_reason"])
	assert.Equal(t, []any{dbtInvalidContractNodeID}, detail["failing_nodes"],
		"the one invalid contract's model is the failing node")
	assert.Contains(t, detail["reject_detail"], dbtInvalidContractNodeID, "the release's reject detail names the model")
	assert.Empty(t, detail["validation_node_ids"], "a release rejected at the parse never reaches validation")

	results, _ := detail["per_node_results"].([]any)
	var named bool
	for _, raw := range results {
		r, _ := raw.(map[string]any)
		if r["node_id"] != dbtInvalidContractNodeID {
			continue
		}
		named = true
		assert.Equal(t, "parse", r["stage"], "an invalid contract is a parse result")
		assert.Equal(t, "failed", r["status"])
		assert.Equal(t, filePath, r["file_path"], "the failed model names its file")
		assert.Contains(t, r["detail"], "criticality", "the failed model's detail names the field")
	}
	assert.True(t, named, "per_node_results names %s: %v", dbtInvalidContractNodeID, results)
	t.Logf("✅ release %s rejected invalid_contract on %s (%s)", releaseID, dbtInvalidContractNodeID, filePath)
}

// TestE2E_DbtContracts_ASeedWithAnUndeclaredHeaderColumnIsRejected proves a
// seed whose CSV header has a column its contract does not declare rejects the
// release before validation: service-1's production-pinned manifest is a copy
// of its baseline with the id column removed from the declared columns of
// seed_table_2, its compile extras (the CSV header the compile recorded) beside
// it unchanged, and a release of service-2 assembles that manifest.
// topology-controller rejects the release invalid_contract, naming the seed,
// its file and the column. No other node fails.
func TestE2E_DbtContracts_ASeedWithAnUndeclaredHeaderColumnIsRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	// the rejection (service-2's compile Job and the parse leg, 10m) plus the
	// baseline reads and the seeding before it.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-seedhdr-" + uuid.NewString()[:8]
	t.Logf("release_id=%s released_project=%s invalid_seed=%s (project %s)",
		releaseID, dbtSeedHeaderReleaseProject, dbtSeedHeaderNodeID, dbtSeedHeaderPinnedProject)

	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects, "no baseline manifests in S3 — setup.sh must run before the e2e suite")
	released, ok := allProjects[dbtSeedHeaderReleaseProject]
	require.True(t, ok, "%s has no baseline manifest", dbtSeedHeaderReleaseProject)
	require.NotEmpty(t, released.imageTag)
	pinned, ok := allProjects[dbtSeedHeaderPinnedProject]
	require.True(t, ok, "%s has no baseline manifest", dbtSeedHeaderPinnedProject)

	// Production holds every baseline node, so the release changes nothing
	// the parse does not reject.
	var prodNodes []map[string]string
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{"unique_id": n.uniqueID, "project": svc, "content_hash": n.contentHash})
		}
	}
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allProjects)

	// Drop the declared id column of seed_table_2 in a copy of service-1's
	// baseline manifest, copy the baseline's compile extras beside it, and pin
	// service-1's production pointer at it, so the assembled set the parse leg
	// reads holds a seed whose header and declared columns differ.
	pinnedReleaseID := releaseID + "-src"
	baselineKey := fmt.Sprintf("%s/%s/manifest.json", dbtSeedHeaderPinnedProject, e2eBaselineReleaseID)
	doc, err := decodeJSONDoc(getS3Object(t, ctx, clients, baselineKey))
	require.NoError(t, err)
	seedManifestID, filePath := dropDeclaredColumn(t, doc, dbtSeedHeaderNodeID, dbtSeedHeaderColumn)
	requireSeedHeaderHolds(t,
		getS3ObjectIfPresent(t, ctx, clients, baselineExtrasKey(dbtSeedHeaderPinnedProject)), seedManifestID, dbtSeedHeaderColumn)
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	putS3Object(t, ctx, clients,
		fmt.Sprintf("%s/%s/manifest.json", dbtSeedHeaderPinnedProject, pinnedReleaseID), body)
	copyBaselineExtras(t, ctx, clients, dbtSeedHeaderPinnedProject, pinnedReleaseID)
	pinServiceProd(t, ctx, clients, dbtSeedHeaderPinnedProject, pinnedReleaseID, pinned.imageTag)
	// Put the pointer back so no later test parses the doctored manifest.
	// Deferred, so it runs when the test function returns, while the pool is
	// open.
	defer pinServiceProd(t, context.Background(), clients,
		dbtSeedHeaderPinnedProject, e2eBaselineReleaseID, pinned.imageTag)

	postRelease(t, clients, dbtSeedHeaderReleaseProject, releaseID, released.imageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, 10*time.Minute)

	detail := getReleaseJSON(t, clients, releaseID)
	assert.Equal(t, string(model.RejectReasonInvalidContract), detail["reject_reason"])
	assert.Equal(t, []any{dbtSeedHeaderNodeID}, detail["failing_nodes"],
		"the one seed whose header differs from its declared columns is the failing node")
	assert.Contains(t, detail["reject_detail"], dbtSeedHeaderNodeID, "the release's reject detail names the seed")

	results, _ := detail["per_node_results"].([]any)
	var named bool
	for _, raw := range results {
		r, _ := raw.(map[string]any)
		if r["node_id"] != dbtSeedHeaderNodeID {
			continue
		}
		named = true
		assert.Equal(t, "parse", r["stage"], "a seed whose header differs from its declared columns is a parse result")
		assert.Equal(t, "failed", r["status"])
		assert.Equal(t, filePath, r["file_path"], "the failed seed names its file")
		assert.Contains(t, r["detail"], dbtSeedHeaderColumn+" is in the CSV header but not declared",
			"the failed seed's detail names the column")
	}
	assert.True(t, named, "per_node_results names %s: %v", dbtSeedHeaderNodeID, results)
	t.Logf("✅ release %s rejected invalid_contract on %s (%s): %s is in the CSV header but not declared",
		releaseID, dbtSeedHeaderNodeID, filePath, dbtSeedHeaderColumn)
}

// dropDeclaredColumn removes column from the declared columns of the seed
// uniqueID (<schema>.<table>) in the dbt manifest doc, and returns the seed's
// manifest id and original_file_path. Every other field of doc stays as it was.
func dropDeclaredColumn(t *testing.T, doc map[string]interface{}, uniqueID, column string) (manifestID, filePath string) {
	t.Helper()
	var edited int
	for id, nv := range asObjectMap(doc["nodes"]) {
		n, ok := nv.(map[string]interface{})
		if !ok || n["resource_type"] != "seed" {
			continue
		}
		schema, _ := n["schema"].(string)
		name, _ := n["name"].(string)
		if schema+"."+name != uniqueID {
			continue
		}
		columns := asObjectMap(n["columns"])
		require.Contains(t, columns, column, "%s must declare the column %s in the manifest", uniqueID, column)
		delete(columns, column)
		manifestID = id
		filePath, _ = n["original_file_path"].(string)
		edited++
	}
	require.Equal(t, 1, edited, "exactly one seed of the manifest is %s", uniqueID)
	require.NotEmpty(t, filePath, "%s must carry an original_file_path", uniqueID)
	return manifestID, filePath
}

// requireSeedHeaderHolds fails unless the compile extras (a baseline
// compile-extras.json, nil when the compile wrote none) record column in the
// CSV header of the seed manifestID: the parse compares a seed's declared
// columns with the header only when the extras name the seed.
func requireSeedHeaderHolds(t *testing.T, extras []byte, manifestID, column string) {
	t.Helper()
	require.NotNil(t, extras, "the baseline compile wrote no compile-extras.json, so no seed header is known")
	var doc struct {
		Seeds map[string]struct {
			Header []string `json:"header"`
		} `json:"seeds"`
	}
	require.NoError(t, json.Unmarshal(extras, &doc))
	seed, ok := doc.Seeds[manifestID]
	require.True(t, ok, "the compile extras name the seed %s", manifestID)
	require.Contains(t, seed.Header, column, "the CSV header of %s holds the column %s", manifestID, column)
}

// setModelCriticality sets config.meta.criticality of the model uniqueID
// (<schema>.<table>) in the dbt manifest doc to criticality, and the copy dbt
// writes in the node's own meta when there is one, and returns the model's
// original_file_path. Every other field of doc stays as it was.
func setModelCriticality(t *testing.T, doc map[string]interface{}, uniqueID, criticality string) string {
	t.Helper()
	var filePath string
	var edited int
	for _, nv := range asObjectMap(doc["nodes"]) {
		n, ok := nv.(map[string]interface{})
		if !ok || n["resource_type"] != "model" {
			continue
		}
		schema, _ := n["schema"].(string)
		name, _ := n["name"].(string)
		if schema+"."+name != uniqueID {
			continue
		}
		meta, ok := asObjectMap(n["config"])["meta"].(map[string]interface{})
		require.True(t, ok, "%s must declare config.meta in the manifest (its project's +meta owner and criticality)", uniqueID)
		require.NotEqual(t, criticality, meta["criticality"], "%s already has criticality %q", uniqueID, criticality)
		meta["criticality"] = criticality
		if nodeMeta, ok := n["meta"].(map[string]interface{}); ok {
			nodeMeta["criticality"] = criticality
		}
		filePath, _ = n["original_file_path"].(string)
		edited++
	}
	require.Equal(t, 1, edited, "exactly one model of the manifest is %s", uniqueID)
	require.NotEmpty(t, filePath, "%s must carry an original_file_path", uniqueID)
	return filePath
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

// contractsCLI builds the continuo binary and returns it with the environment
// that points it at catalog.
func contractsCLI(t *testing.T, ctx context.Context) (string, []string) {
	t.Helper()
	env := []string{
		"CONTINUO_CATALOG_ADDR=" + getEnv("CATALOG_HOST", "catalog") + ":50052",
		"CONTINUO_ACTOR=e2e-cli",
	}
	return buildCLI(t, ctx), env
}

// assertDbtNodeContract checks the contract of a dbt node a promoted release
// keeps, node being its entry in the release's topology artifact:
//
//   - the release's contract bundle holds a contract of the dbt family for it,
//     of its project, with its content_hash, whose team.name is the node's
//     owner and whose schedule property is the schedule of the topology node;
//   - catalog publishes it: once the live folder names the release,
//     `continuo node contract` prints the bundle's contract (the same
//     contract_hash) with its team.name.
func assertDbtNodeContract(t *testing.T, ctx context.Context, clients *testClients, releaseID string, node topologyartifact.Node) {
	t.Helper()
	bundle := candidateContractBundle(t, ctx, clients, releaseID)
	assert.Positive(t, bundle.Summary.ByFamily[model.ContractFamilyDbt], "the bundle counts the release's dbt contracts")

	stored, ok := bundle.Contracts[node.UniqueID]
	require.True(t, ok, "the contract bundle of %s holds a contract for %s", releaseID, node.UniqueID)
	assert.Equal(t, model.ContractFamilyDbt, stored.Family)
	assert.Equal(t, node.ProjectName, stored.Project)
	assert.Equal(t, node.ContentHash, stored.ContentHash, "the contract carries its node's code fingerprint")
	assert.NotEmpty(t, stored.ContractHash)

	var root odcsRoot
	require.NoError(t, json.Unmarshal(stored.Document, &root), "parse the bundle's contract of %s", node.UniqueID)
	assert.Equal(t, dbtProbeTeam, root.Team.Name, "the contract carries the node's owner as team.name")
	schedule, ok := root.continuoProperty("schedule")
	require.True(t, ok, "the contract of %s has a schedule property: %s", node.UniqueID, stored.Document)
	assert.Equal(t, dbtProbeSchedule, schedule)
	assert.Equal(t, schedule, node.Schedule, "the topology node's schedule is the contract's schedule property")

	waitForLiveIndexOf(t, ctx, clients, releaseID, contractsBudget)
	bin, env := contractsCLI(t, ctx)
	shown := showNodeContract(t, ctx, bin, env, node.UniqueID)
	assert.True(t, shown.IsCurrent, "node contract prints the current version")
	assert.Equal(t, stored.ContractHash, shown.ContractHash, "node contract prints the contract the release's bundle holds")
	assert.Equal(t, dbtProbeTeam, documentTeam(t, shown.DocumentYAML), "the printed document carries the contract's team.name")
	t.Logf("✅ %s: dbt contract in the bundle of %s, version %d printed by `continuo node contract` (schedule %s, team %s)",
		node.UniqueID, releaseID, shown.Version, schedule, root.Team.Name)
}

// candidateContractBundle reads the contract bundle of releaseID from MinIO,
// by the URI and SHA-256 release-controller records on the release and
// returns on GET /releases/{id}, and decodes it, which checks the stored bytes
// against that SHA-256.
func candidateContractBundle(t *testing.T, ctx context.Context, clients *testClients, releaseID string) contractbundle.Document {
	t.Helper()
	detail := getReleaseJSON(t, clients, releaseID)
	uri, _ := detail["contract_bundle_uri"].(string)
	sha, _ := detail["contract_bundle_sha256"].(string)
	key := contractbundle.Key(events.DefaultTenantID, releaseID)
	require.Equal(t, "s3://"+e2eS3Bucket+"/"+key, uri, "the release must reference its contract bundle at its deterministic key")
	require.NotEmpty(t, sha, "the release must record the SHA-256 of its contract bundle")

	doc, err := contractbundle.Decode(getS3Object(t, ctx, clients, key), sha)
	require.NoError(t, err, "the contract bundle of %s must decode", releaseID)
	require.Equal(t, releaseID, doc.ReleaseID)
	require.Equal(t, events.DefaultTenantID, doc.TenantID)
	require.Equal(t, topologyContractsVersion, doc.ContinuoContracts, "the bundle records the library that stored its contracts")
	return doc
}

// odcsRoot is the part of a stored contract document's root the dbt checks
// read: its team and its custom properties.
type odcsRoot struct {
	Team struct {
		Name string `json:"name"`
	} `json:"team"`
	CustomProperties []struct {
		Vendor   string `json:"vendor"`
		Property string `json:"property"`
		Value    any    `json:"value"`
	} `json:"customProperties"`
}

// continuoProperty returns the string value of the root's custom property name
// of the vendor continuo, and whether the root has one.
func (r odcsRoot) continuoProperty(name string) (string, bool) {
	for _, p := range r.CustomProperties {
		if p.Vendor == "continuo" && p.Property == name {
			value, ok := p.Value.(string)
			return value, ok
		}
	}
	return "", false
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
