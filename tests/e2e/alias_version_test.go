package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The aliased and versioned models of service-1 (schedule e2e-alias-version).
// alias_src declares the alias alias_src_tbl and alias_dep reads it through
// ref(); ver_src is a versioned model whose version 2 dbt builds as ver_src_v2,
// and ver_dep reads that version through ref(). A node's unique id carries the
// declared name, with a versioned model's version after it.
const (
	aliasSrcUniqueID = "e2e_schema.alias_src"
	aliasDepUniqueID = "e2e_schema.alias_dep"
	verSrcUniqueID   = "e2e_schema.ver_src.v2"
	verDepUniqueID   = "e2e_schema.ver_dep"
)

// TestE2E_AliasedAndVersionedModels_ValidateWithTheirDependants proves that a
// release validates an aliased and a versioned model together with the models
// that read them:
//
//  1. production holds every baseline node except the four models, so a
//     release of service-1 changes exactly those four;
//  2. its validation.requested:v1 names exactly the four, carries
//     relation_name alias_src_tbl on alias_src and ver_src_v2 on ver_src.v2
//     (and none on the dependants, whose relation is their declared name), and
//     names each dependant's model among its upstream_node_ids;
//  3. the release promotes. No run of e2e-alias-version ever builds the four in
//     production, so each dependant's validation could only read the
//     candidate schema's alias_src_tbl and ver_src_v2, which its upstream's
//     validation built there.
func TestE2E_AliasedAndVersionedModels_ValidateWithTheirDependants(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: assertValidationRequestedNodes (8m) + the payload read
	// (2m) + waitForReleasePromoted (12m, two validation levels) +
	// waitForTopologySwap (2m) = 24m.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-aliasver-" + uuid.NewString()[:8]
	changedService := "service-1"
	t.Logf("release_id=%s changed_service=%s", releaseID, changedService)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag, "image_tag missing for %s", changedService)

	changed := []string{aliasSrcUniqueID, aliasDepUniqueID, verSrcUniqueID, verDepUniqueID}
	excluded := make(map[string]bool, len(changed))
	for _, id := range changed {
		excluded[id] = true
	}
	found := map[string]bool{}
	var prodNodes []map[string]string
	for svc, si := range allServices {
		for _, n := range si.nodes {
			if excluded[n.uniqueID] {
				found[n.uniqueID] = true
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "service": svc, "content_hash": n.contentHash,
			})
		}
	}
	for _, id := range changed {
		require.True(t, found[id],
			"%s not found in any baseline manifest — are the models in service-1 and the image rebuilt + manifests re-uploaded?", id)
	}
	t.Logf("seeded prod snapshot with %d nodes (the four aliased and versioned models excluded)", len(prodNodes))

	// Tap the stream before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the tap recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allServices)

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	assertValidationRequestedNodes(t, ctx, validationTap, releaseID, changed)
	nodes := validationRequestedNodes(t, ctx, validationTap, releaseID)
	assert.Equal(t, "alias_src_tbl", nodes[aliasSrcUniqueID].RelationName, "alias_src writes its alias")
	assert.Equal(t, "ver_src_v2", nodes[verSrcUniqueID].RelationName, "version 2 of ver_src writes ver_src_v2")
	assert.Empty(t, nodes[aliasDepUniqueID].RelationName, "alias_dep writes its declared name")
	assert.Empty(t, nodes[verDepUniqueID].RelationName, "ver_dep writes its declared name")
	assert.Contains(t, nodes[aliasDepUniqueID].UpstreamNodeIDs, aliasSrcUniqueID,
		"alias_dep reads alias_src through ref(), so its validation waits for alias_src's")
	assert.Contains(t, nodes[verDepUniqueID].UpstreamNodeIDs, verSrcUniqueID,
		"ver_dep reads version 2 of ver_src through ref(), so its validation waits for that version's")

	waitForReleasePromoted(t, ctx, clients, releaseID, 12*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, aliasDepUniqueID, 2*time.Minute)
	t.Log("✅ the aliased and versioned models validated with their dependants, and the release promoted")
}

// validationRequestNode is one node of a validation.requested:v1 payload, as
// far as the alias and version test reads it.
type validationRequestNode struct {
	UniqueID        string   `json:"unique_id"`
	RelationName    string   `json:"relation_name"`
	UpstreamNodeIDs []string `json:"upstream_node_ids"`
}

// validationRequestedNodes waits for releaseID's validation.requested:v1 entry
// in the tap and returns its nodes by unique id.
func validationRequestedNodes(t *testing.T, ctx context.Context, validationTap *streamTap, releaseID string) map[string]validationRequestNode {
	t.Helper()
	var nodes map[string]validationRequestNode
	pollUntil(t, ctx, 2*time.Minute, 1*time.Second, func() (bool, error) {
		for _, msg := range validationTap.Entries() {
			payload, _ := msg.Values["payload"].(string)
			if payload == "" {
				continue
			}
			var p struct {
				ReleaseID string                  `json:"release_id"`
				Nodes     []validationRequestNode `json:"nodes"`
			}
			if json.Unmarshal([]byte(payload), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			nodes = make(map[string]validationRequestNode, len(p.Nodes))
			for _, n := range p.Nodes {
				nodes[n.UniqueID] = n
			}
			return true, nil
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for validation.requested:v1 for release %s", releaseID))
	return nodes
}
