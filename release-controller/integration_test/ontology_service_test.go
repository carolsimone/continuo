//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	httpinfra "github.com/carolsimone/continuo/release-controller/adapters/http"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postRelease(srv *httpinfra.Server, in httpinfra.ReceiveCandidateRequest) *httptest.ResponseRecorder {
	body, _ := json.Marshal(in)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body)))
	return rec
}

func ontologyRequest(releaseID, service string) httpinfra.ReceiveCandidateRequest {
	return httpinfra.ReceiveCandidateRequest{
		Service: service, ReleaseID: releaseID, ImageTag: "img", Repo: "acme/ontology", CommitSHA: "cafebabe", Kind: "ontology",
	}
}

// With the switch on, the ontology service releases through the whole path
// against real Postgres: POST /releases activates it straight into Parsing with
// the ontology file as its artifact, and a parse that names no node change
// promotes it and stores its production pointer with the kind ontology. While
// it is the install's ontology, a second ontology service and a node kind for
// the same service are conflicts.
func TestIntegration_OntologyRelease_PromotesWithNoNodes(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()
	deps.OntologyEnabled = true
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted, postRelease(srv, ontologyRequest("it-ont-1", "company-ontology")).Code)

	r, err := deps.NewUoW().RunRepo().Get(ctx, "it-ont-1")
	require.NoError(t, err)
	require.NotNil(t, r)
	assert.Equal(t, pipeline.StatusParsing, r.Status(), "an ontology release has no compile leg")
	assert.Equal(t, release.ManifestKindOntology, r.ManifestKind())

	entries, err := deps.NewUoW().OutboxRepo().GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "exactly one outbox row: release.requested:v1")
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)
	var requested struct {
		ManifestKeys []struct {
			Service string `json:"service"`
			S3URI   string `json:"s3_uri"`
			Kind    string `json:"kind"`
		} `json:"manifest_keys"`
	}
	require.NoError(t, json.Unmarshal(entries[0].Payload, &requested))
	require.Len(t, requested.ManifestKeys, 1)
	assert.Equal(t, "ontology", requested.ManifestKeys[0].Kind)
	assert.Equal(t, "s3://test-bucket/company-ontology/it-ont-1/ontology.yaml", requested.ManifestKeys[0].S3URI)

	ref, err := deps.Topologies.Write(ctx, "it-ont-1", release.Topology{})
	require.NoError(t, err)
	require.NoError(t, handlers.HandleParsedManifest(ctx, deps, handlers.HandleParsedManifestInput{
		ReleaseID: "it-ont-1", Status: "ok", TopologyRef: ref,
	}))

	r, err = deps.NewUoW().RunRepo().Get(ctx, "it-ont-1")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusPromoted, r.Status(), "nothing to validate: the ontology release promotes")
	var kind, key string
	require.NoError(t, db.QueryRow(`SELECT manifest_kind, manifest_s3_key FROM service_prod WHERE service_name = 'company-ontology'`).Scan(&kind, &key))
	assert.Equal(t, "ontology", kind)
	assert.Equal(t, "s3://test-bucket/company-ontology/it-ont-1/ontology.yaml", key)

	other := postRelease(srv, ontologyRequest("it-ont-2", "another-ontology"))
	require.Equal(t, http.StatusConflict, other.Code)
	assert.Equal(t, "service \"company-ontology\" is the install's ontology: an install has one ontology service\n", other.Body.String())

	asDbt := postRelease(srv, httpinfra.ReceiveCandidateRequest{
		Service: "company-ontology", ReleaseID: "it-ont-3", ImageTag: "img", Repo: "acme/ontology", CommitSHA: "cafebabe",
	})
	require.Equal(t, http.StatusConflict, asDbt.Code)
	assert.Equal(t, "service \"company-ontology\" is released as kind \"ontology\": a service cannot change between kind \"ontology\" and a node kind\n", asDbt.Body.String())

	require.Equal(t, http.StatusAccepted, postRelease(srv, ontologyRequest("it-ont-4", "company-ontology")).Code,
		"the ontology service releases again")
}

// Two first submissions of the kind ontology for two services race through
// ReceiveCandidate against real Postgres. Both may find no other ontology
// service when they read, so only the release-queue lock lets exactly one be
// accepted: the other is answered ErrOntologyServiceConflict and stores nothing.
func TestIntegration_ConcurrentOntologySubmissionsOfTwoServicesHaveOneWinner(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	deps.OntologyEnabled = true

	for round := 0; round < 20; round++ {
		_, err := db.Exec("DELETE FROM release_pipeline_runs")
		require.NoError(t, err)

		services := []string{"ontology-a", "ontology-b"}
		errs := make([]error, len(services))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, service := range services {
			wg.Add(1)
			go func(i int, service string) {
				defer wg.Done()
				<-start
				errs[i] = handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
					Service: service, ReleaseID: "race-" + service, ImageTag: "img:1",
					Repo: "acme/ontology", CommitSHA: "deadbeefcafe1234", Kind: "ontology",
				})
			}(i, service)
		}
		close(start)
		wg.Wait()

		winners := 0
		for _, err := range errs {
			if err == nil {
				winners++
				continue
			}
			require.ErrorIs(t, err, handlers.ErrOntologyServiceConflict, "round %d", round)
		}
		require.Equal(t, 1, winners, "round %d: errs %v", round, errs)

		var runs int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM release_pipeline_runs`).Scan(&runs))
		assert.Equal(t, 1, runs, "round %d: the loser stores nothing", round)
	}
}
