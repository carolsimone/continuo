// executor-controller/adapters/redis/seed_build_requested_parser_test.go
package redis

import (
	"encoding/json"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedBuildPayload mirrors the flat JSON body release-controller emits in the
// "payload" field of a seed.build.requested:v1 message. Seeds have no
// upstream_node_ids or candidate_artifact_uri.
func seedBuildPayload() map[string]any {
	return map[string]any{
		"release_id": "rel-456",
		"mode":       "seed_build",
		"seeds": []map[string]any{
			{
				"unique_id":    "seed.shop.country_codes",
				"service_name": "shop",
				"node_type":    "dbt-seed",
				"schema_name":  "public",
				"table_name":   "country_codes",
				"image_tag":    "sha-seed1",
			},
			{
				"unique_id":    "seed.shop.currencies",
				"service_name": "shop",
				"node_type":    "dbt-seed",
				"schema_name":  "public",
				"table_name":   "currencies",
				"image_tag":    "sha-seed2",
			},
		},
		"seed_ids_in_order": []string{"seed.shop.country_codes", "seed.shop.currencies"},
		"image_tags":        map[string]string{"shop": "sha-seed1"},
		"candidate_schema":  "_candidate_rel_456",
	}
}

// seedBuildMsg builds a goredis.XMessage whose "payload" field holds the
// marshaled JSON body, matching the outbox publisher's wire format.
func seedBuildMsg(t *testing.T, payload map[string]any, outboxEntryID string) goredis.XMessage {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	values := map[string]interface{}{"payload": string(body)}
	if outboxEntryID != "" {
		values["outbox_entry_id"] = outboxEntryID
	}
	return goredis.XMessage{ID: "2-0", Values: values}
}

func TestParseSeedBuildRequested_HappyPath(t *testing.T) {
	outboxEntryID := uuid.New()
	msg := seedBuildMsg(t, seedBuildPayload(), outboxEntryID.String())

	evt, err := ParseSeedBuildRequested(msg)
	require.NoError(t, err)

	assert.Equal(t, outboxEntryID, evt.OutboxEntryID)
	assert.Equal(t, "rel-456", evt.ReleaseID)
	assert.Equal(t, "seed_build", evt.Mode)
	assert.Equal(t, "_candidate_rel_456", evt.CandidateSchema)
	assert.Equal(t, map[string]string{"shop": "sha-seed1"}, evt.ImageTags)
	assert.Equal(t, []string{"seed.shop.country_codes", "seed.shop.currencies"}, evt.SeedIDsInOrder)

	require.Len(t, evt.Seeds, 2)
	assert.Equal(t, "seed.shop.country_codes", evt.Seeds[0].NodeID)
	assert.Equal(t, "shop", evt.Seeds[0].ServiceName)
	assert.Equal(t, "public", evt.Seeds[0].SchemaName)
	assert.Equal(t, "country_codes", evt.Seeds[0].TableName)
	assert.Equal(t, pkg_model.NodeTypeDbtSeed, evt.Seeds[0].NodeType)
	assert.Equal(t, "sha-seed1", evt.Seeds[0].ImageTag)

	assert.Equal(t, "seed.shop.currencies", evt.Seeds[1].NodeID)
	assert.Equal(t, pkg_model.NodeTypeDbtSeed, evt.Seeds[1].NodeType)
}

func TestParseSeedBuildRequested_OutboxEntryIDAbsentIsNilUUID(t *testing.T) {
	msg := seedBuildMsg(t, seedBuildPayload(), "")
	evt, err := ParseSeedBuildRequested(msg)
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, evt.OutboxEntryID,
		"absent outbox_entry_id is uuid.Nil so dedup degrades to (msg.ID, stream_name)")
}

func TestParseSeedBuildRequested_OutboxEntryIDInvalidIsError(t *testing.T) {
	msg := seedBuildMsg(t, seedBuildPayload(), "not-a-uuid")
	_, err := ParseSeedBuildRequested(msg)
	require.Error(t, err)
}

func TestParseSeedBuildRequested_MissingPayloadIsError(t *testing.T) {
	_, err := ParseSeedBuildRequested(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{}})
	require.Error(t, err)
}

func TestParseSeedBuildRequested_MalformedPayloadIsError(t *testing.T) {
	_, err := ParseSeedBuildRequested(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"payload": "{not json",
	}})
	require.Error(t, err)
}

func TestParseSeedBuildRequested_MissingReleaseID(t *testing.T) {
	p := seedBuildPayload()
	delete(p, "release_id")
	_, err := ParseSeedBuildRequested(seedBuildMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseSeedBuildRequested_WrongMode(t *testing.T) {
	p := seedBuildPayload()
	p["mode"] = "validation"
	_, err := ParseSeedBuildRequested(seedBuildMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseSeedBuildRequested_EmptySeeds(t *testing.T) {
	p := seedBuildPayload()
	p["seeds"] = []map[string]any{}
	p["seed_ids_in_order"] = []string{}
	_, err := ParseSeedBuildRequested(seedBuildMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseSeedBuildRequested_SeedMissingUniqueID(t *testing.T) {
	p := seedBuildPayload()
	seeds := p["seeds"].([]map[string]any)
	delete(seeds[0], "unique_id")
	_, err := ParseSeedBuildRequested(seedBuildMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseSeedBuildRequested_SeedMissingField(t *testing.T) {
	p := seedBuildPayload()
	seeds := p["seeds"].([]map[string]any)
	delete(seeds[0], "schema_name")
	_, err := ParseSeedBuildRequested(seedBuildMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseSeedBuildRequested_NonDbtSeedNodeTypeIsError(t *testing.T) {
	p := seedBuildPayload()
	seeds := p["seeds"].([]map[string]any)
	seeds[0]["node_type"] = "dbt-model"
	_, err := ParseSeedBuildRequested(seedBuildMsg(t, p, ""))
	require.Error(t, err, "non-dbt-seed node_type must be rejected in seed build event")
}

func TestParseSeedBuildRequested_InvalidNodeTypeIsError(t *testing.T) {
	p := seedBuildPayload()
	seeds := p["seeds"].([]map[string]any)
	seeds[0]["node_type"] = "no_such_type"
	_, err := ParseSeedBuildRequested(seedBuildMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseSeedBuildRequested_SeedsMismatchOrderList(t *testing.T) {
	p := seedBuildPayload()
	p["seed_ids_in_order"] = []string{"seed.shop.country_codes", "seed.shop.missing"}
	_, err := ParseSeedBuildRequested(seedBuildMsg(t, p, ""))
	require.Error(t, err)
}

// Verify seeds have no upstream_node_ids or candidate_artifact_uri fields — they
// are simply absent from the SeedBuildNode struct (no validation needed, just
// confirming the parsed struct has no stray fields).
func TestParseSeedBuildRequested_SeedHasNoUpstreamsOrSQLURI(t *testing.T) {
	msg := seedBuildMsg(t, seedBuildPayload(), "")
	evt, err := ParseSeedBuildRequested(msg)
	require.NoError(t, err)
	require.Len(t, evt.Seeds, 2)
	// SeedBuildNode has no UpstreamNodeIDs / CandidateArtifactURI fields by design.
	// Just confirm the parse succeeds and node identity is correct.
	assert.Equal(t, "seed.shop.country_codes", evt.Seeds[0].NodeID)
}
