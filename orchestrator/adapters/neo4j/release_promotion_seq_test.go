package neo4jinfra_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// promoteSeq is the seq the next promote call uses. Every call takes a larger
// number than the one before it, so each promotion in a test is newer than the
// last — the order release-controller assigns.
var promoteSeq atomic.Int64

// promote applies a promotion under the next seq with no service metadata and
// reports whether it swapped the topology.
func promote(ctx context.Context, repo *neo4jinfra.ReleasePromotionRepository, releaseID string,
	nodes []topology.ReleasePromotedTopologyNode, now time.Time) (bool, error) {
	outcome, err := repo.PromoteRelease(ctx, releaseID, promoteSeq.Add(1), nodes, nil, now)
	return outcome == topology.PromotionApplied, err
}

func seqNode(uniqueID string) []topology.ReleasePromotedTopologyNode {
	return []topology.ReleasePromotedTopologyNode{{
		UniqueID: uniqueID, SchemaName: "p", TableName: uniqueID, ServiceName: "s",
		ImageTag: "img", Schedule: "d", UpstreamUniqueIDs: []string{},
	}}
}

// runWrite runs one write statement and consumes its result.
func runWrite(t *testing.T, client neo4jinfra.Neo4jClient, cypher string, params map[string]any) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, cypher, params)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// liveMeta returns the live release and promotion seq recorded on :Meta, and
// checks the swap's lock property never outlives its transaction.
func liveMeta(t *testing.T, client neo4jinfra.Neo4jClient) (string, int64) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MATCH (m:Meta {key:'current_release'})
		RETURN m.release_id AS rid, m.promotion_seq AS seq, m._lock AS lock`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx), ":Meta {key:'current_release'} must exist")
	rec := res.Record()
	rid, _ := rec.Get("rid")
	seq, _ := rec.Get("seq")
	lock, _ := rec.Get("lock")
	assert.Nil(t, lock, "the lock property never outlives the swap transaction")
	r, _ := rid.(string)
	n, _ := seq.(int64)
	return r, n
}

// activeTableIDs lists the unique_ids of the active :Table nodes, sorted.
func activeTableIDs(t *testing.T, client neo4jinfra.Neo4jClient) []string {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `MATCH (t:Table) WHERE COALESCE(t.active, true) RETURN t.unique_id AS uid ORDER BY uid`, nil)
	require.NoError(t, err)
	var ids []string
	for res.Next(ctx) {
		uid, _ := res.Record().Get("uid")
		ids = append(ids, uid.(string))
	}
	require.NoError(t, res.Err())
	return ids
}

func TestReleasePromotionRepository_NewerSeqRecordsSeqOnMetaAndTopologyRoot(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	outcome, err := newReleaseRepo(client).PromoteRelease(ctx, "rel-1", 3, seqNode("a"),
		map[string]map[string]string{"s": {"image_tag": "img"}}, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, topology.PromotionApplied, outcome)

	rid, seq := liveMeta(t, client)
	assert.Equal(t, "rel-1", rid)
	assert.Equal(t, int64(3), seq)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MATCH (root:TopologyRoot {id:'singleton'})
		RETURN root.promotion_seq AS seq, root.service_metadata AS meta, root.topology_generation AS legacy`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx), ":TopologyRoot is written in the swap transaction")
	rootSeq, _ := res.Record().Get("seq")
	meta, _ := res.Record().Get("meta")
	legacy, _ := res.Record().Get("legacy")
	assert.Equal(t, int64(3), rootSeq)
	assert.Nil(t, legacy, "the legacy counter property is removed")
	var decoded map[string]map[string]string
	require.NoError(t, json.Unmarshal([]byte(meta.(string)), &decoded))
	assert.Equal(t, map[string]map[string]string{"s": {"image_tag": "img"}}, decoded)
}

// An older promotion arriving after a newer one — a retry, a late delivery or a
// dead letter redriven days later — must never revert the live topology.
func TestReleasePromotionRepository_OlderSeqIsStaleAndLeavesTheGraph(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })
	repo := newReleaseRepo(client)

	outcome, err := repo.PromoteRelease(ctx, "rel-new", 8, seqNode("new"), nil, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, topology.PromotionApplied, outcome)

	outcome, err = repo.PromoteRelease(ctx, "rel-old", 7, seqNode("old"), nil, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, topology.PromotionStale, outcome)

	rid, seq := liveMeta(t, client)
	assert.Equal(t, "rel-new", rid)
	assert.Equal(t, int64(8), seq)
	assert.Equal(t, []string{"new"}, activeTableIDs(t, client))
}

func TestReleasePromotionRepository_SameSeqAndReleaseIsARedelivery(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })
	repo := newReleaseRepo(client)

	_, err := repo.PromoteRelease(ctx, "rel-1", 4, seqNode("a"), nil, time.Now().UTC())
	require.NoError(t, err)

	// The redelivery carries an empty node list: it must not be applied, or the
	// live topology would be retired.
	outcome, err := repo.PromoteRelease(ctx, "rel-1", 4, nil, nil, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, topology.PromotionRedelivered, outcome)
	assert.Equal(t, []string{"a"}, activeTableIDs(t, client))
}

func TestReleasePromotionRepository_SameSeqForAnotherReleaseIsStale(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })
	repo := newReleaseRepo(client)

	_, err := repo.PromoteRelease(ctx, "rel-1", 4, seqNode("a"), nil, time.Now().UTC())
	require.NoError(t, err)
	outcome, err := repo.PromoteRelease(ctx, "rel-other", 4, seqNode("b"), nil, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, topology.PromotionStale, outcome)
	rid, _ := liveMeta(t, client)
	assert.Equal(t, "rel-1", rid)
}

// A graph swapped before promotion seqs existed carries :Meta.release_id and the
// legacy :TopologyRoot counter but no seq; the first seq-carrying promotion — the
// upgrade's re-announcement of the same release — applies.
func TestReleasePromotionRepository_GraphWithoutSeqAcceptsTheFirstPromotion(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })
	runWrite(t, client, `
		MERGE (m:Meta {key:'current_release'}) SET m.release_id = 'rel-live'
		MERGE (root:TopologyRoot {id:'singleton'}) SET root.topology_generation = 140`, nil)

	outcome, err := newReleaseRepo(client).PromoteRelease(ctx, "rel-live", 1, seqNode("a"), nil, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, topology.PromotionApplied, outcome)
	_, seq := liveMeta(t, client)
	assert.Equal(t, int64(1), seq)
}

// Two promotions racing for the live pointer serialise on the :Meta lock: the
// newer one ends live whichever transaction starts first, and the older one
// never reverts it.
func TestReleasePromotionRepository_ConcurrentPromotionsKeepTheNewestLive(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	repo := newReleaseRepo(client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	for i := 0; i < 10; i++ {
		wipeReleaseFixtures(t, client)
		runWrite(t, client, `MERGE (m:Meta {key:'current_release'}) SET m.promotion_seq = 0`, nil)

		var wg sync.WaitGroup
		start := make(chan struct{})
		outcomes := make([]topology.PromotionOutcome, 2)
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			outcomes[0], errs[0] = repo.PromoteRelease(ctx, "rel-old", 1, seqNode("old"), nil, time.Now().UTC())
		}()
		go func() {
			defer wg.Done()
			<-start
			outcomes[1], errs[1] = repo.PromoteRelease(ctx, "rel-new", 2, seqNode("new"), nil, time.Now().UTC())
		}()
		close(start)
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		assert.Equal(t, topology.PromotionApplied, outcomes[1], "iteration %d: the newer promotion always applies", i)
		rid, seq := liveMeta(t, client)
		assert.Equal(t, "rel-new", rid, "iteration %d", i)
		assert.Equal(t, int64(2), seq, "iteration %d", i)
		assert.Equal(t, []string{"new"}, activeTableIDs(t, client), "iteration %d", i)
	}
}
