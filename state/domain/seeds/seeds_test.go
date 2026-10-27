package seeds_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func desired(table, hash string) seeds.Desired {
	return seeds.Desired{
		Key: topology.KeyOf("raw", table), ServiceName: "svc", SchemaName: "raw", TableName: table,
		NodeType: "dbt-seed", ContentHash: hash, ImageTag: "svc:2",
	}
}

func seedNode(table, hash string) topology.Node {
	return topology.Node{
		Key: topology.KeyOf("raw", table), UniqueID: "raw." + table, ServiceName: "svc", SchemaName: "raw",
		TableName: table, NodeType: "dbt-seed", ContentHash: hash, ImageTag: "svc:2",
	}
}

// One case per rule of the seed decision, in the order the rules apply.
func TestDecide(t *testing.T) {
	attempt := uuid.MustParse("6f1c0c1e-1111-4a4a-8b8b-000000000001")
	imageOnly := desired("orders", "h2")
	imageOnly.ImageTag = "svc:9"
	cases := []struct {
		name  string
		d     seeds.Desired
		m     seeds.Materialization
		found bool
		want  seeds.Decision
	}{
		{"a seed without a content hash is skipped", desired("orders", ""), seeds.Materialization{}, false, seeds.DecisionSkipUnknownHash},
		{"an unknown hash is skipped even when a version is recorded", desired("orders", ""),
			seeds.Materialization{ContentHash: "h1"}, true, seeds.DecisionSkipUnknownHash},
		{"a seed never built is built", desired("orders", "h2"), seeds.Materialization{}, false, seeds.DecisionBuild},
		{"built at the desired hash", desired("orders", "h2"), seeds.Materialization{ContentHash: "h2"}, true, seeds.DecisionNothing},
		{"an image-only change builds nothing", imageOnly, seeds.Materialization{ContentHash: "h2"}, true, seeds.DecisionNothing},
		{"built at the desired hash while another build runs", desired("orders", "h2"),
			seeds.Materialization{ContentHash: "h2", AttemptContentHash: "h3", AttemptRunID: attempt, AttemptState: seeds.AttemptInFlight},
			true, seeds.DecisionNothing},
		{"a build of the desired hash is still running", desired("orders", "h2"),
			seeds.Materialization{ContentHash: "h1", AttemptContentHash: "h2", AttemptRunID: attempt, AttemptState: seeds.AttemptInFlight},
			true, seeds.DecisionWait},
		{"a build of an older hash is still running", desired("orders", "h2"),
			seeds.Materialization{ContentHash: "h1", AttemptContentHash: "h1b", AttemptRunID: attempt, AttemptState: seeds.AttemptInFlight},
			true, seeds.DecisionWait},
		{"the build of the desired hash failed", desired("orders", "h2"),
			seeds.Materialization{ContentHash: "h1", AttemptContentHash: "h2", AttemptRunID: attempt, AttemptState: seeds.AttemptFailed},
			true, seeds.DecisionNoRetry},
		{"the build of an older hash failed", desired("orders", "h2"),
			seeds.Materialization{ContentHash: "h1", AttemptContentHash: "h1b", AttemptRunID: attempt, AttemptState: seeds.AttemptFailed},
			true, seeds.DecisionBuild},
		{"the build of the desired hash succeeded but recorded no version", desired("orders", "h2"),
			seeds.Materialization{ContentHash: "h1", AttemptContentHash: "h2", AttemptRunID: attempt, AttemptState: seeds.AttemptSucceeded},
			true, seeds.DecisionBuild},
		{"the run of the last request no longer exists", desired("orders", "h2"),
			seeds.Materialization{ContentHash: "h1", AttemptContentHash: "h2", AttemptRunID: attempt, AttemptState: seeds.AttemptNone},
			true, seeds.DecisionBuild},
		{"a row that records only a failed request at another hash", desired("orders", "h2"),
			seeds.Materialization{AttemptContentHash: "h1", AttemptRunID: attempt, AttemptState: seeds.AttemptFailed},
			true, seeds.DecisionBuild},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, seeds.Decide(tc.d, tc.m, tc.found))
		})
	}
}

func TestDecision_String(t *testing.T) {
	assert.Equal(t, "nothing", seeds.DecisionNothing.String())
	assert.Equal(t, "skip-unknown-hash", seeds.DecisionSkipUnknownHash.String())
	assert.Equal(t, "wait", seeds.DecisionWait.String())
	assert.Equal(t, "no-retry", seeds.DecisionNoRetry.String())
	assert.Equal(t, "build", seeds.DecisionBuild.String())
	assert.Equal(t, "unknown", seeds.Decision(99).String())
}

func TestDesiredFrom_KeepsSeedsInKeyOrder(t *testing.T) {
	got := seeds.DesiredFrom([]topology.Node{
		seedNode("users", "hu"),
		{Key: "stg.orders", UniqueID: "stg.orders", SchemaName: "stg", TableName: "orders", NodeType: "dbt-model", ContentHash: "hm"},
		seedNode("orders", ""),
	})
	assert.Equal(t, []seeds.Desired{desired("orders", ""), desired("users", "hu")}, got)
}

func TestPlan_ReturnsTheSeedsToBuildInKeyOrder(t *testing.T) {
	built := map[topology.NodeKey]seeds.Materialization{
		topology.KeyOf("raw", "built"):   {ContentHash: "hb"},
		topology.KeyOf("raw", "running"): {ContentHash: "old", AttemptContentHash: "hr", AttemptState: seeds.AttemptInFlight},
		topology.KeyOf("raw", "failed"):  {ContentHash: "old", AttemptContentHash: "hf", AttemptState: seeds.AttemptFailed},
	}
	got := seeds.Plan([]seeds.Desired{
		desired("zeta", "hz"), desired("alpha", "ha"), desired("built", "hb"),
		desired("running", "hr"), desired("failed", "hf"), desired("unknown", ""),
	}, built)
	assert.Equal(t, []seeds.Desired{desired("alpha", "ha"), desired("zeta", "hz")}, got)
}

func TestPlan_NothingToBuildIsEmpty(t *testing.T) {
	got := seeds.Plan([]seeds.Desired{desired("built", "hb")},
		map[topology.NodeKey]seeds.Materialization{topology.KeyOf("raw", "built"): {ContentHash: "hb"}})
	assert.Empty(t, got)
}

// On the first promotion state applies: a bootstrap release adopts nothing;
// otherwise every seed not in changed_node_ids is adopted, and every seed when
// the release's first promote-seed run already exists. A seed without a
// content hash is never adopted, and no other node type is.
func TestAdoptions(t *testing.T) {
	live := []topology.Node{
		seedNode("users", "h2"),
		seedNode("orders", "h1"),
		seedNode("blank", ""),
		{Key: "stg.orders", UniqueID: "stg.orders", SchemaName: "stg", TableName: "orders", NodeType: "dbt-model", ContentHash: "hm"},
	}
	changed := []string{"raw.users", "stg.orders"}
	cases := []struct {
		name                      string
		bootstrap, firstRunExists bool
		want                      []seeds.Desired
	}{
		{"bootstrap adopts nothing", true, false, nil},
		{"bootstrap wins over an existing first run", true, true, nil},
		{"unchanged seeds are adopted", false, false, []seeds.Desired{desired("orders", "h1")}},
		{"every seed is adopted when the first run exists", false, true, []seeds.Desired{desired("orders", "h1"), desired("users", "h2")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := seeds.Adoptions(live, changed, tc.bootstrap, tc.firstRunExists)
			if tc.want == nil {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestAdopted(t *testing.T) {
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, seeds.Built{
		Key: "raw.orders", ContentHash: "h1", ImageTag: "svc:2", ReleaseID: "rel-1", At: at, Source: seeds.SourceAdopted,
	}, seeds.Adopted(desired("orders", "h1"), "rel-1", at))
}

// The namespace is fixed, so a release's first run id is the UUIDv5 of the
// tenant and the release under it and every later run id is derived the same
// way.
func TestSeedsRunID_IsStablePerTenantReleaseAndCount(t *testing.T) {
	ns := uuid.MustParse("3e7b1a2f-8c4d-4e9a-b5f0-1d2c3a4e5f60")
	assert.Equal(t, uuid.NewSHA1(ns, []byte("acme\x00schedule:rel-1")), seeds.RunID("acme", "rel-1", 1))
	assert.Equal(t, uuid.NewSHA1(ns, []byte("acme\x00schedule:rel-1#2")), seeds.RunID("acme", "rel-1", 2))
	assert.Equal(t, uuid.NewSHA1(ns, []byte("acme\x00schedule:rel-1#7")), seeds.RunID("acme", "rel-1", 7))
	assert.Equal(t, seeds.RunID("acme", "rel-1", 1), seeds.RunID("acme", "rel-1", 0), "a count below 1 names the first run")
	assert.NotEqual(t, seeds.RunID("acme", "rel-1", 2), seeds.RunID("acme", "rel-2", 2))
}

// The same release id promoted under two tenants plans two runs.
func TestSeedsRunID_DiffersPerTenant(t *testing.T) {
	assert.NotEqual(t, seeds.RunID("acme", "rel-1", 1), seeds.RunID("globex", "rel-1", 1))
	assert.NotEqual(t, seeds.RunID("acme", "rel-1", 3), seeds.RunID("globex", "rel-1", 3))
}
