package streams

import (
	"strings"
	"testing"
)

func groupsOf(t *testing.T, stream string, want ...string) {
	t.Helper()
	got := Groups[stream]
	if len(got) != len(want) {
		t.Fatalf("%s groups = %v, want %v", stream, got, want)
	}
	have := map[string]bool{}
	for _, g := range got {
		have[g] = true
	}
	for _, g := range want {
		if !have[g] {
			t.Errorf("%s is missing group %q (has %v)", stream, g, got)
		}
	}
}

// Every stream starts at its first version: no message of an earlier version
// exists, so no stream carries a version above :v1.
func TestEveryStreamIsAtV1(t *testing.T) {
	for _, s := range All {
		if !strings.HasSuffix(s, ":v1") {
			t.Errorf("%s is not at :v1", s)
		}
	}
}

// No stream name is retired, so dead-letter-controller deletes no stream key.
func TestNoStreamIsRetired(t *testing.T) {
	if len(Retired) != 0 {
		t.Fatalf("Retired = %v, want none", Retired)
	}
}

// heal.triggered:v1 carries one heal trigger per rejected release and round,
// from the remediation classifier to agent-remediation and to catalog's
// case base.
func TestHealTriggeredV1_FeedsAgentRemediationAndCatalog(t *testing.T) {
	if HealTriggeredV1 != "heal.triggered:v1" {
		t.Fatalf("HealTriggeredV1 = %q", HealTriggeredV1)
	}
	if AgentRemediationHealTriggered != "agent-remediation-heal-triggered" {
		t.Fatalf("AgentRemediationHealTriggered = %q", AgentRemediationHealTriggered)
	}
	if CatalogHealTriggeredRejections != "catalog-heal-triggered-rejections" {
		t.Fatalf("CatalogHealTriggeredRejections = %q", CatalogHealTriggeredRejections)
	}
	groupsOf(t, HealTriggeredV1, AgentRemediationHealTriggered, CatalogHealTriggeredRejections)
}

// release.parsed:v1 is how topology-controller's parse of a candidate release
// ended, read by release-controller.
func TestReleaseParsedV1_FeedsReleaseController(t *testing.T) {
	if ReleaseParsedV1 != "release.parsed:v1" {
		t.Fatalf("ReleaseParsedV1 = %q", ReleaseParsedV1)
	}
	if ReleaseControllerReleaseParsed != "release-controller-release-parsed" {
		t.Fatalf("ReleaseControllerReleaseParsed = %q", ReleaseControllerReleaseParsed)
	}
	groupsOf(t, ReleaseParsedV1, ReleaseControllerReleaseParsed)
}

func TestRemediationRetryRequestedV1_FeedsTheClassifier(t *testing.T) {
	if RemediationRetryRequestedV1 != "remediation.retry_requested:v1" {
		t.Fatalf("RemediationRetryRequestedV1 = %q", RemediationRetryRequestedV1)
	}
	groupsOf(t, RemediationRetryRequestedV1, RemediationRetryRequested)
}

func TestTaskStatusUpdatedV1_FeedsRunController(t *testing.T) {
	if TaskStatusUpdatedV1 != "task.status.updated:v1" {
		t.Fatalf("TaskStatusUpdatedV1 = %q", TaskStatusUpdatedV1)
	}
	groupsOf(t, TaskStatusUpdatedV1, RunControllerTaskStatusUpdated)
}

func TestReleasePromotedV1_FeedsCatalogRunControllerAndExecutionController(t *testing.T) {
	if ReleasePromotedV1 != "release.promoted:v1" {
		t.Fatalf("ReleasePromotedV1 = %q", ReleasePromotedV1)
	}
	groupsOf(t, ReleasePromotedV1,
		CatalogReleaseTopology,
		CatalogReleaseVersions,
		CatalogReleaseContracts,
		ExecutionControllerReleasePromoted,
		RunControllerReleasePromoted,
	)
}

// remediation.handed_over:v1 has a producer and no consumer: a human reads it.
func TestRemediationHandedOverV1_HasNoConsumer(t *testing.T) {
	if RemediationHandedOverV1 != "remediation.handed_over:v1" {
		t.Fatalf("RemediationHandedOverV1 = %q", RemediationHandedOverV1)
	}
	groupsOf(t, RemediationHandedOverV1)
}

func TestGroups_CoversEveryStream(t *testing.T) {
	if len(Groups) != len(All) {
		t.Fatalf("Groups has %d streams, All has %d", len(Groups), len(All))
	}
	for _, s := range All {
		if _, ok := Groups[s]; !ok {
			t.Errorf("Groups is missing %s", s)
		}
	}
}
