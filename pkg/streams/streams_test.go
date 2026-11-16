package streams

import (
	"testing"
)

func retiredHas(name string) bool {
	for _, r := range Retired {
		if r == name {
			return true
		}
	}
	return false
}

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

func TestRemediationRequestedV3_ReplacesV2(t *testing.T) {
	if RemediationRequestedV3 != "remediation.requested:v3" {
		t.Fatalf("RemediationRequestedV3 = %q", RemediationRequestedV3)
	}
	if AgentRemediationRemediationRequested != "agent-remediation-remediation-requested" {
		t.Fatalf("consumer group renamed: %q", AgentRemediationRemediationRequested)
	}
	if CatalogRemediationRequestedRejections != "catalog-remediation-requested-rejections" {
		t.Fatalf("consumer group renamed: %q", CatalogRemediationRequestedRejections)
	}
	groupsOf(t, RemediationRequestedV3, AgentRemediationRemediationRequested, CatalogRemediationRequestedRejections)
	if _, live := Groups["remediation.requested:v2"]; live {
		t.Error("remediation.requested:v2 is still a live stream")
	}
	if !retiredHas("remediation.requested:v2") {
		t.Fatal("remediation.requested:v2 must be listed in retired_streams")
	}
}

func TestRemediationRetryRequestedV2_ReplacesV1(t *testing.T) {
	if RemediationRetryRequestedV2 != "remediation.retry_requested:v2" {
		t.Fatalf("RemediationRetryRequestedV2 = %q", RemediationRetryRequestedV2)
	}
	if RemediationRetryRequested != "remediation-retry-requested" {
		t.Fatalf("consumer group renamed: %q", RemediationRetryRequested)
	}
	groupsOf(t, RemediationRetryRequestedV2, RemediationRetryRequested)
	if _, live := Groups["remediation.retry_requested:v1"]; live {
		t.Error("remediation.retry_requested:v1 is still a live stream")
	}
	if !retiredHas("remediation.retry_requested:v1") {
		t.Fatal("remediation.retry_requested:v1 must be listed in retired_streams")
	}
}

// remediation.handed_over:v1 has a producer and no consumer: a human reads it.
func TestRemediationHandedOverV1_HasNoConsumer(t *testing.T) {
	if RemediationHandedOverV1 != "remediation.handed_over:v1" {
		t.Fatalf("RemediationHandedOverV1 = %q", RemediationHandedOverV1)
	}
	groupsOf(t, RemediationHandedOverV1)
	if retiredHas(RemediationHandedOverV1) {
		t.Error("remediation.handed_over:v1 is retired")
	}
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
	for _, r := range Retired {
		if _, live := Groups[r]; live {
			t.Errorf("%s is both live and retired", r)
		}
	}
}

func TestManifestLoadedCandidateV2_ReplacesV1(t *testing.T) {
	if ManifestLoadedCandidateV2 != "manifest.loaded.candidate:v2" {
		t.Fatalf("ManifestLoadedCandidateV2 = %q", ManifestLoadedCandidateV2)
	}
	if ReleaseControllerManifestLoadedCandidate != "release-controller-manifest-loaded-candidate" {
		t.Fatalf("consumer group renamed: %q", ReleaseControllerManifestLoadedCandidate)
	}
	if got := Groups[ManifestLoadedCandidateV2]; len(got) != 1 || got[0] != ReleaseControllerManifestLoadedCandidate {
		t.Fatalf("manifest.loaded.candidate:v2 groups = %v", got)
	}
	retired := false
	for _, r := range Retired {
		if r == "manifest.loaded.candidate:v1" {
			retired = true
		}
	}
	if !retired {
		t.Fatal("manifest.loaded.candidate:v1 must be listed in retired_streams")
	}
}

func TestReleasePromotedV2_ReplacesV1(t *testing.T) {
	if ReleasePromotedV2 != "release.promoted:v2" {
		t.Fatalf("ReleasePromotedV2 = %q", ReleasePromotedV2)
	}
	want := map[string]bool{
		CatalogReleaseTopology:             false,
		CatalogReleaseVersions:             false,
		ExecutionControllerReleasePromoted: false,
		RunControllerReleasePromoted:       false,
	}
	for _, g := range Groups[ReleasePromotedV2] {
		if _, ok := want[g]; !ok {
			t.Errorf("unexpected group %q on release.promoted:v2", g)
		}
		want[g] = true
	}
	for g, seen := range want {
		if !seen {
			t.Errorf("release.promoted:v2 is missing group %q", g)
		}
	}
	retired := false
	for _, r := range Retired {
		if r == "release.promoted:v1" {
			retired = true
		}
	}
	if !retired {
		t.Fatal("release.promoted:v1 must be listed in retired_streams")
	}
}

// run-controller reads the schedule names from the live release it stores, so
// catalog announces none: schedules.loaded:v1 is retired, and no stream
// keeps the group run-controller read it with.
func TestSchedulesLoadedV1_IsRetired(t *testing.T) {
	const name, group = "schedules.loaded:v1", "state-schedule-catalog"
	if _, live := Groups[name]; live {
		t.Fatalf("%s is still a live stream", name)
	}
	retired := false
	for _, r := range Retired {
		if r == name {
			retired = true
		}
	}
	if !retired {
		t.Fatalf("%s must be listed in retired_streams", name)
	}
	for stream, groups := range Groups {
		for _, g := range groups {
			if g == group {
				t.Errorf("group %s still reads %s", group, stream)
			}
		}
	}
}

func TestReleaseSeedsPendingV1_IsRetired(t *testing.T) {
	const name = "release.seeds.pending:v1"
	if _, live := Groups[name]; live {
		t.Fatalf("%s is still a live stream", name)
	}
	retired := false
	for _, r := range Retired {
		if r == name {
			retired = true
		}
	}
	if !retired {
		t.Fatalf("%s must be listed in retired_streams", name)
	}
}
