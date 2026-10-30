package streams

import (
	"testing"
)

func TestRemediationRequestedV2_ReplacesV1(t *testing.T) {
	if RemediationRequestedV2 != "remediation.requested:v2" {
		t.Fatalf("RemediationRequestedV2 = %q", RemediationRequestedV2)
	}
	if AgentRemediationRemediationRequested != "agent-remediation-remediation-requested" {
		t.Fatalf("consumer group renamed: %q", AgentRemediationRemediationRequested)
	}
	if CatalogRemediationRequestedRejections != "catalog-remediation-requested-rejections" {
		t.Fatalf("consumer group renamed: %q", CatalogRemediationRequestedRejections)
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
// orchestrator announces none: schedules.loaded:v1 is retired, and no stream
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
