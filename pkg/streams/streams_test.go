package streams

import (
	"slices"
	"testing"
)

func TestRemediationRequestedV2_ReplacesV1(t *testing.T) {
	if RemediationRequestedV2 != "remediation.requested:v2" {
		t.Fatalf("RemediationRequestedV2 = %q", RemediationRequestedV2)
	}
	if AgentRemediationRemediationRequested != "agent-remediation-remediation-requested" {
		t.Fatalf("consumer group renamed: %q", AgentRemediationRemediationRequested)
	}
	if OrchestratorRemediationRequestedRejections != "orchestrator-remediation-requested-rejections" {
		t.Fatalf("consumer group renamed: %q", OrchestratorRemediationRequestedRejections)
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

func TestReleasePromotedV2_HasItsThreeGroups(t *testing.T) {
	if ReleasePromotedV2 != "release.promoted:v2" {
		t.Fatalf("ReleasePromotedV2 = %q", ReleasePromotedV2)
	}
	want := []string{OrchestratorReleasePromotedV2, OrchestratorReleasePromotedVersionsV2, ExecutorReleasePromotedV2}
	if got := Groups[ReleasePromotedV2]; !slices.Equal(got, want) {
		t.Fatalf("Groups[%s] = %v, want %v", ReleasePromotedV2, got, want)
	}
}
