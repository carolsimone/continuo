package streams

import "testing"

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

func TestManifestLoadedCandidateV2_HasReleaseControllersGroup(t *testing.T) {
	if ManifestLoadedCandidateV2 != "manifest.loaded.candidate:v2" {
		t.Fatalf("ManifestLoadedCandidateV2 = %q", ManifestLoadedCandidateV2)
	}
	groups := Groups[ManifestLoadedCandidateV2]
	if len(groups) != 1 || groups[0] != ReleaseControllerManifestLoadedCandidateV2 {
		t.Fatalf("Groups[%s] = %v, want release-controller's one group", ManifestLoadedCandidateV2, groups)
	}
}
