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
