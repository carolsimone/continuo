package remediation

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

var legalTry = map[TryStatus][]TryStatus{
	TryDrafting:  {TryChecking, TryFailed},
	TryChecking:  {TryVerifying, TryFailed},
	TryVerifying: {TryPassed, TryFailed},
}

var legalCluster = map[ClusterStatus][]ClusterStatus{
	ClusterGathering: {ClusterProposing, ClusterHandedOver, ClusterFellBack},
	ClusterProposing: {ClusterPreflight, ClusterHandedOver, ClusterFellBack},
	ClusterPreflight: {ClusterChecked, ClusterProposing, ClusterHandedOver, ClusterFellBack},
}

var legalRemediation = map[RemediationStatus][]RemediationStatus{
	StatusRunning:       {StatusDone, StatusSystemFailure},
	StatusSystemFailure: {StatusRunning},
}

var legalStep = map[StepStatus][]StepStatus{
	StepRunning: {StepDone, StepFailed, StepWaiting},
	StepWaiting: {StepDone, StepFailed},
	StepFailed:  {StepRunning},
}

func TestAllListsHoldEveryStatusOnce(t *testing.T) {
	assert.ElementsMatch(t, []TryStatus{TryDrafting, TryChecking, TryVerifying, TryPassed, TryFailed}, AllTryStatuses())
	assert.ElementsMatch(t,
		[]ClusterStatus{ClusterGathering, ClusterProposing, ClusterPreflight, ClusterChecked, ClusterHandedOver, ClusterFellBack},
		AllClusterStatuses())
	assert.ElementsMatch(t, []RemediationStatus{StatusRunning, StatusDone, StatusSystemFailure}, AllRemediationStatuses())
	assert.ElementsMatch(t, []StepStatus{StepRunning, StepWaiting, StepDone, StepFailed}, AllStepStatuses())
}

func TestStatusValuesAreSnakeCase(t *testing.T) {
	assert.Equal(t, "system_failure", string(StatusSystemFailure))
	assert.Equal(t, "handed_over", string(ClusterHandedOver))
	assert.Equal(t, "fell_back", string(ClusterFellBack))
	assert.Equal(t, "preflight", string(ClusterPreflight))
}

func TestTryTransitions(t *testing.T) {
	for _, from := range AllTryStatuses() {
		for _, to := range AllTryStatuses() {
			assert.Equal(t, slices.Contains(legalTry[from], to), from.CanMoveTo(to), "%s -> %s", from, to)
		}
	}
}

func TestClusterTransitions(t *testing.T) {
	for _, from := range AllClusterStatuses() {
		for _, to := range AllClusterStatuses() {
			assert.Equal(t, slices.Contains(legalCluster[from], to), from.CanMoveTo(to), "%s -> %s", from, to)
		}
	}
}

func TestRemediationTransitions(t *testing.T) {
	for _, from := range AllRemediationStatuses() {
		for _, to := range AllRemediationStatuses() {
			assert.Equal(t, slices.Contains(legalRemediation[from], to), from.CanMoveTo(to), "%s -> %s", from, to)
		}
	}
}

func TestStepTransitions(t *testing.T) {
	for _, from := range AllStepStatuses() {
		for _, to := range AllStepStatuses() {
			assert.Equal(t, slices.Contains(legalStep[from], to), from.CanMoveTo(to), "%s -> %s", from, to)
		}
	}
}
