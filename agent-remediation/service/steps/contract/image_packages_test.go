package contract_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/contract"
)

func gatherImages(t *testing.T, images *fakeImages, logger *slog.Logger) (step.ContextOutput, error) {
	t.Helper()
	return contract.NewImagePackages(images, logger).Gather(context.Background(),
		step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})
}

func TestImagePackagesStep_MetaNamesTheStepAndWhatItProvides(t *testing.T) {
	m := contract.NewImagePackages(nil, nil).Meta()
	assert.Equal(t, "image_packages", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyImagePackages}, m.Provides)
}

func TestImagePackagesStep_ProvidesTheReleasesPackageList(t *testing.T) {
	images := &fakeImages{}

	out, err := gatherImages(t, images, discardLogger())

	require.NoError(t, err)
	assert.False(t, out.Declined)
	assert.Equal(t, map[remediation.EvidenceKey]any{
		remediation.KeyImagePackages: remediation.ImagePackages(thePackages),
	}, out.Evidence)
	assert.Equal(t, [][2]string{{"rel-1", "svc"}}, images.asked, "the rejected release's list, for the failing node's project")
}

// TestImagePackagesStep_DeclinesWhenTheReleaseRecordsNone covers a release none
// of whose bundles records the project's package list: it is known before any
// draft and no draft can change it, so the cluster is declined with the reason
// before the model is called, and the reason is logged.
func TestImagePackagesStep_DeclinesWhenTheReleaseRecordsNone(t *testing.T) {
	noPackages := fmt.Errorf("release rel-1, project svc: %w (the release has no contract bundle)", ports.ErrNoImagePackages)
	var logs bytes.Buffer

	out, err := gatherImages(t, &fakeImages{err: noPackages}, slog.New(slog.NewJSONHandler(&logs, nil)))

	require.NoError(t, err)
	assert.True(t, out.Declined)
	assert.Equal(t, noPackages.Error(), out.Decline)
	assert.Empty(t, out.Evidence)
	var line map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &line), "one log line: %s", logs.String())
	assert.Equal(t, "INFO", line["level"])
	assert.Equal(t, noPackages.Error(), line["msg"])
	assert.Equal(t, "rel-1", line["release"])
	assert.Equal(t, nodeID, line["node"])
}

func TestImagePackagesStep_AReadThatMayClearIsTheStepsError(t *testing.T) {
	out, err := gatherImages(t, &fakeImages{err: errBoom}, discardLogger())

	require.ErrorIs(t, err, errBoom)
	assert.NotErrorIs(t, err, ports.ErrNoImagePackages)
	assert.False(t, out.Declined)
}
