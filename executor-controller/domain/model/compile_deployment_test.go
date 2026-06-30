package model_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func compileCmd() command.ValidationDeployTask {
	return command.ValidationDeployTask{
		ReleaseID:     "rel-compile-2026",
		NodeID:        "finance",
		ServiceName:   "finance",
		ImageTag:      "sha-compile1",
		JobName:       "compile-rel-compile-2026-finance",
		ManifestS3URI: "s3://my-bucket/finance/rel-compile-2026/manifest.json",
	}
}

func TestNewCompileDeployment_ModeIsCompile(t *testing.T) {
	now := time.Now()
	d := model.NewCompileDeployment(compileCmd(), nil, now)

	assert.Equal(t, model.ModeCompile, d.Mode(),
		"NewCompileDeployment must set mode=compile")
}

func TestNewCompileDeployment_StartsStatusPending(t *testing.T) {
	now := time.Now()
	d := model.NewCompileDeployment(compileCmd(), nil, now)

	assert.Equal(t, model.StatusPending, d.Status(),
		"compile deployment is a single root node — always starts pending")
}

func TestNewCompileDeployment_HasValidID(t *testing.T) {
	now := time.Now()
	d := model.NewCompileDeployment(compileCmd(), nil, now)

	assert.NotEqual(t, uuid.Nil, d.ID())
}

func TestNewCompileDeployment_MessageProcessingIDStored(t *testing.T) {
	now := time.Now()
	msgProcID := uuid.New()
	d := model.NewCompileDeployment(compileCmd(), &msgProcID, now)

	require.NotNil(t, d.MessageProcessingID())
	assert.Equal(t, msgProcID, *d.MessageProcessingID())
}

func TestNewCompileDeployment_MessageProcessingIDNilAllowed(t *testing.T) {
	now := time.Now()
	d := model.NewCompileDeployment(compileCmd(), nil, now)
	assert.Nil(t, d.MessageProcessingID())
}

func TestNewCompileDeployment_ValidationCommandStored(t *testing.T) {
	now := time.Now()
	cmd := compileCmd()
	d := model.NewCompileDeployment(cmd, nil, now)

	got := d.ValidationCommand()
	assert.Equal(t, cmd.ReleaseID, got.ReleaseID)
	assert.Equal(t, cmd.NodeID, got.NodeID)
	assert.Equal(t, cmd.ServiceName, got.ServiceName)
	assert.Equal(t, cmd.ImageTag, got.ImageTag)
	assert.Equal(t, cmd.JobName, got.JobName)
	assert.Equal(t, cmd.ManifestS3URI, got.ManifestS3URI)
}

func TestNewCompileDeployment_IsDeployable(t *testing.T) {
	now := time.Now()
	// Compile jobs have no NodeType — the full service manifest is compiled,
	// not a single dbt node. IsDeployable only requires identity + image.
	cmd := command.ValidationDeployTask{
		ReleaseID:   "rel-1",
		NodeID:      "finance",
		ServiceName: "finance",
		ImageTag:    "sha-1",
		JobName:     "compile-rel-1-finance",
	}
	d := model.NewCompileDeployment(cmd, nil, now)
	assert.True(t, d.IsDeployable())
}

func TestNewCompileDeployment_DefaultMaxRetries(t *testing.T) {
	now := time.Now()
	d := model.NewCompileDeployment(compileCmd(), nil, now)
	assert.Equal(t, 3, d.MaxRetries())
}

func TestReconstituteCompile_RoundTrip(t *testing.T) {
	now := time.Now()
	id := uuid.New()
	msgProcID := uuid.New()
	cmd := compileCmd()
	deployedAt := now.Add(5 * time.Second)
	errMsg := "some error"
	outcomeAt := now.Add(10 * time.Second)

	d := model.ReconstituteCompile(
		id, &msgProcID, cmd, model.StatusDeployed,
		1, 3, now, now,
		&deployedAt, &errMsg,
		"ok", "s3://logs", "s3://results", &outcomeAt,
	)

	assert.Equal(t, id, d.ID())
	assert.Equal(t, model.ModeCompile, d.Mode())
	assert.Equal(t, model.StatusDeployed, d.Status())
	assert.Equal(t, 1, d.RetryCount())
	assert.Equal(t, 3, d.MaxRetries())
	assert.Equal(t, "ok", d.Outcome())
	assert.Equal(t, "s3://logs", d.DBTLogURI())
	assert.Equal(t, "s3://results", d.DBTRunResultsURI())
	require.NotNil(t, d.OutcomeAt())
	assert.Equal(t, outcomeAt, *d.OutcomeAt())

	vc := d.ValidationCommand()
	assert.Equal(t, cmd.ManifestS3URI, vc.ManifestS3URI)
}
