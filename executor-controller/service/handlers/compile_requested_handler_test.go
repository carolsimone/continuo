package handlers_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompileRequestedHandler_EnqueuesOneCompileDeployment(t *testing.T) {
	depl := &stubDeploymentsRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl}

	evt := events.CompileRequested{
		ReleaseID: "rel-compile-2026",
		Service:   "finance",
		ImageTag:  "sha-compile-1",
		Bucket:    "my-artifacts-bucket",
	}

	msgProcID := uuid.New()
	h := handlers.NewCompileRequestedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, msgProcID))
	require.Len(t, depl.added, 1, "exactly one compile deployment must be enqueued")

	dep := depl.added[0]
	assert.Equal(t, model.ModeCompile, dep.Mode(),
		"handler must enqueue ModeCompile deployment")
	assert.Equal(t, model.StatusPending, dep.Status(),
		"compile deployment always starts pending — single root node")
	require.NotNil(t, dep.MessageProcessingID())
	assert.Equal(t, msgProcID, *dep.MessageProcessingID())
	assert.True(t, dep.IsDeployable())

	cmd := dep.ValidationCommand()
	assert.Equal(t, "rel-compile-2026", cmd.ReleaseID)
	assert.Equal(t, "finance", cmd.NodeID,
		"NodeID must equal the service name for compile")
	assert.Equal(t, "finance", cmd.ServiceName)
	assert.Equal(t, "sha-compile-1", cmd.ImageTag)
	assert.NotEmpty(t, cmd.JobName, "handler must populate JobName via BuildValidationJobName")
	assert.Equal(t,
		"s3://my-artifacts-bucket/finance/rel-compile-2026/manifest.json",
		cmd.ManifestS3URI,
		"ManifestS3URI must be s3://<bucket>/<service>/<release_id>/manifest.json",
	)
}

func TestCompileRequestedHandler_MsgProcIDNilAllowed(t *testing.T) {
	depl := &stubDeploymentsRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl}

	evt := events.CompileRequested{
		ReleaseID: "rel-nil",
		Service:   "analytics",
		ImageTag:  "sha-t1",
		Bucket:    "bucket-x",
	}

	h := handlers.NewCompileRequestedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.Nil))
	require.Len(t, depl.added, 1)
	assert.Nil(t, depl.added[0].MessageProcessingID(),
		"uuid.Nil msgProcID must store nil pointer on deployment")
}

func TestCompileRequestedHandler_JobNameDeterministic(t *testing.T) {
	depl := &stubDeploymentsRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl}

	evt := events.CompileRequested{
		ReleaseID: "rel-abc",
		Service:   "shop",
		ImageTag:  "sha-2",
		Bucket:    "bkt",
	}

	h := handlers.NewCompileRequestedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.Nil))
	require.Len(t, depl.added, 1)

	// Verify job name matches BuildValidationJobName(releaseID, service)
	expected := handlers.BuildValidationJobName("rel-abc", "shop")
	assert.Equal(t, expected, depl.added[0].ValidationCommand().JobName)
}
