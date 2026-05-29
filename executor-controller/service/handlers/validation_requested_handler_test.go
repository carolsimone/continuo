package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"regexp"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestValidationRequestedHandler_EnqueuesOneRowPerNode(t *testing.T) {
	depl := &stubDeploymentsRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl}

	evt := events.ValidationRequested{
		ReleaseID:       "rel-2026-05-29",
		Mode:            "validation",
		CandidateSchema: "public_candidate",
		DeferStateURI:   "s3://state/prod/manifest.json",
		Nodes: []events.ValidationNode{
			{
				NodeID:      "model.shop.orders",
				ServiceName: "dbt", SchemaName: "public", TableName: "orders",
				NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "sha-orders",
			},
			{
				NodeID:      "model.shop.customers",
				ServiceName: "dbt", SchemaName: "public", TableName: "customers",
				NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "sha-customers",
			},
		},
	}

	msgProcID := uuid.New()
	h := handlers.NewValidationRequestedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, msgProcID))
	require.Len(t, depl.added, 2)

	seen := map[string]bool{}
	for _, dep := range depl.added {
		assert.Equal(t, model.ModeValidation, dep.Mode())
		assert.Equal(t, model.StatusPending, dep.Status())
		require.NotNil(t, dep.MessageProcessingID())
		assert.Equal(t, msgProcID, *dep.MessageProcessingID())

		cmd := dep.ValidationCommand()
		assert.Equal(t, "rel-2026-05-29", cmd.ReleaseID)
		assert.Equal(t, "public_candidate", cmd.CandidateSchema)
		assert.Equal(t, "s3://state/prod/manifest.json", cmd.DeferStateURI)
		assert.NotEmpty(t, cmd.JobName, "every enqueued node carries a K8s Job name")
		assert.True(t, dep.IsDeployable(), "validation rows must be deployable")
		seen[cmd.NodeID] = true

		switch cmd.NodeID {
		case "model.shop.orders":
			assert.Equal(t, "orders", cmd.TableName)
			assert.Equal(t, "sha-orders", cmd.ImageTag)
			assert.Equal(t, string(pkg_model.NodeTypeDbtModel), cmd.NodeType)
		case "model.shop.customers":
			assert.Equal(t, "customers", cmd.TableName)
			assert.Equal(t, "sha-customers", cmd.ImageTag)
		}
	}
	assert.True(t, seen["model.shop.orders"])
	assert.True(t, seen["model.shop.customers"])
}

func TestValidationRequestedHandler_EmptyDeferStateURI_StillEnqueues(t *testing.T) {
	depl := &stubDeploymentsRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl}

	evt := events.ValidationRequested{
		ReleaseID:       "rel-1",
		CandidateSchema: "cand",
		DeferStateURI:   "",
		Nodes: []events.ValidationNode{
			{NodeID: "model.shop.orders", NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "sha-1"},
		},
	}

	h := handlers.NewValidationRequestedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)

	cmd := depl.added[0].ValidationCommand()
	assert.Equal(t, "", cmd.DeferStateURI, "empty defer state URI stored faithfully")
}

func TestBuildValidationJobName_TruncationKeepsDeterministicHash(t *testing.T) {
	longRelease := "release-abcdefghijklmnopqrstuvwxyz0123456789"
	longNode := "model.very_long_service_name.an_extremely_long_table_identifier_indeed"

	a := handlers.BuildValidationJobName(longRelease, longNode)
	b := handlers.BuildValidationJobName(longRelease, longNode)

	assert.LessOrEqual(t, len(a), 63, "K8s Job name must be <= 63 chars")
	assert.Equal(t, a, b, "job name must be deterministic for the same inputs")

	// A different node must yield a different name even after truncation,
	// because the hash tail is derived from (release, node).
	c := handlers.BuildValidationJobName(longRelease, longNode+"_x")
	assert.NotEqual(t, a, c, "hash tail differentiates distinct nodes post-truncation")
}

func TestBuildValidationJobName_K8sCharsOnly(t *testing.T) {
	rfc1123 := regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

	cases := []struct{ release, node string }{
		{"Rel_2026.05.29", "model.SHOP.Orders"},
		{"REL", "X"},
		{"a", "b"},
		{"release/with/slashes", "model.with.DOTS_and_under"},
		{"   ", "model.x"},
	}
	for _, tc := range cases {
		name := handlers.BuildValidationJobName(tc.release, tc.node)
		assert.Regexp(t, rfc1123, name,
			"job name for (%q,%q) must be a valid RFC1123 label: got %q", tc.release, tc.node, name)
		assert.LessOrEqual(t, len(name), 63)
	}
}
