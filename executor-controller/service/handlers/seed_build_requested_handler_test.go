package handlers_test

import (
	"context"
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

func TestSeedBuildRequestedHandler_EnqueuesOneSeedBuildDeploymentPerSeed(t *testing.T) {
	depl := &stubDeploymentsRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl}

	evt := events.SeedBuildRequested{
		ReleaseID:       "rel-seed-2026",
		Mode:            "seed_build",
		CandidateSchema: "_candidate_rel_seed_2026",
		Seeds: []events.SeedBuildNode{
			{
				NodeID:      "seed.shop.country_codes",
				ServiceName: "shop",
				SchemaName:  "public",
				TableName:   "country_codes",
				NodeType:    pkg_model.NodeTypeDbtSeed,
				ImageTag:    "sha-seed1",
			},
		},
		SeedIDsInOrder: []string{"seed.shop.country_codes"},
	}

	msgProcID := uuid.New()
	h := handlers.NewSeedBuildRequestedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, msgProcID))
	require.Len(t, depl.added, 1)

	dep := depl.added[0]
	assert.Equal(t, model.ModeSeedBuild, dep.Mode(),
		"handler must enqueue ModeSeedBuild deployments, not ModeValidation")
	assert.Equal(t, model.StatusPending, dep.Status(),
		"seed-build deployments always start pending (no upstreams)")
	require.NotNil(t, dep.MessageProcessingID())
	assert.Equal(t, msgProcID, *dep.MessageProcessingID())
	assert.True(t, dep.IsDeployable(), "seed-build row must be immediately deployable")

	cmd := dep.ValidationCommand()
	assert.Equal(t, "rel-seed-2026", cmd.ReleaseID)
	assert.Equal(t, "seed.shop.country_codes", cmd.NodeID)
	assert.Equal(t, "shop", cmd.ServiceName)
	assert.Equal(t, "public", cmd.SchemaName)
	assert.Equal(t, "country_codes", cmd.TableName)
	assert.Equal(t, string(pkg_model.NodeTypeDbtSeed), cmd.NodeType)
	assert.Equal(t, "sha-seed1", cmd.ImageTag)
	assert.Equal(t, "_candidate_rel_seed_2026", cmd.CandidateSchema)
	assert.NotEmpty(t, cmd.JobName, "handler must populate JobName via BuildValidationJobName")
	// Seed-build tasks have no SQL URI, upstreams, validation op, or prod schema.
	assert.Empty(t, cmd.CandidateSQLURI)
	assert.Empty(t, cmd.UpstreamNodeIDs)
	assert.Empty(t, cmd.ValidationOp)
	assert.Empty(t, cmd.ProdSchema)
}

func TestSeedBuildRequestedHandler_MultipleSeeds(t *testing.T) {
	depl := &stubDeploymentsRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl}

	evt := events.SeedBuildRequested{
		ReleaseID:       "rel-multi",
		CandidateSchema: "_candidate_rel_multi",
		Seeds: []events.SeedBuildNode{
			{
				NodeID: "seed.shop.a", ServiceName: "shop", SchemaName: "public",
				TableName: "a", NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "t1",
			},
			{
				NodeID: "seed.shop.b", ServiceName: "shop", SchemaName: "public",
				TableName: "b", NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "t2",
			},
		},
	}

	h := handlers.NewSeedBuildRequestedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.Nil))
	require.Len(t, depl.added, 2)

	seen := map[string]bool{}
	for _, dep := range depl.added {
		assert.Equal(t, model.ModeSeedBuild, dep.Mode())
		assert.Equal(t, model.StatusPending, dep.Status())
		seen[dep.ValidationCommand().NodeID] = true
	}
	assert.True(t, seen["seed.shop.a"])
	assert.True(t, seen["seed.shop.b"])
}

func TestSeedBuildRequestedHandler_MsgProcIDNilAllowed(t *testing.T) {
	depl := &stubDeploymentsRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl}

	evt := events.SeedBuildRequested{
		ReleaseID:       "rel-nil",
		CandidateSchema: "_cand",
		Seeds: []events.SeedBuildNode{
			{NodeID: "seed.s.x", ServiceName: "s", SchemaName: "sc", TableName: "x",
				NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "t"},
		},
	}

	h := handlers.NewSeedBuildRequestedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.Nil))
	require.Len(t, depl.added, 1)
	// uuid.Nil msgProcID → nil pointer stored on deployment
	assert.Nil(t, depl.added[0].MessageProcessingID())
}
