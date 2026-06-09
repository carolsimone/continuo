package handlers

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestCallerFromContext_Success(t *testing.T) {
	md := metadata.Pairs("x-caller-id", "rerun-reset")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	caller, err := callerFromContext(ctx)

	require.NoError(t, err)
	assert.Equal(t, run.CallerRerunReset, caller)
}

func TestCallerFromContext_NoMetadata(t *testing.T) {
	_, err := callerFromContext(context.Background())
	assert.Error(t, err)
}

func TestCallerFromContext_MissingKey(t *testing.T) {
	md := metadata.Pairs("other-key", "value")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := callerFromContext(ctx)
	assert.Error(t, err)
}
