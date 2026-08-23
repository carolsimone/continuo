package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResolve_DefaultsWhenEmpty(t *testing.T) {
	cfg := Resolve(Inputs{})
	assert.Equal(t, "localhost:50051", cfg.StateEndpoint)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.False(t, cfg.Human)
}

func TestResolve_EnvOverridesDefault(t *testing.T) {
	cfg := Resolve(Inputs{
		EnvStateAddr: "state.cluster:50051",
		EnvTimeout:   "30s",
	})
	assert.Equal(t, "state.cluster:50051", cfg.StateEndpoint)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
}

func TestResolve_FlagOverridesEnv(t *testing.T) {
	cfg := Resolve(Inputs{
		FlagEndpoint: "override:9999",
		FlagTimeout:  "5s",
		EnvStateAddr: "state.cluster:50051",
		EnvTimeout:   "30s",
	})
	assert.Equal(t, "override:9999", cfg.StateEndpoint)
	assert.Equal(t, 5*time.Second, cfg.Timeout)
}

func TestResolve_InvalidTimeoutFallsBackToDefault(t *testing.T) {
	cfg := Resolve(Inputs{EnvTimeout: "not-a-duration"})
	assert.Equal(t, 10*time.Second, cfg.Timeout)
}

func TestResolve_ActorFromEnv(t *testing.T) {
	cfg := Resolve(Inputs{EnvActor: "agent-chat-llm"})
	assert.Equal(t, "agent-chat-llm", cfg.Actor)
}

func TestResolve_ActorEmptyByDefault(t *testing.T) {
	cfg := Resolve(Inputs{})
	assert.Equal(t, "", cfg.Actor)
}
