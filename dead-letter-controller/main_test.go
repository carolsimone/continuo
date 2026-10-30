package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	dlhttp "github.com/carolsimone/continuo/dead-letter-controller/adapters/http"
	"github.com/carolsimone/continuo/dead-letter-controller/config"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/pkg/liveness"
	"github.com/carolsimone/continuo/pkg/streams"
)

// readiness and liveness build the two health handlers exactly as main wires
// them through the HTTP adapter (readiness → /ready, liveness → /livez), so
// these tests exercise the real registry-check split plumbing.
func readiness(reg *liveness.Registry) http.HandlerFunc {
	return dlhttp.NewReadinessHandler(reg)
}
func liveHandler(reg *liveness.Registry) http.HandlerFunc {
	return dlhttp.NewLivenessHandler(reg)
}

func code(h http.HandlerFunc) int {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec.Code
}

// The redrive contract is pkg/streams': a group the contract declares can be
// redriven to, a retired stream cannot.
func TestRedriveContractIsTheStreamsContract(t *testing.T) {
	c := redriveContract()
	live := deadletter.DeadLetter{Source: deadletter.SourceConsumer, Stream: streams.ReleasePromotedV2,
		Group: streams.CatalogReleaseTopology, Publishable: true}
	if r := live.RedriveRefusal(c); r != "" {
		t.Fatalf("live stream refused: %s", r)
	}
	retired := live
	retired.Stream = streams.Retired[0]
	if r := retired.RedriveRefusal(c); r != "stream "+streams.Retired[0]+" is retired" {
		t.Fatalf("retired stream refusal = %q", r)
	}
}

// The dead letters of schedules.loaded:v1, the consumer's of
// state-schedule-catalog and the outbox's of its producer, are never
// redriven: the stream is retired, so nothing reads it, as with every other
// retired stream.
func TestRedriveContract_SchedulesLoadedV1DeadLettersAreNotRedrivable(t *testing.T) {
	const stream = "schedules.loaded:v1"
	if !slices.Contains(streams.Retired, stream) {
		t.Fatalf("%s is not a retired stream", stream)
	}
	c := redriveContract()
	consumer := deadletter.DeadLetter{Source: deadletter.SourceConsumer, Stream: stream,
		Group: "state-schedule-catalog", Publishable: true}
	outbox := deadletter.DeadLetter{Source: deadletter.SourceOutbox, Stream: stream, Publishable: true}
	for name, d := range map[string]deadletter.DeadLetter{"consumer": consumer, "outbox": outbox} {
		if r := d.RedriveRefusal(c); r != "stream "+stream+" is retired" {
			t.Errorf("%s dead letter refusal = %q", name, r)
		}
		if err := d.CheckRedrive(d.OriginalAt, c); !errors.Is(err, deadletter.ErrNotRedrivable) {
			t.Errorf("%s dead letter redrive err = %v, want ErrNotRedrivable", name, err)
		}
	}
	for _, retired := range streams.Retired {
		d := consumer
		d.Stream = retired
		if r := d.RedriveRefusal(c); r != "stream "+retired+" is retired" {
			t.Errorf("retired stream %s refusal = %q", retired, r)
		}
	}
}

// TestServiceNameMatchesDirectory pins config.ServiceName to this service's
// directory name: the contract's producer lists, the consumer groups and the
// dead letters' producer field all have to agree on it.
func TestServiceNameMatchesDirectory(t *testing.T) {
	if config.ServiceName != "dead-letter-controller" {
		t.Fatalf("ServiceName = %q, want dead-letter-controller", config.ServiceName)
	}
}

// TestHealthyRegistryPassesBothProbes: a live worker + fresh heartbeat → both
// readiness and liveness answer 200.
func TestHealthyRegistryPassesBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("consumer_dead_letters")
	reg.AddWorkerProbe("consumer_dead_letters_heartbeat", 0, func(context.Context) error { return nil })

	if c := code(readiness(reg)); c != http.StatusOK {
		t.Fatalf("readiness: expected 200, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusOK {
		t.Fatalf("liveness: expected 200, got %d", c)
	}
}

// TestDependencyOutageFailsReadinessNotLiveness: a failing dependency probe
// fails readiness but NOT liveness.
func TestDependencyOutageFailsReadinessNotLiveness(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("consumer_dead_letters")
	reg.AddDependencyProbe("postgres", 0, func(context.Context) error { return errors.New("down") })

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness during dependency outage: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusOK {
		t.Fatalf("liveness during dependency outage: expected 200 (must NOT restart), got %d", c)
	}
}

// TestConsumerExitFailsBothProbes: a consumer that exited with an error fails
// both readiness and liveness.
func TestConsumerExitFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("consumer_dead_letters")
	reg.WorkerExited("consumer_dead_letters", errors.New("permanent bootstrap failure: WRONGTYPE"))

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness after consumer exit: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("liveness after consumer exit: expected 503, got %d", c)
	}
}

// TestStaleHeartbeatFailsBothProbes: a wedged consumer (stale heartbeat) fails
// both probes.
func TestStaleHeartbeatFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("consumer_dead_letters")
	reg.AddWorkerProbe("consumer_dead_letters_heartbeat", 0, func(context.Context) error {
		return errors.New("read loop stalled — no activity for 5m0s")
	})

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness with stale heartbeat: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("liveness with stale heartbeat: expected 503, got %d", c)
	}
}
