package conformance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
)

const (
	// owner is the worker every scenario claims its remediation as.
	owner = "conformance"
	// lease is how long a scenario's claim lasts.
	lease = 5 * time.Minute
	// verificationTimeout is how long a verification run may take.
	verificationTimeout = 20 * time.Minute
	// messageID is the stream message id a scenario delivers its trigger as.
	messageID = "1-0"
	// maxPasses bounds how often a scenario drives or polls its remediation
	// before it gives up on it ending.
	maxPasses = 25
)

// scenarioRun is one scenario's runner over its world, and the remediation it
// accepted.
type scenarioRun struct {
	w    World
	deps runner.Deps
	r    *runner.Runner
	id   uuid.UUID
}

// start builds the world of a scenario and a runner over it, as the service
// wires one: the suite's contract locator and inspector, the world's other
// ports, and the suite's files.
func (s Suite) start(f Fixture, script Script, maintenance bool) (*scenarioRun, error) {
	if s.NewWorld == nil {
		return nil, errors.New("the suite has no world to run a scenario in")
	}
	w, err := s.NewWorld(f, script)
	if err != nil {
		return nil, fmt.Errorf("build the scenario's world: %w", err)
	}
	collab := w.Collaborators()
	collab.ContractLocator = s.Locator
	collab.ContractInspector = s.Inspector
	collab.VerificationTimeout = verificationTimeout
	deps := runner.Deps{
		NewUoW:              w.NewUoW,
		Registry:            runner.NewRegistry(collab),
		Files:               s.Files,
		LLM:                 w.LLM(),
		Collab:              collab,
		Clock:               w.Clock(),
		Logger:              slog.New(slog.DiscardHandler),
		Maintenance:         maintenance,
		Owner:               owner,
		VerificationTimeout: verificationTimeout,
	}
	return &scenarioRun{w: w, deps: deps, r: runner.New(deps)}, nil
}

// restart replaces the runner with a new one over the same world, as a worker
// that restarted.
func (sr *scenarioRun) restart() {
	sr.r = runner.New(sr.deps)
}

// accept delivers t as the stream message messageID and finds the remediation
// it created.
func (sr *scenarioRun) accept(ctx context.Context, t remediation.Trigger) error {
	payload, err := serialization.EncodeRemediationRequestedV2(t)
	if err != nil {
		return err
	}
	fields := map[string]string{}
	if t.TenantID != "" {
		fields["tenant_id"] = t.TenantID
	}
	if err := sr.r.Accept(ctx, runner.Accepted{Trigger: t, Payload: payload, MessageID: messageID, Fields: fields}); err != nil {
		return fmt.Errorf("accept the trigger: %w", err)
	}
	rems := sr.w.Remediations()
	if len(rems) != 1 {
		return fmt.Errorf("the trigger left %d remediations, want 1", len(rems))
	}
	sr.id = rems[0].ID
	return nil
}

// finish drives the remediation until it ends, as the worker and the gate
// poller do: each pass claims it, drives it, or polls its gate while its try
// is verifying, and releases it. The first error ends the run.
func (sr *scenarioRun) finish(ctx context.Context) error {
	repo := sr.w.Repository()
	for range maxPasses {
		snap, err := repo.Get(ctx, sr.id)
		if err != nil {
			return fmt.Errorf("load the remediation: %w", err)
		}
		if snap.Remediation.Status != remediation.StatusRunning {
			return nil
		}
		claimed, err := repo.Claim(ctx, sr.id, owner, sr.w.Clock().Now(), lease)
		if err != nil {
			return fmt.Errorf("claim the remediation: %w", err)
		}
		if !claimed {
			return errors.New("the remediation could not be claimed")
		}
		if snap.Try != nil && snap.Try.Status == remediation.TryVerifying {
			err = sr.r.PollGate(ctx, snap)
		} else {
			err = sr.r.Drive(ctx, sr.id)
		}
		if rerr := repo.ReleaseLease(ctx, sr.id, owner); rerr != nil && err == nil {
			err = fmt.Errorf("release the remediation: %w", rerr)
		}
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("the remediation did not end after %d drives and polls", maxPasses)
}
