package observer

import (
	"context"
	"errors"
	"strings"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/validationresult"
)

// probeViews probes the external views the preflight of d's seed found outside
// the allowed set, and returns the findings with each view's answer as Probes.
// facts are the facts d settles on. It returns nil, and probes nothing, unless
// d is a production attempt whose preflight ran in report mode (a
// late-binding warehouse), its findings list a dependant outside the allowed
// set and hold no probes yet, and the attempt succeeded. A probe that cannot
// run, that times out, or whose block says the warehouse was not reached
// records every view as not probed; it never fails the attempt.
func (o *Observer) probeViews(ctx context.Context, d *model.Deployment, facts model.AttemptFacts) *seedreload.Findings {
	cmd := d.Command()
	if d.Mode() != model.ModeProduction || cmd.SeedPreflight == nil || cmd.SeedPreflight.Mode != seedreload.PreflightReport {
		return nil
	}
	findings := facts.SeedPreflight
	if findings == nil || len(findings.Probes) > 0 || !model.Classify(facts).Succeeded {
		return nil
	}
	outside := findings.Outside()
	if len(outside) == 0 {
		return nil
	}
	views := make([]string, len(outside))
	for i, dep := range outside {
		views[i] = dep.Schema + "." + dep.Name
	}
	nodeID := strings.ToLower(cmd.SchemaName + "." + cmd.TableName)

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	log, err := o.deps.Prober.ProbeViews(probeCtx, ports.ProbeRequest{JobName: d.JobName(), NodeID: nodeID, Views: views})
	probes, err := probesOf(log, err)
	if err != nil {
		o.deps.Logger.Warn("The views of a seed load were not probed", "run_id", cmd.RunID, "task_id", cmd.TaskID,
			"node_id", nodeID, "views", views, "error", err)
		probes = make([]seedreload.Probe, len(outside))
		for i, dep := range outside {
			probes[i] = seedreload.Probe{Schema: dep.Schema, Name: dep.Name, Error: "not probed: " + err.Error()}
		}
	}
	probed := *findings
	probed.Probes = probes
	return &probed
}

// probesOf is the answer of each view in the probe block that log holds. An
// error is returned when the probe failed to run (runErr), printed no probe
// block, or reports that it could not reach the warehouse.
func probesOf(log string, runErr error) ([]seedreload.Probe, error) {
	if runErr != nil {
		return nil, runErr
	}
	_, raw := validationresult.Split(log)
	if raw == "" {
		return nil, errors.New("the probe printed no result")
	}
	p, err := validationresult.ParsePreflight([]byte(raw))
	if err != nil || p.Op != validationresult.OpProbeViews {
		return nil, errors.New("the probe printed no probe result")
	}
	if !p.Checked {
		msg := strings.TrimSpace(p.Message)
		if msg == "" {
			msg = strings.TrimSpace(p.Reason)
		}
		if msg == "" {
			msg = "the warehouse was not reached"
		}
		return nil, errors.New(msg)
	}
	probes := make([]seedreload.Probe, len(p.Views))
	for i, v := range p.Views {
		probes[i] = seedreload.Probe(v)
	}
	return probes, nil
}
