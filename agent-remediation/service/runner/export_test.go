package runner

import "time"

// SetLeaseHeartbeat sets how often the poller holds the lease of the
// remediation it polls, so a test sees the renewals in milliseconds.
func (p *GatePoller) SetLeaseHeartbeat(d time.Duration) { p.heartbeat = d }

// ReplaceStep puts s in place of the registered step that has its name, so a
// test can script that one step.
func (r *Registry) ReplaceStep(s any) {
	d := s.(described)
	r.steps[d.Meta().Name] = entry{step: s, meta: d.Meta()}
}
