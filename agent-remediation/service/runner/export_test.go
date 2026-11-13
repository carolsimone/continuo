package runner

import "time"

// SetLeaseHeartbeat sets how often the poller holds the lease of the
// remediation it polls, so a test sees the renewals in milliseconds.
func (p *GatePoller) SetLeaseHeartbeat(d time.Duration) { p.heartbeat = d }
