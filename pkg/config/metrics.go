package config

import (
	"fmt"
	"os"
	"strconv"
)

// DefaultMetricsPort is the port every Go service serves /metrics on when
// METRICS_PORT is unset.
const DefaultMetricsPort = 9464

// LoadMetricsPort reads METRICS_PORT. Unset returns DefaultMetricsPort. A value
// that is not a port number is recorded on v, so the service refuses to start.
func LoadMetricsPort(v *Validator) int {
	raw := os.Getenv("METRICS_PORT")
	if raw == "" {
		return DefaultMetricsPort
	}
	p, err := strconv.Atoi(raw)
	if err != nil || p < 1 || p > 65535 {
		v.Add(fmt.Sprintf("METRICS_PORT (invalid port %q: expected a whole number from 1 to 65535)", raw))
		return DefaultMetricsPort
	}
	return p
}
