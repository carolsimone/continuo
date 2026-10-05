package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadMetricsPort(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    int
		invalid bool
	}{
		{"", DefaultMetricsPort, false},
		{"9100", 9100, false},
		{"0", DefaultMetricsPort, true},
		{"70000", DefaultMetricsPort, true},
		{"metrics", DefaultMetricsPort, true},
	} {
		t.Setenv("METRICS_PORT", tc.raw)
		v := &Validator{}
		assert.Equal(t, tc.want, LoadMetricsPort(v), tc.raw)
		assert.Equal(t, tc.invalid, len(v.Missing()) == 1, tc.raw)
	}
}
