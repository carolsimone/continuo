package config

import (
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
)

// state sizes its pool from DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS. Load names
// the set pool keys it does not read, so main can warn about them.
func TestLoad_NamesTheIgnoredPoolKeys(t *testing.T) {
	cases := []struct {
		name, poolSize, maxOverflow string
		want                        []string
	}{
		{name: "neither set"},
		{name: "pool size set", poolSize: "20", want: []string{"DB_POOL_SIZE"}},
		{name: "both set", poolSize: "20", maxOverflow: "10", want: []string{"DB_POOL_SIZE", "DB_MAX_OVERFLOW"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DB_POOL_SIZE", tc.poolSize)
			t.Setenv("DB_MAX_OVERFLOW", tc.maxOverflow)
			cfg := Load(&pkgconfig.Validator{})
			assert.Equal(t, tc.want, cfg.IgnoredPoolKeys)
		})
	}
}
