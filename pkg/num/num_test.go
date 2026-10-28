package num

import (
	"math"
	"testing"
)

func TestClampInt32_IntInput(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int32
	}{
		{"in-range positive value round-trips", 42, 42},
		{"in-range negative value round-trips", -42, -42},
		{"value above int32 max clamps to MaxInt32", int(math.MaxInt32) + 1, math.MaxInt32},
		{"value below int32 min clamps to MinInt32", int(math.MinInt32) - 1, math.MinInt32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClampInt32(tt.in); got != tt.want {
				t.Errorf("ClampInt32(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestClampInt32_Int64Input(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want int32
	}{
		{"in-range positive value round-trips", 42, 42},
		{"in-range negative value round-trips", -42, -42},
		{"value above int32 max clamps to MaxInt32", int64(math.MaxInt32) + 1, math.MaxInt32},
		{"value below int32 min clamps to MinInt32", int64(math.MinInt32) - 1, math.MinInt32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClampInt32(tt.in); got != tt.want {
				t.Errorf("ClampInt32(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}
