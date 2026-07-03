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

func TestInt32_IntInput(t *testing.T) {
	tests := []struct {
		name    string
		in      int
		want    int32
		wantErr bool
	}{
		{"in-range positive value round-trips", 42, 42, false},
		{"in-range negative value round-trips with no error", -42, -42, false},
		{"value above int32 max returns an error", int(math.MaxInt32) + 1, 0, true},
		{"value below int32 min returns an error", int(math.MinInt32) - 1, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Int32(tt.in, "field")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Int32(%d) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Int32(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestInt32_Int64Input(t *testing.T) {
	tests := []struct {
		name    string
		in      int64
		want    int32
		wantErr bool
	}{
		{"in-range positive value round-trips", 42, 42, false},
		{"in-range negative value round-trips with no error", -42, -42, false},
		{"value above int32 max returns an error", int64(math.MaxInt32) + 1, 0, true},
		{"value below int32 min returns an error", int64(math.MinInt32) - 1, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Int32(tt.in, "field")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Int32(%d) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Int32(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}
