package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestParseNodeType_ValidTypes(t *testing.T) {
	tests := []struct {
		input    string
		expected model.NodeType
	}{
		{"dbt-model", model.NodeTypeDbtModel},
		{"dbt-seed", model.NodeTypeDbtSeed},
		{"dbt-snapshot", model.NodeTypeDbtSnapshot},
	}
	for _, tt := range tests {
		got, err := model.ParseNodeType(tt.input)
		if err != nil {
			t.Errorf("ParseNodeType(%q) returned error: %v", tt.input, err)
		}
		if got != tt.expected {
			t.Errorf("ParseNodeType(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestParseNodeType_InvalidTypes(t *testing.T) {
	for _, s := range []string{"", "dbt-unknown", "model"} {
		_, err := model.ParseNodeType(s)
		if err == nil {
			t.Errorf("ParseNodeType(%q) expected error, got nil", s)
		}
	}
}

func TestNodeType_Command_DbtModel(t *testing.T) {
	got := model.NodeTypeDbtModel.Command("orders")
	want := []string{"dbt", "run", "--select", "orders"}
	assertSliceEqual(t, want, got)
}

func TestNodeType_Command_DbtSeed(t *testing.T) {
	got := model.NodeTypeDbtSeed.Command("my_seed")
	want := []string{"dbt", "seed", "--select", "my_seed"}
	assertSliceEqual(t, want, got)
}

func TestNodeType_Command_DbtSnapshot(t *testing.T) {
	got := model.NodeTypeDbtSnapshot.Command("my_snap")
	want := []string{"dbt", "snapshot", "--select", "my_snap"}
	assertSliceEqual(t, want, got)
}

func assertSliceEqual(t *testing.T, want, got []string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("len mismatch: want %v, got %v", want, got)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Errorf("index %d: want %q, got %q", i, want[i], got[i])
		}
	}
}
