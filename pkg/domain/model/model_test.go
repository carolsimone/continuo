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

func TestParseOperation(t *testing.T) {
	cases := map[string]struct{ want model.Operation; wantErr bool }{
		"":      {model.OperationRun, false},
		"run":   {model.OperationRun, false},
		"test":  {model.OperationTest, false},
		"build": {model.OperationBuild, false},
		"bogus": {"", true},
	}
	for in, c := range cases {
		got, err := model.ParseOperation(in)
		if (err != nil) != c.wantErr {
			t.Fatalf("ParseOperation(%q) err=%v wantErr=%v", in, err, c.wantErr)
		}
		if err == nil && got != c.want {
			t.Fatalf("ParseOperation(%q)=%q want %q", in, got, c.want)
		}
	}
}

func TestParseNodeType_PythonModel(t *testing.T) {
	got, err := model.ParseNodeType("python-model")
	if err != nil {
		t.Fatalf("ParseNodeType(python-model): %v", err)
	}
	if got != model.NodeTypePythonModel {
		t.Fatalf("got %q, want %q", got, model.NodeTypePythonModel)
	}
}

func TestParseNodeType_UnknownStillRejected(t *testing.T) {
	if _, err := model.ParseNodeType("r-model"); err == nil {
		t.Fatal("expected error for unknown node_type")
	}
}
