package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// TestParseOperation_AcceptsTheDefaultAndRejectsOtherSpellings: the empty wire
// form and the declared name of the default both parse to the default, every
// other declared value parses to itself, and any other spelling is refused.
func TestParseOperation_AcceptsTheDefaultAndRejectsOtherSpellings(t *testing.T) {
	cases := map[string]struct {
		want    model.Operation
		wantErr bool
	}{
		"":             {model.OperationRun, false},
		"run":          {model.OperationRun, false},
		"test":         {model.OperationTest, false},
		"build":        {model.OperationBuild, false},
		"full_refresh": {model.OperationFullRefresh, false},
		"FULL_REFRESH": {"", true},
		"full-refresh": {"", true},
		"bogus":        {"", true},
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

// TestOperation_SingleNodeOnlyIsExactlyFullRefresh: of every operation the
// contract declares, only full_refresh may not fan out across a schedule.
func TestOperation_SingleNodeOnlyIsExactlyFullRefresh(t *testing.T) {
	ops := model.Operations()
	if len(ops) != 4 {
		t.Fatalf("Operations() = %v, want the four declared operations", ops)
	}
	for _, op := range ops {
		if got, want := op.IsSingleNodeOnly(), op == model.OperationFullRefresh; got != want {
			t.Errorf("%q.IsSingleNodeOnly() = %v, want %v", op, got, want)
		}
		if !op.IsValid() {
			t.Errorf("%q must be valid", op)
		}
	}
}

// TestOperationRun_KeepsTheEmptyWireForm: the default is the empty string on
// the wire, so a message that names no operation reads as a plain run.
func TestOperationRun_KeepsTheEmptyWireForm(t *testing.T) {
	if model.OperationRun != "" {
		t.Fatalf("OperationRun = %q, want the empty wire form", model.OperationRun)
	}
}
