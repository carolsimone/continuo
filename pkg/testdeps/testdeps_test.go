package testdeps_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/testdeps"
)

func TestRequired(t *testing.T) {
	cases := []struct {
		value string
		want  bool
	}{
		{"", false},
		{"0", false},
		{"1", true},
		{"true", true},
	}
	for _, c := range cases {
		t.Run("value="+c.value, func(t *testing.T) {
			t.Setenv(testdeps.RequireEnv, c.value)
			if got := testdeps.Required(); got != c.want {
				t.Fatalf("Required() with %q = %v, want %v", c.value, got, c.want)
			}
		})
	}
}

// fakeT records whether the test was failed or skipped without stopping the
// calling goroutine, which a real *testing.T would.
type fakeT struct {
	testing.TB
	failed, skipped bool
}

func (f *fakeT) Helper()               {}
func (f *fakeT) Fatalf(string, ...any) { f.failed = true }
func (f *fakeT) Skipf(string, ...any)  { f.skipped = true }

func TestUnavailable_SkipsWhenNotRequired(t *testing.T) {
	t.Setenv(testdeps.RequireEnv, "")
	f := &fakeT{}
	testdeps.Unavailable(f, "neo4j down")
	if !f.skipped || f.failed {
		t.Fatalf("skipped=%v failed=%v, want skip only", f.skipped, f.failed)
	}
}

func TestUnavailable_FailsWhenRequired(t *testing.T) {
	t.Setenv(testdeps.RequireEnv, "1")
	f := &fakeT{}
	testdeps.Unavailable(f, "neo4j down")
	if !f.failed || f.skipped {
		t.Fatalf("skipped=%v failed=%v, want fail only", f.skipped, f.failed)
	}
}
