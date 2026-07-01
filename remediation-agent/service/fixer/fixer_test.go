package fixer

import (
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
)

func TestFor_KnownSources(t *testing.T) {
	for _, src := range []string{"compile", "seed_build", "validation"} {
		if _, err := For(src); err != nil {
			t.Fatalf("For(%q): unexpected error %v", src, err)
		}
	}
}

func TestFor_UnknownSource(t *testing.T) {
	if _, err := For("nonsense"); err == nil {
		t.Fatal("For(nonsense): want error, got nil")
	}
}

func TestNormalizeConfidence(t *testing.T) {
	cases := map[string]proposal.Confidence{
		"low": proposal.ConfidenceLow, "high": proposal.ConfidenceHigh,
		"medium": proposal.ConfidenceMedium, "": proposal.ConfidenceMedium,
	}
	for in, want := range cases {
		if got := normalizeConfidence(in); got != want {
			t.Fatalf("normalizeConfidence(%q)=%v want %v", in, got, want)
		}
	}
}
