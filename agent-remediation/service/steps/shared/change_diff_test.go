package shared_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// fakeVersions serves the version each node runs now.
type fakeVersions struct {
	versions map[string]ports.CurrentVersion
	errs     map[string]error
	calls    []string
}

func (f *fakeVersions) CurrentVersion(_ context.Context, id string) (ports.CurrentVersion, bool, error) {
	f.calls = append(f.calls, id)
	if err := f.errs[id]; err != nil {
		return ports.CurrentVersion{}, false, err
	}
	v, ok := f.versions[id]
	return v, ok, nil
}

var _ ports.VersionReader = (*fakeVersions)(nil)

func differ(v *fakeVersions) shared.ChangeDiffer {
	return shared.ChangeDiffer{Versions: v, Sanitizer: markSanitizer{}, Logger: discardLogger()}
}

func TestChangeDiffer_IsTheSanitizedDiffFromThePromotedVersion(t *testing.T) {
	v := &fakeVersions{versions: map[string]ports.CurrentVersion{"model.a": {RawCode: "select 1"}}}

	got := differ(v).Diff(context.Background(), "model.a", "select 2")

	assert.Equal(t, "<"+proposal.ComputeUnifiedDiff("select 1", "select 2", "model.a")+">", got)
	assert.Equal(t, []string{"model.a"}, v.calls)
}

func TestChangeDiffer_IsEmptyForANewNodeOrAnUnreadableVersion(t *testing.T) {
	cases := map[string]*fakeVersions{
		"a new node":            {},
		"an unreadable version": {errs: map[string]error{"model.a": errors.New("boom")}},
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Empty(t, differ(v).Diff(context.Background(), "model.a", "select 2"))
		})
	}
}

func TestChangeDiffer_ASmallDiffIsNotMarked(t *testing.T) {
	v := &fakeVersions{versions: map[string]ports.CurrentVersion{"model.a": {RawCode: "select 1"}}}

	assert.NotContains(t, differ(v).Diff(context.Background(), "model.a", "select 2"), "diff truncated")
}

func TestChangeDiffer_CapsALongDiffAtARuneBoundary(t *testing.T) {
	// A diff of two-byte runes cut at 8192 bytes lands inside a rune for one of
	// two neighbouring prefix lengths, so one of these inputs needs the cut
	// backed up.
	for _, pad := range []string{"", "x"} {
		t.Run("pad "+pad, func(t *testing.T) {
			next := "select '" + pad + strings.Repeat("é", 6000) + "'"
			v := &fakeVersions{versions: map[string]ports.CurrentVersion{"model.a": {RawCode: "select 1"}}}

			got := differ(v).Diff(context.Background(), "model.a", next)

			assert.True(t, strings.HasSuffix(got, "\n… (diff truncated)"))
			body := strings.TrimSuffix(got, "\n… (diff truncated)")
			assert.LessOrEqual(t, len(body), 8192)
			assert.Greater(t, len(body), 8190)
			assert.True(t, utf8.ValidString(body), "the cut does not split a rune")
		})
	}
}

func TestChangeDiffer_TruncatesAfterSanitizing(t *testing.T) {
	// The sanitizer wraps the whole diff, so the result ends with the cut marker
	// when truncation runs last, and with the sanitizer's closing mark when it
	// runs first.
	v := &fakeVersions{versions: map[string]ports.CurrentVersion{"model.a": {RawCode: "select 1"}}}

	got := differ(v).Diff(context.Background(), "model.a", "select '"+strings.Repeat("x", 9000)+"'")

	assert.True(t, strings.HasPrefix(got, "<"))
	assert.True(t, strings.HasSuffix(got, "\n… (diff truncated)"), "truncation runs after the sanitizer")
}

func TestLoggerOrDefault_ReplacesANilLogger(t *testing.T) {
	assert.NotNil(t, shared.LoggerOrDefault(nil))
	l := discardLogger()
	assert.Same(t, l, shared.LoggerOrDefault(l))
}
