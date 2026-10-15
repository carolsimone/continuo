package allowlist

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNew(t *testing.T, entries ...Entry) *Allowlist {
	t.Helper()
	a, err := New(entries)
	require.NoError(t, err)
	return a
}

func TestMatch_ExactHostIgnoresCaseAndTrailingDot(t *testing.T) {
	a := mustNew(t, Entry{Host: "API.Anthropic.com", Port: 443})
	for _, host := range []string{"api.anthropic.com", "API.ANTHROPIC.COM", "api.anthropic.com."} {
		e, ok := a.Match(host, 443)
		require.True(t, ok, host)
		assert.Equal(t, "api.anthropic.com", e.Host)
	}
	_, ok := a.Match("api.anthropic.com", 80)
	assert.False(t, ok, "another port")
	_, ok = a.Match("anthropic.com", 443)
	assert.False(t, ok, "the parent domain")
	_, ok = a.Match("evil-api.anthropic.com", 443)
	assert.False(t, ok, "a sibling name")
}

func TestMatch_WildcardCoversNamesBelowNotTheApex(t *testing.T) {
	a := mustNew(t, Entry{Host: "*.example.com", Port: 443})
	_, ok := a.Match("api.example.com", 443)
	assert.True(t, ok)
	_, ok = a.Match("a.b.example.com", 443)
	assert.True(t, ok)
	_, ok = a.Match("example.com", 443)
	assert.False(t, ok, "the apex")
	_, ok = a.Match("badexample.com", 443)
	assert.False(t, ok, "a name that only ends with the same letters")
	_, ok = a.Match("api.example.com", 8443)
	assert.False(t, ok, "another port")
	_, ok = a.Match("a..example.com", 443)
	assert.False(t, ok, "an empty label")
}

func TestMatch_ExactEntryWinsThenLongestWildcard(t *testing.T) {
	a := mustNew(t,
		Entry{Host: "*.example.com", Port: 443},
		Entry{Host: "*.internal.example.com", Port: 443, AllowPrivate: true},
		Entry{Host: "vault.internal.example.com", Port: 443},
	)
	e, _ := a.Match("db.internal.example.com", 443)
	assert.True(t, e.AllowPrivate, "the longest wildcard applies")
	e, _ = a.Match("vault.internal.example.com", 443)
	assert.False(t, e.AllowPrivate, "an exact entry beats every wildcard")
	e, _ = a.Match("api.example.com", 443)
	assert.False(t, e.AllowPrivate)
}

func TestNew_MergesDuplicates(t *testing.T) {
	a := mustNew(t, Entry{Host: "minio.storage.svc", Port: 9000}, Entry{Host: "MINIO.storage.svc", Port: 9000, AllowPrivate: true})
	assert.Equal(t, 1, a.Len())
	e, ok := a.Match("minio.storage.svc", 9000)
	require.True(t, ok)
	assert.True(t, e.AllowPrivate)
}

func TestNew_RejectsInvalidEntries(t *testing.T) {
	label64 := strings.Repeat("a", 64)
	for _, e := range []Entry{
		{Host: "", Port: 443},
		{Host: "1.2.3.4", Port: 443},
		{Host: "[::1]", Port: 443},
		{Host: "::1", Port: 443},
		{Host: "*", Port: 443},
		{Host: "*.com", Port: 443},
		{Host: "a.*.example.com", Port: 443},
		{Host: "*foo.example.com", Port: 443},
		{Host: "-a.example.com", Port: 443},
		{Host: "a-.example.com", Port: 443},
		{Host: "a_b.example.com", Port: 443},
		{Host: label64 + ".example.com", Port: 443},
		{Host: "api.example.com", Port: 0},
	} {
		_, err := New([]Entry{e})
		require.Error(t, err, "%+v", e)
		assert.True(t, errors.Is(err, ErrInvalidEntry), "%+v", e)
	}
}

func TestNew_AcceptsSingleLabelHosts(t *testing.T) {
	a := mustNew(t, Entry{Host: "stub-llm", Port: 9100, AllowPrivate: true})
	_, ok := a.Match("stub-llm", 9100)
	assert.True(t, ok)
}
